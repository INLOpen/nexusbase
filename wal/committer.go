package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

// commit processes a batch of commitRecords. It's been extracted into its
// own file to reduce the size of wal.go and make the committer logic easier
// to reason about.
func (w *WAL) commit(records []*commitRecord) {
	start := time.Now()
	// Fast path: if WAL is closing, notify waiters and return.
	if w.isClosing.Load() {
		err := errors.New("wal is closed")
		for _, rec := range records {
			rec.done <- err
		}
		return
	}

	// Aggregate entries and detect control requests (rotate/sync).
	var allEntries []core.WALEntry
	var shouldRotate bool
	for _, rec := range records {
		if rec.entries == nil {
			shouldRotate = true
		} else {
			allEntries = append(allEntries, rec.entries...)
		}
	}

	// Handle the no-entries case (rotate or sync) with a short lock.
	if len(allEntries) == 0 {
		var err error
		w.mu.Lock()
		if shouldRotate {
			err = w.rotateLocked()
		} else if w.opts.SyncMode != core.WALSyncDisabled {
			if w.activeSegment != nil {
				err = w.activeSegment.Sync()
				if err == nil && w.TestingOnlySyncCount != nil {
					w.TestingOnlySyncCount.Add(1)
				}
			}
		}
		w.mu.Unlock()

		for _, rec := range records {
			rec.done <- err
		}
		return
	}

	// Assign sequence numbers to logical entries now so the on-disk records
	// contain correct SeqNum values and stream readers observe monotonic
	// sequences. This must be done before encoding the entries.
	for i := range allEntries {
		// If caller provided a SeqNum (non-zero), honor it and ensure the
		// WAL sequence counter is advanced to at least that value so future
		// assignments remain monotonic. Otherwise allocate the next seq.
		if allEntries[i].SeqNum != 0 {
			// Advance sequenceCounter to at least the provided value.
			for {
				cur := w.sequenceCounter.Load()
				if cur >= allEntries[i].SeqNum {
					break
				}
				if w.sequenceCounter.CompareAndSwap(cur, allEntries[i].SeqNum) {
					break
				}
			}
		} else {
			newSeq := w.sequenceCounter.Add(1)
			allEntries[i].SeqNum = newSeq
		}
	}

	// Build encoded entries payloads (may split into multiple payloads if too large).
	// We encode entries first without holding the WAL lock.
	var encodedEntries [][]byte
	var encodedEntryLists [][]core.WALEntry
	var payloadBufPtrs []*[]byte

	// Helper to start a new payload backed by a pooled []byte
	var currentBuf []byte
	var currentBufPtr *[]byte
	var currentEntries []core.WALEntry
	startNewPayload := func() {
		bptr := w.bufPool.Get().(*[]byte)
		*bptr = (*bptr)[:0]
		// Diagnostic: record allocation / checkout of pooled buffer
		slog.Default().Debug("WAL: payload buffer allocated", "buf_ptr", fmt.Sprintf("%p", bptr), "cap", cap(*bptr))
		currentBufPtr = bptr
		currentBuf = (*bptr)[:0]
		currentBuf = append(currentBuf, byte(core.EntryTypePutBatch))
		// placeholder for count (4 bytes)
		currentBuf = append(currentBuf, 0, 0, 0, 0)
		currentEntries = currentEntries[:0]
	}

	startNewPayload()

	for i := range allEntries {
		e := allEntries[i]
		// encode this single entry into a pooled temporary buffer to measure size
		bptr := w.bufPool.Get().(*[]byte)
		tmp := (*bptr)[:0]
		var err error
		tmp, err = encodeEntryToSlice(&e, tmp)
		if err != nil {
			*bptr = (*bptr)[:0]
			w.bufPool.Put(bptr)
			for _, rec := range records {
				rec.done <- err
			}
			return
		}

		// If adding this entry would exceed MaxSegmentSize for an empty payload and
		// it's a single entry, return ErrRecordTooLarge.
		entrySize := int64(len(tmp))
		// estimated record overhead (length + checksum)
		recordOverhead := int64(8)

		// Compute current payload size including the eventual length+checksum
		currentPayloadSize := int64(len(currentBuf)) + entrySize + recordOverhead

		if currentPayloadSize > w.opts.MaxSegmentSize {
			if len(currentEntries) == 0 {
				// Single entry alone exceeds MaxSegmentSize -> reject
				err := fmt.Errorf("%w: record_size=%d max_segment_size=%d", core.ErrRecordTooLarge, currentPayloadSize, w.opts.MaxSegmentSize)
				// return temporary buffer
				*bptr = (*bptr)[:0]
				w.bufPool.Put(bptr)
				for _, rec := range records {
					rec.done <- err
				}
				return
			}
			// finalize current payload
			b := currentBuf
			binary.LittleEndian.PutUint32(b[1:5], uint32(len(currentEntries)))
			encodedEntries = append(encodedEntries, b)
			// copy entries slice
			copied := make([]core.WALEntry, len(currentEntries))
			copy(copied, currentEntries)
			encodedEntryLists = append(encodedEntryLists, copied)
			// keep pointer for returning to pool after write
			payloadBufPtrs = append(payloadBufPtrs, currentBufPtr)
			// start new payload
			startNewPayload()
		}

		// append the entry to currentBuf and currentEntries
		currentBuf = append(currentBuf, tmp...)
		// return the temporary buffer to pool
		*bptr = (*bptr)[:0]
		w.bufPool.Put(bptr)
		currentEntries = append(currentEntries, e)
	}

	// finalize last payload
	if len(currentEntries) > 0 {
		b := currentBuf
		binary.LittleEndian.PutUint32(b[1:5], uint32(len(currentEntries)))
		encodedEntries = append(encodedEntries, b)
		copied := make([]core.WALEntry, len(currentEntries))
		copy(copied, currentEntries)
		encodedEntryLists = append(encodedEntryLists, copied)
		payloadBufPtrs = append(payloadBufPtrs, currentBufPtr)
	}

	// Now write each payload while holding the lock only for the write/sync/rotate
	var finalErr error
	var totalBytes int64
	var totalEntries int

	for pi, payload := range encodedEntries {
		payloadStart := time.Now()
		payloadBytes := payload
		newRecordSize := int64(len(payloadBytes) + 8)
		// (debug prints removed)

		w.mu.Lock()
		if w.activeSegment == nil || w.isClosing.Load() {
			w.mu.Unlock()
			finalErr = errors.New("wal is closed")
			break
		}

		currentSize, err := w.activeSegment.Size()
		if err != nil {
			w.mu.Unlock()
			finalErr = err
			break
		}
		headerSize := fileHeaderSize
		if (currentSize+newRecordSize) > w.opts.MaxSegmentSize && currentSize > headerSize {
			if err := w.rotateLocked(); err != nil {
				w.mu.Unlock()
				finalErr = err
				break
			}
		}

		// perform the write and optional sync while holding the lock to preserve ordering
		writeErr := w.activeSegment.WriteRecord(payloadBytes)
		// Only perform per-payload syncs when sync mode is explicitly "always".
		if writeErr == nil && w.opts.SyncMode == core.WALSyncAlways {
			writeErr = w.activeSegment.Sync()
			if writeErr == nil && w.TestingOnlySyncCount != nil {
				w.TestingOnlySyncCount.Add(1)
			}
		}

		// log payload write+sync duration for diagnostics
		payloadDur := time.Since(payloadStart)
		slog.Default().Info("WAL: payload write+sync", "payload_index", pi, "duration_ms", payloadDur.Milliseconds(), "payload_bytes", len(payloadBytes))

		// If this payload requested rotation (only at end), will perform after loop
		w.mu.Unlock()

		if writeErr != nil {
			finalErr = writeErr
			// return payload buffer to pool
			if payloadBufPtrs[pi] != nil {
				*payloadBufPtrs[pi] = (*payloadBufPtrs[pi])[:0]
				w.bufPool.Put(payloadBufPtrs[pi])
			}
			break
		}

		// Update metrics and notify streamers
		if w.metricsBytesWritten != nil {
			w.metricsBytesWritten.Add(newRecordSize)
		}
		if w.metricsEntriesWritten != nil {
			w.metricsEntriesWritten.Add(int64(len(encodedEntryLists[pi])))
		}
		totalBytes += newRecordSize
		totalEntries += len(encodedEntryLists[pi])
		// notify streamers with the logical entries
		// Build a small seq preview for correlation with stream-reader logs
		preview := make([]uint64, 0, len(encodedEntryLists[pi]))
		for i := 0; i < len(encodedEntryLists[pi]) && i < 8; i++ {
			preview = append(preview, encodedEntryLists[pi][i].SeqNum)
		}
		slog.Default().Debug("WAL: notifying streamers about payload", "payload_index", pi, "payload_buf_ptr", fmt.Sprintf("%p", payloadBufPtrs[pi]), "seq_preview", preview)
		w.notifyStreamers(encodedEntryLists[pi])

		// return payload buffer to pool (after notify)
		if payloadBufPtrs[pi] != nil {
			slog.Default().Debug("WAL: returning payload buffer to pool", "payload_index", pi, "payload_buf_ptr", fmt.Sprintf("%p", payloadBufPtrs[pi]))
			*payloadBufPtrs[pi] = (*payloadBufPtrs[pi])[:0]
			w.bufPool.Put(payloadBufPtrs[pi])
		}
	}

	// If everything succeeded and rotation was requested, perform it now.
	if finalErr == nil && shouldRotate {
		w.mu.Lock()
		if err := w.rotateLocked(); err != nil {
			finalErr = err
		}
		w.mu.Unlock()
	}

	// Notify all original waiters of the final result (same error for everyone)
	for _, rec := range records {
		rec.done <- finalErr
	}
	// update metrics and log overall commit duration for diagnostics
	slog.Default().Info("WAL: commit processed", "records", len(records), "duration_ms", time.Since(start).Milliseconds(), "total_bytes", totalBytes, "total_entries", totalEntries)
}

// encodeEntryToSlice appends the binary encoding of a WALEntry into the provided
// buffer and returns the extended slice. This avoids allocations done by
// creating temporary bytes.Buffers per entry.
func encodeEntryToSlice(entry *core.WALEntry, buf []byte) ([]byte, error) {
	// entry type (1 byte)
	buf = append(buf, byte(entry.EntryType))

	// seq num (8 bytes little endian)
	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], entry.SeqNum)
	buf = append(buf, seqBuf[:]...)

	// key length (uvarint) + key
	var tmpVar [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmpVar[:], uint64(len(entry.Key)))
	buf = append(buf, tmpVar[:n]...)
	if len(entry.Key) > 0 {
		buf = append(buf, entry.Key...)
	}

	// value length (uvarint) + value
	n = binary.PutUvarint(tmpVar[:], uint64(len(entry.Value)))
	buf = append(buf, tmpVar[:n]...)
	if len(entry.Value) > 0 {
		buf = append(buf, entry.Value...)
	}
	return buf, nil
}
