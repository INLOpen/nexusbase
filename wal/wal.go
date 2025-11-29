package wal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sys"
)

// fileHeaderSize caches the on-disk size of a `core.FileHeader` so writers
// and readers can agree on the header length without relying on repeated
// calls to `binary.Size`. Use core.FileHeader.Size() to compute this value.
var fileHeaderSize = int64((&core.FileHeader{}).Size())

// commitRecord represents a single commit request to the WAL.
// It bundles the entries to be written with a channel to signal completion.
type commitRecord struct {
	entries []core.WALEntry
	done    chan error
}

// streamerRegistration holds the information needed to notify a single stream reader.
type streamerRegistration struct {
	id      uint64
	notifyC chan notifyPayload
}

// notifyPayload is the payload sent to stream readers when new WAL entries
// are available. It contains an opaque notify ID for correlation and the
// batch of WALEntries (deep-copied by the sender).
type notifyPayload struct {
	notifyID uint64
	entries  []core.WALEntry
}

// WAL (Write-Ahead Log) provides durability by logging operations before they are applied to memtable.
// It manages a directory of segment files.
type WAL struct {
	dir  string
	mu   sync.Mutex
	opts Options

	activeSegment  *SegmentWriter
	segmentIndexes []uint64

	// Group Commit fields
	commitChan   chan *commitRecord
	shutdownChan chan struct{}
	committerWg  sync.WaitGroup
	closeOnce    sync.Once
	isClosing    atomic.Bool

	// Replication Streamer fields
	streamerIDCounter atomic.Uint64
	notifyCounter     atomic.Uint64
	streamerMu        sync.Mutex
	streamers         map[uint64]*streamerRegistration

	metricsBytesWritten   *expvar.Int
	metricsEntriesWritten *expvar.Int

	logger      *slog.Logger
	hookManager hooks.HookManager

	testingOnlyInjectCloseError  error
	testingOnlyInjectAppendError error

	// Testing-only hook: if non-nil, signal when a new streamer registration occurs.
	TestingOnlyStreamerRegistered chan struct{}

	// Buffer pool for encoding WALEntry payloads to reduce allocations.
	bufPool *sync.Pool
	// sequenceCounter holds the last assigned sequence number for WAL entries.
	sequenceCounter atomic.Uint64
}

var _ WALInterface = (*WAL)(nil)

// NewStreamReader creates a new reader for streaming WAL entries.
// It registers the reader to receive live notifications of new WAL entries.
func (w *WAL) NewStreamReader(fromSeqNum uint64) (StreamReader, error) {
	w.streamerMu.Lock()
	defer w.streamerMu.Unlock()

	id := w.streamerIDCounter.Add(1)
	reg := &streamerRegistration{
		id:      id,
		notifyC: make(chan notifyPayload, 256), // Buffered channel to avoid blocking the committer
	}

	w.streamers[id] = reg
	w.logger.Info("New WAL stream reader registered", "streamer_id", id)

	// Testing-only: signal test harness that a new streamer was registered.
	if w.TestingOnlyStreamerRegistered != nil {
		select {
		case w.TestingOnlyStreamerRegistered <- struct{}{}:
		default:
		}
	}

	// Initialize lastReadSeqNum so the reader will start returning the entry
	// with sequence number `fromSeqNum` on the first Next() call.
	// If the caller requests `fromSeqNum == 0`, treat it as starting from
	// the beginning (so lastReadSeqNum stays 0 and entries with seq>0 are returned).
	var lastReadSeqNum uint64
	if fromSeqNum > 0 {
		lastReadSeqNum = fromSeqNum - 1
	}

	sr := &streamReader{
		wal:            w,
		lastReadSeqNum: lastReadSeqNum, // A follower requesting from N needs to see N, so lastRead should be N-1.
		logger:         w.logger.With("component", "wal_stream_reader", "streamer_id", id),
		registration:   reg,
	}
	w.logger.Debug("NewStreamReader created", "streamer_id", id, "from_seq", fromSeqNum, "lastReadSeqNum", lastReadSeqNum)
	return sr, nil
}

// unregisterStreamer removes a streamer from the WAL's notification list.
func (w *WAL) unregisterStreamer(reg *streamerRegistration) {
	w.streamerMu.Lock()
	defer w.streamerMu.Unlock()

	if _, ok := w.streamers[reg.id]; ok {
		close(reg.notifyC) // Close the channel to signal the reader to stop
		delete(w.streamers, reg.id)
		w.logger.Info("WAL stream reader unregistered", "streamer_id", reg.id)
	}
}

// Options holds configuration for the WAL.
type Options struct {
	Dir                string
	SyncMode           core.WALSyncMode
	MaxSegmentSize     int64
	BytesWritten       *expvar.Int
	EntriesWritten     *expvar.Int
	Logger             *slog.Logger
	CommitMaxDelay     time.Duration // Max delay before a pending batch is committed.
	CommitMaxBatchSize int           // Max number of records in a batch before a commit is forced.
	StartRecoveryIndex uint64
	HookManager        hooks.HookManager
	// WriterBufferSize configures the size of the buffered writer used for segment writes.
	// If zero, a sensible default will be used.
	WriterBufferSize int
	// PreallocateSegments controls whether new segment files should be preallocated
	// to `PreallocSize` bytes at creation time. This is performed using a
	// platform-specific helper and is best-effort by default.
	//
	// Use a pointer so callers can explicitly disable preallocation by passing
	// `&false`. A nil value means the caller did not specify a preference and
	// the constructor will apply the default behavior.
	PreallocateSegments *bool
	// PreallocSize is the size (in bytes) to preallocate for new segments when
	// `PreallocateSegments` is enabled. If zero, `MaxSegmentSize` is used.
	PreallocSize int64
}

// Open creates or opens a WAL directory.
func Open(opts Options) (*WAL, []core.WALEntry, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default().With("component", "WAL_default")
	} else {
		opts.Logger = opts.Logger.With("component", "WAL")
	}
	if opts.MaxSegmentSize == 0 {
		opts.MaxSegmentSize = core.WALMaxSegmentSize
	}
	if opts.CommitMaxDelay == 0 {
		opts.CommitMaxDelay = 10 * time.Millisecond
	}
	if opts.CommitMaxBatchSize == 0 {
		opts.CommitMaxBatchSize = 64
	}
	if opts.WriterBufferSize == 0 {
		opts.WriterBufferSize = 64 * 1024 // 64 KiB default
	}
	// Default to preallocating segments; callers may explicitly disable this
	// by passing `PreallocateSegments: &false` in Options. A nil pointer means
	// no preference was provided and we enable preallocation by default.
	if opts.PreallocateSegments == nil {
		v := true
		opts.PreallocateSegments = &v
	}
	if opts.PreallocSize == 0 {
		opts.PreallocSize = opts.MaxSegmentSize
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create WAL directory %s: %w", opts.Dir, err)
	}

	w := &WAL{
		dir:                   opts.Dir,
		opts:                  opts,
		logger:                opts.Logger,
		metricsBytesWritten:   opts.BytesWritten,
		metricsEntriesWritten: opts.EntriesWritten,
		hookManager:           opts.HookManager,
		streamers:             make(map[uint64]*streamerRegistration),
	}
	// Initialize buffer pool used for encoding entries
	bufSize := opts.WriterBufferSize
	w.bufPool = &sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, bufSize)
			return &b
		},
	}
	w.isClosing.Store(false)

	if err := w.loadSegments(); err != nil {
		return nil, nil, fmt.Errorf("failed to load WAL segments: %w", err)
	}

	recoveredEntries, recoveryErr := w.recover(opts.StartRecoveryIndex)

	// Initialize sequence counter from recovered entries so newly-written
	// entries receive strictly-increasing sequence numbers that continue
	// from on-disk values.
	var maxSeq uint64
	for _, re := range recoveredEntries {
		if re.SeqNum > maxSeq {
			maxSeq = re.SeqNum
		}
	}
	w.sequenceCounter.Store(maxSeq)

	if err := w.openForAppend(); err != nil {
		return nil, nil, fmt.Errorf("failed to open WAL for appending: %w", err)
	}

	w.commitChan = make(chan *commitRecord, 128)
	w.shutdownChan = make(chan struct{})
	w.committerWg.Add(1)
	go w.runCommitter()

	if recoveryErr == io.EOF {
		return w, recoveredEntries, nil
	}
	return w, recoveredEntries, recoveryErr
}

func (w *WAL) loadSegments() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory %s: %w", w.dir, err)
	}

	w.segmentIndexes = make([]uint64, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		index, err := core.ParseSegmentFileName(file.Name())
		if err == nil {
			w.segmentIndexes = append(w.segmentIndexes, index)
		}
	}
	sort.Slice(w.segmentIndexes, func(i, j int) bool {
		return w.segmentIndexes[i] < w.segmentIndexes[j]
	})
	return nil
}

func (w *WAL) SetTestingOnlyInjectCloseError(err error) {
	w.testingOnlyInjectCloseError = err
}

func (w *WAL) SetTestingOnlyInjectAppendError(err error) {
	w.testingOnlyInjectAppendError = err
}

func (w *WAL) Append(entry core.WALEntry) error {
	return w.AppendBatch([]core.WALEntry{entry})
}

func (w *WAL) AppendBatch(entries []core.WALEntry) error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	if len(entries) == 0 {
		return nil
	}

	if w.testingOnlyInjectAppendError != nil {
		return w.testingOnlyInjectAppendError
	}

	rec := &commitRecord{
		entries: entries,
		done:    make(chan error, 1),
	}

	w.commitChan <- rec
	return <-rec.done
}

func (w *WAL) Sync() error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	rec := &commitRecord{
		entries: []core.WALEntry{},
		done:    make(chan error, 1),
	}
	w.commitChan <- rec
	return <-rec.done
}

func (w *WAL) Rotate() error {
	if w.isClosing.Load() {
		return errors.New("wal is closed")
	}
	rec := &commitRecord{
		entries: nil,
		done:    make(chan error, 1),
	}
	w.commitChan <- rec
	return <-rec.done
}

func (w *WAL) Close() (closeErr error) {
	w.closeOnce.Do(func() {
		w.isClosing.Store(true)

		if w.shutdownChan != nil {
			close(w.shutdownChan)
		}

		w.committerWg.Wait()

		w.streamerMu.Lock()
		for _, streamer := range w.streamers {
			close(streamer.notifyC)
		}
		w.streamers = make(map[uint64]*streamerRegistration)
		w.streamerMu.Unlock()

		w.mu.Lock()
		defer w.mu.Unlock()

		if w.testingOnlyInjectCloseError != nil {
			closeErr = w.testingOnlyInjectCloseError
			return
		}

		if w.activeSegment == nil {
			return
		}

		closeErr = w.activeSegment.Close()
		w.activeSegment = nil

		if closeErr != nil {
			w.logger.Error("Error during WAL close.", "error", closeErr)
		} else {
			w.logger.Info("WAL closed.")
		}
	})
	return
}

func (w *WAL) Purge(upToIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var remainingIndexes []uint64
	var purgedCount int
	for _, index := range w.segmentIndexes {
		if index <= upToIndex {
			if w.activeSegment != nil && w.activeSegment.index == index {
				w.logger.Warn("Skipping purge of active WAL segment", "index", index)
				remainingIndexes = append(remainingIndexes, index)
				continue
			}
			path := filepath.Join(w.dir, core.FormatSegmentFileName(index))
			if err := sys.Remove(path); err != nil {
				w.logger.Error("Failed to purge WAL segment", "path", path, "error", err)
			} else {
				purgedCount++
			}
		} else {
			remainingIndexes = append(remainingIndexes, index)
		}
	}
	w.segmentIndexes = remainingIndexes
	if purgedCount > 0 {
		w.logger.Info("Purged WAL segments", "count", purgedCount, "up_to_index", upToIndex)
	}
	return nil
}

func (w *WAL) Path() string {
	return w.dir
}

func (w *WAL) activeSegmentIndexLocked() uint64 {
	if w.activeSegment == nil {
		return 0
	}
	return w.activeSegment.index
}

func (w *WAL) ActiveSegmentIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeSegmentIndexLocked()
}

func (w *WAL) rotateLocked() error {
	var nextIndex uint64 = 1
	if len(w.segmentIndexes) > 0 {
		nextIndex = w.segmentIndexes[len(w.segmentIndexes)-1] + 1
	}

	prealloc := int64(0)
	if w.opts.PreallocateSegments != nil && *w.opts.PreallocateSegments {
		prealloc = w.opts.PreallocSize
	}
	newSegment, err := CreateSegment(w.dir, nextIndex, w.opts.WriterBufferSize, prealloc)
	if err != nil {
		return err
	}

	var oldIndex uint64
	if w.activeSegment != nil {
		oldIndex = w.activeSegment.index
		if err := w.activeSegment.Close(); err != nil {
			w.logger.Error("failed to close active segment during rotation", "path", w.activeSegment.path, "error", err)
		}
	}

	w.activeSegment = newSegment
	w.segmentIndexes = append(w.segmentIndexes, nextIndex)
	w.logger.Info("Rotated to new WAL segment", "index", nextIndex, "path", newSegment.path)
	if w.hookManager != nil && oldIndex > 0 {
		payload := hooks.PostWALRotatePayload{
			OldSegmentIndex: oldIndex,
			NewSegmentIndex: newSegment.index,
			NewSegmentPath:  newSegment.path,
		}
		w.hookManager.Trigger(context.Background(), hooks.NewPostWALRotateEvent(payload))
	}
	return nil
}

// rotate is a defensive wrapper that acquires the WAL mutex and then calls
// rotateLocked. Some code paths call rotation while already holding the
// mutex (and should call rotateLocked directly). Callers that do not hold
// the lock should call rotate() to avoid races.
func (w *WAL) rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

// encodeEntryData is retained for compatibility with older codepaths that may
// rely on an io.Writer-based encoding. Newer code uses `encodeEntryToSlice`.
func encodeEntryData(w io.Writer, entry *core.WALEntry) error {
	if err := binary.Write(w, binary.LittleEndian, entry.EntryType); err != nil {
		return fmt.Errorf("failed to write entry type: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, entry.SeqNum); err != nil {
		return fmt.Errorf("failed to write sequence number: %w", err)
	}

	if err := writeUvarintPrefixed(w, entry.Key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if err := writeUvarintPrefixed(w, entry.Value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}
	return nil
}

func decodeEntryData(r io.Reader) (*core.WALEntry, error) {
	entry := &core.WALEntry{}

	byteReader, ok := r.(io.ByteReader)
	if !ok {
		byteReader = bufio.NewReader(r)
	}

	if err := binary.Read(r, binary.LittleEndian, &entry.EntryType); err != nil {
		return nil, fmt.Errorf("failed to read entry type: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &entry.SeqNum); err != nil {
		return nil, fmt.Errorf("failed to read sequence number: %w", err)
	}

	var err error
	entry.Key, err = readUvarintPrefixed(byteReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}
	entry.Value, err = readUvarintPrefixed(byteReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return entry, nil
}

func (w *WAL) recover(startRecoveryIndex uint64) ([]core.WALEntry, error) {
	segmentsToRecover := make([]uint64, 0)
	for _, index := range w.segmentIndexes {
		if index <= startRecoveryIndex {
			continue
		}
		segmentsToRecover = append(segmentsToRecover, index)
	}

	if len(segmentsToRecover) == 0 {
		return nil, nil
	}

	numWorkers := runtime.NumCPU()
	if len(segmentsToRecover) < numWorkers {
		numWorkers = len(segmentsToRecover)
	}

	w.logger.Info("Starting parallel WAL recovery", "segments_to_recover", len(segmentsToRecover), "workers", numWorkers)

	var wg sync.WaitGroup
	segmentChan := make(chan uint64, len(segmentsToRecover))
	resultsChan := make(chan []core.WALEntry, len(segmentsToRecover))
	errChan := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for index := range segmentChan {
				path := filepath.Join(w.dir, core.FormatSegmentFileName(index))
				entries, err := recoverFromSegment(path, w.logger)
				if len(entries) > 0 {
					resultsChan <- entries
				}
				if err != nil && err != io.EOF {
					w.logger.Error("Error recovering from segment", "worker_id", workerID, "segment_index", index, "error", err)
					errChan <- err
					return
				}
			}
		}(i)
	}

	for _, index := range segmentsToRecover {
		segmentChan <- index
	}
	close(segmentChan)

	wg.Wait()
	close(resultsChan)
	close(errChan)

	var allEntries []core.WALEntry
	for entries := range resultsChan {
		allEntries = append(allEntries, entries...)
	}

	// Drain errChan and return the first non-nil error if any workers failed.
	var firstErr error
	for e := range errChan {
		if firstErr == nil && e != nil {
			firstErr = e
		}
	}

	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].SeqNum < allEntries[j].SeqNum
	})

	return allEntries, firstErr
}

func recoverFromSegment(filePath string, logger *slog.Logger) ([]core.WALEntry, error) {
	reader, err := OpenSegmentForRead(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("WAL segment does not exist, nothing to recover.", "path", filePath)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open WAL segment for reading %s: %w", filePath, err)
	}
	defer reader.Close()

	var entries []core.WALEntry
	for {
		recordData, err := reader.ReadRecord()
		if err != nil {
			return entries, err
		}

		batchEntries, err := decodeBatchRecord(recordData)
		if len(batchEntries) > 0 {
			entries = append(entries, batchEntries...)
		}
		if err != nil {
			return entries, fmt.Errorf("failed to decode batch record from segment: %w", err)
		}
	}
}

func (w *WAL) openForAppend() error {
	// Acquire the WAL lock while opening/rotating so that concurrent
	// committers cannot race with segment creation. Some callers may run
	// this during initialization (no concurrency), but taking the lock is
	// cheap and makes the behavior consistent.
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.segmentIndexes) == 0 {
		return w.rotateLocked()
	}

	lastIndex := w.segmentIndexes[len(w.segmentIndexes)-1]
	path := filepath.Join(w.dir, core.FormatSegmentFileName(lastIndex))

	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat last segment %s: %w", path, err)
	}

	headerSize := fileHeaderSize

	// If the last segment already contains data beyond the header, rotate
	// to start a fresh segment for appends.
	if stat.Size() > headerSize {
		return w.rotateLocked()
	}

	// The last segment exists but only contains the header (empty).
	// Open it for append without truncating so we don't lose state.
	// Open existing segment for append. Use O_APPEND to ensure writes are
	// appended by the OS even if another descriptor exists for the same
	// file (prevents races where independent offsets cause overlapping
	// writes on platforms that permit multiple open handles).
	f, err := sys.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open existing WAL segment for append %s: %w", path, err)
	}
	// Ensure the file offset is at the end for appending.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return fmt.Errorf("failed to seek to end of existing WAL segment %s: %w", path, err)
	}

	seg := &Segment{
		file:  f,
		path:  path,
		index: lastIndex,
	}
	w.activeSegment = &SegmentWriter{
		Segment: seg,
		writer:  bufio.NewWriterSize(f, w.opts.WriterBufferSize),
		size:    stat.Size(),
	}
	return nil
}

func writeUvarintPrefixed(w io.Writer, data []byte) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(data)))
	if _, err := w.Write(buf[:n]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func readUvarintPrefixed(r io.ByteReader) ([]byte, error) {
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		data := make([]byte, length)
		if _, err := io.ReadFull(r.(io.Reader), data); err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, nil
}

func decodeBatchRecord(recordData []byte) ([]core.WALEntry, error) {
	reader := bytes.NewReader(recordData)
	var entryTypeByte byte
	entryTypeByte, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading entry type from WAL record: %w", err)
	}
	if core.EntryType(entryTypeByte) != core.EntryTypePutBatch {
		return nil, fmt.Errorf("unexpected WAL record type: got %d, want %d (EntryTypePutBatch)", entryTypeByte, core.EntryTypePutBatch)
	}

	var numEntries uint32
	if err := binary.Read(reader, binary.LittleEndian, &numEntries); err != nil {
		return nil, fmt.Errorf("error reading batch entry count: %w", err)
	}

	entries := make([]core.WALEntry, 0, numEntries)
	for i := 0; i < int(numEntries); i++ {
		entry, err := decodeEntryData(reader)
		if err != nil {
			return entries, fmt.Errorf("error decoding entry %d in batch: %w", i+1, err)
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

func (w *WAL) runCommitter() {
	defer w.committerWg.Done()

	ticker := time.NewTicker(w.opts.CommitMaxDelay)
	defer ticker.Stop()

	var pending []*commitRecord

	for {
		select {
		case rec := <-w.commitChan:
			pending = append(pending, rec)
			if len(pending) >= w.opts.CommitMaxBatchSize {
				w.commit(pending)
				pending = nil
			}

		case <-ticker.C:
			if len(pending) > 0 {
				w.commit(pending)
				pending = nil
			}

		case <-w.shutdownChan:
			if len(pending) > 0 {
				w.commit(pending)
			}
			for len(w.commitChan) > 0 {
				rec := <-w.commitChan
				rec.done <- errors.New("wal is closed")
			}
			return
		}
	}
}

// notifyStreamers broadcasts a batch of entries to all registered streamers.
func (w *WAL) notifyStreamers(entries []core.WALEntry) {
	w.streamerMu.Lock()
	defer w.streamerMu.Unlock()

	if len(w.streamers) == 0 {
		return
	}

	w.logger.Info("Notifying streamers", "count", len(w.streamers), "entries", len(entries))
	// Diagnostic: log pointers of entry Key/Value slices to help detect aliasing
	for i, e := range entries {
		if i >= 16 {
			// limit per-notify verbosity
			break
		}
		kp := ""
		vp := ""
		if len(e.Key) > 0 {
			kp = fmt.Sprintf("%p", &e.Key[0])
		}
		if len(e.Value) > 0 {
			vp = fmt.Sprintf("%p", &e.Value[0])
		}
		w.logger.Debug("WAL: notify entry ptrs", "index", i, "seq", e.SeqNum, "key_ptr", kp, "value_ptr", vp, "key_len", len(e.Key), "value_len", len(e.Value))
	}

	for id, streamer := range w.streamers {
		// Build a deep copy of the entries slice including copies of inner
		// Key/Value byte slices to avoid any chance that pooled backing
		// storage can be modified or reclaimed while readers are processing
		// the batch.
		batch := make([]core.WALEntry, len(entries))
		for i := range entries {
			// Shallow-copy the struct first
			batch[i] = entries[i]
			// Deep-copy key and value bytes
			if len(entries[i].Key) > 0 {
				batch[i].Key = append([]byte(nil), entries[i].Key...)
			}
			if len(entries[i].Value) > 0 {
				batch[i].Value = append([]byte(nil), entries[i].Value...)
			}
		}
		// Assign a notify ID for correlation across logs
		notifyID := w.notifyCounter.Add(1)
		// Build a small seq preview for logging
		preview := make([]uint64, 0, len(batch))
		for i := 0; i < len(batch) && i < 8; i++ {
			preview = append(preview, batch[i].SeqNum)
		}
		// Use Info level here so the dispatch is visible in test runs and can
		// be correlated with committer and stream-reader logs.
		w.logger.Debug("WAL: dispatching notify payload", "notify_id", notifyID, "streamer_id", id, "seq_preview", preview)
		payload := notifyPayload{notifyID: notifyID, entries: batch}
		select {
		case streamer.notifyC <- payload:
			// Sent successfully
		default:
			// Channel is full, meaning the reader is lagging badly.
			w.logger.Warn("WAL streamer notification channel full; reader may be lagging", "streamer_id", id)
		}
	}
}
