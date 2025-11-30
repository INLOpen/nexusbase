package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog" // Import slog
	"math"
	"sync"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/cache" // Import the cache package
	"github.com/INLOpen/nexusbase/core"  // Import core for EntryType, CompressionType
	"github.com/INLOpen/nexusbase/filter"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// noopFilter is a tiny Filter implementation used when a block-format SSTable
// doesn't carry a Bloom filter on disk. It always returns true for Contains,
// deferring correctness to the sparse index and disk reads.
type noopFilter struct{}

func (n *noopFilter) Contains(_ []byte) bool { return true }
func (n *noopFilter) Bytes() []byte          { return nil }

// sstable.go: Defines SSTable struct, LoadSSTable, Get, Close
// Placeholder for SSTable structure and core logic.
// Details to be filled based on FR4.

// SSTable represents a Sorted String Table file.
// It holds metadata and provides methods to access its data.
type SSTable struct {
	file     sys.FileHandle
	mu       sync.RWMutex // Protects access to the file handle and other internal state
	filePath string
	id       uint64 // Unique identifier for the SSTable

	index          *Index        // In-memory index for data blocks/entries (FR4.2), defined in index.go
	filter         filter.Filter // Bloom filter for fast existence checks (FR4.3)
	minKey         []byte        // Minimum key in this SSTable (FR4.4)
	maxKey         []byte        // Maximum key in this SSTable (FR4.4)
	size           int64         // Size of the SSTable file on disk (FR4.5)
	keyCount       uint64        // Total number of entries in the SSTable
	tombstoneCount uint64        // Total number of tombstone entries

	// dataEndOffset is the offset in the file where the actual key-value data blocks end
	// and the footer (index, bloom filter, metadata) begins.
	dataEndOffset int64
	blockCache    cache.Interface // Shared cache for data blocks (FR4.8), defined in block.go (or a general cache package)
	tracer        trace.Tracer    // For OpenTelemetry tracing
	logger        *slog.Logger    // Add logger field

	closed atomic.Bool // Tracks if the SSTable has been closed
}

// LoadSSTableOptions holds all parameters for loading an SSTable.
type LoadSSTableOptions struct {
	FilePath   string
	ID         uint64
	BlockCache cache.Interface
	Tracer     trace.Tracer
	Logger     *slog.Logger
}

// LoadSSTable loads an SSTable from the given file path.
// It reads the footer, deserializes the index and Bloom filter,
// and populates the SSTable metadata.
// Corresponds to FR4.6.
func LoadSSTable(opts LoadSSTableOptions) (sst *SSTable, err error) {
	var span trace.Span
	if opts.Tracer != nil {
		// Assuming context.Background() for internal operations not tied to a specific request context
		_, span = opts.Tracer.Start(context.Background(), "SSTable.LoadSSTable")
		var sstableIDint64 int64
		if opts.ID <= uint64(math.MaxInt64) {
			sstableIDint64 = int64(opts.ID)
		} else {
			sstableIDint64 = math.MaxInt64
			opts.Logger.Warn("SSTable ID exceeds int64 range, using MaxInt64 for tracing attribute.", "sstable_id", opts.ID)
		}
		span.SetAttributes(attribute.String("sstable.filepath", opts.FilePath), attribute.Int64("sstable.id", sstableIDint64))
		defer span.End()
	}
	// Ensure logger is not nil
	if opts.Logger == nil {
		opts.Logger = slog.Default().With("component", "SSTable_default")
	} else {
		opts.Logger = opts.Logger.With("sstable_id", opts.ID)
	}
	// Ensure logger is not nil
	if opts.Logger == nil {
		opts.Logger = slog.Default().With("component", "SSTable_default")
	} else {
		opts.Logger = opts.Logger.With("sstable_id", opts.ID)
	}
	// FR6.1: Consider using a FileOpener interface for testability. For now, direct os.Open.
	file, err := sys.Open(opts.FilePath)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		return nil, fmt.Errorf("failed to open sstable file %s: %w", opts.FilePath, err)
	}
	// Use defer with a named error return to ensure the file is closed on any error path.
	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	// Read header bytes first to isolate file I/O from binary parsing.
	// This helps debug issues where binary.Read might behave unexpectedly on a file handle.
	headerSize := binary.Size(core.FileHeader{})
	if headerSize <= 0 {
		// This would be a programming error in core.FileHeader definition.
		return nil, fmt.Errorf("invalid FileHeader size: %d", headerSize) // Early return for programming error
	}
	headerBytes := make([]byte, headerSize)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read sstable header bytes from %s: %w", opts.FilePath, err)
	}

	// Now parse the header from the in-memory byte slice.
	var header core.FileHeader
	if err := binary.Read(bytes.NewReader(headerBytes), binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to parse sstable header from bytes: %w", err) // Return err directly
	}

	if header.Magic != core.SSTableMagicNumber {
		// Not a legacy-format SSTable header. Try to detect block-level v2 format
		// and load it via the block-format loader. This allows the system to
		// read both legacy and block-format SSTables transparently.
		if sst, err = loadBlockFormatSSTable(opts); err == nil {
			return sst, nil
		}
		return nil, fmt.Errorf("invalid sstable magic number in %s. Got: %x, Want: %x", opts.FilePath, header.Magic, core.SSTableMagicNumber)
	}
	if header.Version != core.FormatVersion {
		return nil, fmt.Errorf("unsupported sstable version in %s. Got: %d, Want: %d", opts.FilePath, header.Version, core.FormatVersion)
	}

	stat, err := file.Stat()
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to stat sstable file %s: %w", opts.FilePath, err)
	}
	fileSize := stat.Size()

	// FR7.2: Basic corruption check - file size
	minValidSize := int64(header.Size() + FooterSize)
	if fileSize < minValidSize {
		if span != nil {
			span.RecordError(ErrCorrupted)
			span.SetStatus(codes.Error, ErrCorrupted.Error())
		}
		return nil, fmt.Errorf("sstable file %s is too small to be valid (size: %d, min: %d): %w", opts.FilePath, fileSize, minValidSize, ErrCorrupted)
	}

	// Read the fixed part of the footer (excluding magic string)
	footerFixedBytes := make([]byte, FooterFixedComponentSize)
	// Read from position: fileSize - FooterSize up to fileSize - MagicStringLen
	if _, err = file.ReadAt(footerFixedBytes, fileSize-int64(FooterSize)); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to read footer fixed components from %s: %w", opts.FilePath, err)
	}

	footerReader := bytes.NewReader(footerFixedBytes)
	var indexOffset, bloomFilterOffset uint64
	var minKeyOffset, maxKeyOffset uint64
	var keyCount, tombstoneCount uint64
	var indexLen, bloomFilterLen uint32
	var minKeyLen, maxKeyLen uint32

	binary.Read(footerReader, binary.LittleEndian, &indexOffset)
	binary.Read(footerReader, binary.LittleEndian, &indexLen)
	binary.Read(footerReader, binary.LittleEndian, &bloomFilterOffset)
	binary.Read(footerReader, binary.LittleEndian, &bloomFilterLen)
	binary.Read(footerReader, binary.LittleEndian, &minKeyOffset)
	binary.Read(footerReader, binary.LittleEndian, &minKeyLen)
	binary.Read(footerReader, binary.LittleEndian, &maxKeyOffset)
	binary.Read(footerReader, binary.LittleEndian, &maxKeyLen)
	binary.Read(footerReader, binary.LittleEndian, &keyCount)
	binary.Read(footerReader, binary.LittleEndian, &tombstoneCount)

	// Read and verify Magic String (FR4.1, FR7.2)
	magicBytes := make([]byte, core.SSTableMagicStringLen)
	if _, err = file.ReadAt(magicBytes, fileSize-int64(core.SSTableMagicStringLen)); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to read magic string from %s: %w", opts.FilePath, err)
	}
	if string(magicBytes) != core.SSTableMagicString {
		if span != nil {
			span.RecordError(ErrCorrupted)
			span.SetStatus(codes.Error, ErrCorrupted.Error())
		}
		return nil, fmt.Errorf("invalid magic string in sstable file %s: got %s, want %s: %w", opts.FilePath, string(magicBytes), core.SSTableMagicString, ErrCorrupted)
	}

	// Read and deserialize Bloom Filter (FR4.3)
	bloomFilterData := make([]byte, bloomFilterLen)
	if _, err = file.ReadAt(bloomFilterData, int64(bloomFilterOffset)); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to read bloom filter data from %s: %w", opts.FilePath, err)
	}
	var filter filter.Filter
	if filter, err = DeserializeBloomFilter(bloomFilterData); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to deserialize filter from %s: %w", opts.FilePath, err)
	}

	// Read and deserialize Index (FR4.2)
	// The index data is preceded by its checksum.
	// The indexOffset in the footer points to the start of the actual index data.
	// So, the checksum is at indexOffset - core.ChecksumSize.
	checksumOffset := int64(indexOffset) - int64(core.ChecksumSize)
	if checksumOffset < 0 {
		if span != nil {
			span.RecordError(ErrCorrupted)
			span.SetStatus(codes.Error, ErrCorrupted.Error())
		}
		return nil, fmt.Errorf("invalid index offset for checksum in %s: %w", opts.FilePath, ErrCorrupted)
	}

	checksumBytes := make([]byte, core.ChecksumSize)
	if _, err = file.ReadAt(checksumBytes, checksumOffset); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to read index checksum from %s: %w", opts.FilePath, err)
	}
	indexChecksum := binary.LittleEndian.Uint32(checksumBytes)

	actualIndexData := make([]byte, indexLen)
	if _, err = file.ReadAt(actualIndexData, int64(indexOffset)); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to read index data from %s: %w", opts.FilePath, err)
	}

	var idx *Index
	if idx, err = DeserializeIndex(actualIndexData, indexChecksum, opts.Tracer, opts.Logger); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("failed to deserialize index from %s: %w", opts.FilePath, err)
	}

	// FR4.4: Read minKey and maxKey directly from their sections using offsets from the footer
	minKey := make([]byte, minKeyLen)
	if _, err := file.ReadAt(minKey, int64(minKeyOffset)); err != nil {
		return nil, fmt.Errorf("failed to read minKey data from %s: %w", opts.FilePath, err)
	}

	maxKey := make([]byte, maxKeyLen)
	if _, err := file.ReadAt(maxKey, int64(maxKeyOffset)); err != nil {
		return nil, fmt.Errorf("failed to read maxKey data from %s: %w", opts.FilePath, err)
	}

	// dataEndOffset is the offset where the actual key-value data blocks end.
	// This is effectively the offset of the first metadata block (e.g., index block).
	actualDataEndOffset := int64(indexOffset)

	sst = &SSTable{
		file:           file,
		filePath:       opts.FilePath,
		id:             opts.ID,
		index:          idx,
		filter:         filter,
		minKey:         minKey,
		maxKey:         maxKey,
		size:           fileSize, // FR4.5
		keyCount:       keyCount,
		tombstoneCount: tombstoneCount,
		dataEndOffset:  actualDataEndOffset,
		blockCache:     opts.BlockCache, // FR4.8
		tracer:         opts.Tracer,
		logger:         opts.Logger,
	}

	return sst, nil
}

// loadBlockFormatSSTable loads a block-level (v2) SSTable and adapts its
// block index into the in-memory Index structure so the rest of the code
// can treat it like a regular SSTable.
func loadBlockFormatSSTable(opts LoadSSTableOptions) (sst *SSTable, err error) {
	// Open the file
	file, ferr := sys.Open(opts.FilePath)
	if ferr != nil {
		return nil, ferr
	}
	// Read last 64 bytes footer
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if fi.Size() < 64 {
		file.Close()
		return nil, fmt.Errorf("file too small to be block-format sstable: %s", opts.FilePath)
	}
	if _, err := file.Seek(-64, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	footer := make([]byte, 64)
	if _, err := io.ReadFull(file, footer); err != nil {
		file.Close()
		return nil, err
	}
	// parse block writer footer layout (see block_writer.go)
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int(binary.LittleEndian.Uint32(footer[8:12]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomLen := int(binary.LittleEndian.Uint32(footer[20:24]))
	metaOffset := int64(binary.LittleEndian.Uint64(footer[24:32]))
	metaLen := int(binary.LittleEndian.Uint32(footer[32:36]))
	_ = metaOffset
	_ = metaLen
	// format_version := binary.LittleEndian.Uint32(footer[36:40])
	// verify footer magic
	magic := string(footer[56:64])
	if magic != footerMagic {
		file.Close()
		return nil, fmt.Errorf("invalid block-format footer magic in %s: got %s", opts.FilePath, magic)
	}

	// read index block
	if _, err := file.Seek(indexOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	indexBuf := make([]byte, indexLen)
	if _, err := io.ReadFull(file, indexBuf); err != nil {
		file.Close()
		return nil, err
	}
	// parse index entries: uvarint keylen + key + off(8) + compLen(4) + uncompLen(4)
	off := 0
	idx := &Index{tracer: opts.Tracer, logger: opts.Logger}
	for off < len(indexBuf) {
		v, n := binary.Uvarint(indexBuf[off:])
		if n <= 0 {
			file.Close()
			return nil, fmt.Errorf("failed to parse index uvarint in %s", opts.FilePath)
		}
		off += n
		keyLen := int(v)
		if off+keyLen > len(indexBuf) {
			file.Close()
			return nil, io.ErrUnexpectedEOF
		}
		key := make([]byte, keyLen)
		copy(key, indexBuf[off:off+keyLen])
		off += keyLen
		if off+8+4+4 > len(indexBuf) {
			file.Close()
			return nil, io.ErrUnexpectedEOF
		}
		blockOffset := int64(binary.LittleEndian.Uint64(indexBuf[off : off+8]))
		off += 8
		compLen := binary.LittleEndian.Uint32(indexBuf[off : off+4])
		off += 4
		_ = binary.LittleEndian.Uint32(indexBuf[off : off+4])
		off += 4
		// Disk block length includes compLen + trailer (13 bytes)
		diskLen := compLen + 13
		idx.entries = append(idx.entries, BlockIndexEntry{FirstKey: key, BlockOffset: blockOffset, BlockLength: diskLen})
	}

	// determine min/max keys from index
	var minKey, maxKey []byte
	if len(idx.entries) > 0 {
		minKey = idx.entries[0].FirstKey
		// Attempt to determine a proper maxKey by reading the last block and extracting its last key.
		lastEntry := idx.entries[len(idx.entries)-1]
		// Read the raw block (compressed payload + trailer)
		rawBlock := make([]byte, lastEntry.BlockLength)
		if _, err := file.ReadAt(rawBlock, lastEntry.BlockOffset); err == nil {
			// trailer is last 13 bytes
			if len(rawBlock) >= 13 {
				trailer := rawBlock[len(rawBlock)-13:]
				compType := core.CompressionType(trailer[12])
				compressedPayload := rawBlock[:len(rawBlock)-13]
				// Decompress
				compressor, cerr := GetCompressor(compType)
				if cerr == nil {
					if dr, derr := compressor.Decompress(compressedPayload); derr == nil {
						defer dr.Close()
						// Read decompressed bytes
						buf := make([]byte, 0)
						tmp := make([]byte, 4096)
						for {
							n, rerr := dr.Read(tmp)
							if n > 0 {
								buf = append(buf, tmp[:n]...)
							}
							if rerr != nil {
								if rerr == io.EOF {
									break
								}
								break
							}
						}
						if len(buf) > 0 {
							blk := NewBlock(buf)
							it := NewBlockIterator(blk.getEntriesData())
							var lastKey []byte
							for it.Next() {
								lastKey = append(lastKey[:0], it.Key()...)
							}
							if it.Error() == nil && len(lastKey) > 0 {
								maxKey = lastKey
							} else {
								maxKey = lastEntry.FirstKey
							}
						} else {
							maxKey = lastEntry.FirstKey
						}
					} else {
						// failed to decompress; fall back
						maxKey = lastEntry.FirstKey
					}
				} else {
					maxKey = lastEntry.FirstKey
				}
			} else {
				maxKey = lastEntry.FirstKey
			}
		} else {
			maxKey = lastEntry.FirstKey
		}
	}

	// Attempt to deserialize a persisted Bloom filter if one was written by
	// the BlockWriter. If not present or deserialization fails, fall back to
	// reconstructing a Bloom filter by scanning all blocks.
	var reconstructedFilter *BloomFilter
	var persistedFilter *BloomFilter
	if bloomLen > 0 && bloomOffset > 0 {
		bloomData := make([]byte, bloomLen)
		if _, err := file.ReadAt(bloomData, bloomOffset); err == nil {
			if bf, berr := DeserializeBloomFilter(bloomData); berr == nil {
				persistedFilter = bf
			}
		}
	}
	// If we didn't get a persisted filter, rebuild one from the blocks.
	if persistedFilter == nil {
		// Reconstruct an in-memory Bloom filter by scanning all data blocks and
		// adding each key seen. This provides parity with legacy SSTables where a
		// Bloom filter was written at write-time. The reconstruction can be
		// expensive for very large tables, but ensures reasonable lookup pruning
		// for block-format SSTables when no persisted filter is available.
		allKeysCount := 0
		// First pass: count keys to size the Bloom filter
		for _, be := range idx.entries {
			// read raw disk block (comp payload + 13-byte trailer)
			raw := make([]byte, be.BlockLength)
			if _, err := file.ReadAt(raw, be.BlockOffset); err != nil {
				// If we cannot read a block, skip adding from this block.
				continue
			}
			if len(raw) < 13 {
				continue
			}
			compPayload := raw[:len(raw)-13]
			trailer := raw[len(raw)-13:]
			compType := core.CompressionType(trailer[12])
			comp, cerr := GetCompressor(compType)
			if cerr != nil {
				continue
			}
			dr, derr := comp.Decompress(compPayload)
			if derr != nil {
				continue
			}
			// Read decompressed bytes and iterate entries to count keys
			decompBuf := make([]byte, 0)
			tmp := make([]byte, 4096)
			for {
				n, rerr := dr.Read(tmp)
				if n > 0 {
					decompBuf = append(decompBuf, tmp[:n]...)
				}
				if rerr != nil {
					_ = dr.Close()
					break
				}
			}
			_ = dr.Close()
			if len(decompBuf) == 0 {
				continue
			}
			blk := NewBlock(decompBuf)
			it := NewBlockIterator(blk.getEntriesData())
			for it.Next() {
				allKeysCount++
			}
		}
		if allKeysCount == 0 {
			allKeysCount = 1
		}
		bf, berr := NewBloomFilter(uint64(allKeysCount), 0.01)
		if berr == nil {
			// Second pass: actually add keys
			for _, be := range idx.entries {
				raw := make([]byte, be.BlockLength)
				if _, err := file.ReadAt(raw, be.BlockOffset); err != nil {
					continue
				}
				if len(raw) < 13 {
					continue
				}
				compPayload := raw[:len(raw)-13]
				trailer := raw[len(raw)-13:]
				compType := core.CompressionType(trailer[12])
				comp, cerr := GetCompressor(compType)
				if cerr != nil {
					continue
				}
				dr, derr := comp.Decompress(compPayload)
				if derr != nil {
					continue
				}
				decompBuf := make([]byte, 0)
				tmp := make([]byte, 4096)
				for {
					n, rerr := dr.Read(tmp)
					if n > 0 {
						decompBuf = append(decompBuf, tmp[:n]...)
					}
					if rerr != nil {
						_ = dr.Close()
						break
					}
				}
				_ = dr.Close()
				if len(decompBuf) == 0 {
					continue
				}
				blk := NewBlock(decompBuf)
				it := NewBlockIterator(blk.getEntriesData())
				for it.Next() {
					k := it.Key()
					// Add a copy of k to the bloom filter
					kb := make([]byte, len(k))
					copy(kb, k)
					bf.Add(kb)
				}
			}
			reconstructedFilter = bf
		}
	}

	// Build SSTable wrapper
	stat, _ := file.Stat()
	fileSize := stat.Size()
	// Prefer a reconstructed Bloom filter if available, otherwise fall back
	// to the noopFilter to avoid short-circuiting Get() for v2 SSTables.
	var filterToUse filter.Filter
	if reconstructedFilter != nil {
		filterToUse = reconstructedFilter
	} else {
		filterToUse = &noopFilter{}
	}
	sst = &SSTable{
		file:           file,
		filePath:       opts.FilePath,
		id:             opts.ID,
		index:          idx,
		filter:         filterToUse,
		minKey:         minKey,
		maxKey:         maxKey,
		size:           fileSize,
		keyCount:       0,
		tombstoneCount: 0,
		dataEndOffset:  indexOffset,
		blockCache:     opts.BlockCache,
		tracer:         opts.Tracer,
		logger:         opts.Logger,
	}
	return sst, nil
}

// Get retrieves a value and its type for a given key from the SSTable.
// It first checks the Bloom filter, then uses the index to locate the data.
// Returns value, core.EntryType, and an error. If not found, error will be ErrNotFound.
// Corresponds to FR1.3 (partially) and integrates with Block Cache (FR4.8).
func (s *SSTable) Get(key []byte) (value []byte, entryType core.EntryType, err error) {
	if s.closed.Load() {
		return nil, 0, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return nil, 0, ErrClosed
	}
	if s.filter == nil || s.index == nil || len(s.index.entries) == 0 {
		return nil, 0, ErrNotFound
	}
	// NOTE: The bloom filter check (`Contains`) is expected to be performed by the caller (the engine)
	// before calling Get. This allows the engine to manage metrics for checks and false positives.

	// If we reach here, Bloom Filter said "maybe". Now we need to confirm.

	// FR4.4: Check if key is within [s.minKey, s.maxKey].
	if bytes.Compare(key, s.minKey) < 0 || bytes.Compare(key, s.maxKey) > 0 {
		// This check might seem redundant if Bloom filter is accurate and index is dense,
		// but it's a good safeguard and essential for sparse indexes or range checks.
		return nil, 0, ErrNotFound
	}

	// FR4.2 (Sparse Index): Use s.index to find the block that might contain the key.
	blockMeta, found := s.index.Find(key) // Find returns BlockIndexEntry
	if !found {
		s.logger.Debug("Key not found via index.Find", "key", string(key))
		// If a Bloom filter existed and caller checked it, this situation
		// likely represents a false-positive from the Bloom filter. Signal
		// the hook so higher layers can increment false-positive metrics.
		if s.filter != nil {
			invokeBloomFalsePositiveHook()
		}
		return nil, 0, ErrNotFound
	}

	// FR4.8: Read block (through cache)
	block, err := s.readBlock(blockMeta.BlockOffset, blockMeta.BlockLength, nil) // Pass nil semaphore for single Get operations
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read block for key %s: %w", string(key), err)
	}
	// The readBlock function now returns a *Block and manages its own buffers.
	// We don't need to put anything back into a pool here.

	// Find in block now returns value, entryType, seqNum, error
	valResult, typeResultFromBlock, _, errResultFromBlock := block.Find(key) // We ignore seqNum for Get's return signature for now

	if errResultFromBlock != nil { // This includes ErrNotFound from block.Find (e.g. tombstone or truly not found)
		// The logic to increment the false positive metric now resides in the engine,
		// which has the context of calling Contains() before calling Get().
		return nil, 0, errResultFromBlock // Propagate ErrNotFound or other errors
	}
	return valResult, typeResultFromBlock, nil // Should be EntryTypePut if no error
}

// Contains checks if the key might be in the SSTable using the Bloom filter.
// If no filter is present, it returns true to maintain correctness (forcing a disk read).
func (s *SSTable) Contains(key []byte) bool {
	if s.filter == nil {
		return true // No filter, so we must assume it might contain the key.
	}
	// Signal a Bloom filter check to any registered hook before returning
	// the result. Higher layers may use the hook to increment metrics.
	invokeBloomCheckHook()
	return s.filter.Contains(key)
}

// readBlock reads a data block from the SSTable, utilizing the block cache.
func (s *SSTable) readBlock(offset int64, length uint32, sem chan struct{}) (*Block, error) { //nolint:revive
	// Acquire a semaphore permit if provided. This limits concurrency for block reads. //nolint:revive
	if sem != nil {
		sem <- struct{}{}
		defer func() {
			<-sem // Release the permit when done.
		}()
	}

	var span trace.Span
	if s.file == nil {
		return nil, ErrClosed
	}
	// This method is internal and assumes the caller holds the necessary lock.
	if s.tracer != nil {
		_, span = s.tracer.Start(context.Background(), "SSTable.readBlock")
		span.SetAttributes(attribute.Int64("sstable.block_offset", offset), attribute.Int64("sstable.block_length", int64(length)))
		defer span.End()
	}

	// --- Cache Path ---
	// 1. Check Block Cache
	if s.blockCache != nil {
		cacheKey := fmt.Sprintf("%d-%d", s.id, offset)
		if cachedVal, found := s.blockCache.Get(cacheKey); found {
			if span != nil {
				span.SetAttributes(attribute.Bool("cache.hit", true))
			}
			if data, ok := cachedVal.([]byte); ok {
				// Create a new block from the cached (and already decompressed) data.
				// The block itself is ephemeral and not returned to any pool.
				return NewBlock(data), nil
			}
			return nil, fmt.Errorf("invalid type in block cache for key %s", cacheKey)
		}

		if span != nil {
			span.SetAttributes(attribute.Bool("cache.hit", false))
		}
		// If cache miss, fall through to the read logic below.
	}

	// --- Direct Read / Cache Miss Path ---
	// 2. Read and Verify Raw Block from Disk
	compressedPayload, compressionType, err := s.readAndVerifyRawBlock(offset, length)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "read_raw_block_failed")
		}
		return nil, err
	}

	// 3. Decompress Block
	decompressedBytes, err := s.decompressBlock(compressedPayload, compressionType, offset)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "decompress_block_failed")
		}
		return nil, err
	}

	// 4. Update Cache
	if s.blockCache != nil {
		cacheKey := fmt.Sprintf("%d-%d", s.id, offset)
		// Create a copy for the cache because the slice's backing array might be reused.
		dataToCache := make([]byte, len(decompressedBytes))
		copy(dataToCache, decompressedBytes)
		s.blockCache.Put(cacheKey, dataToCache)
		if span != nil {
			span.SetAttributes(attribute.Int("cache.put_block_size", len(dataToCache)))
		}
	}

	return NewBlock(decompressedBytes), nil
}

// readAndVerifyRawBlock reads a raw block from disk and verifies its checksum.
// It returns the compressed data payload and its compression type.
func (s *SSTable) readAndVerifyRawBlock(offset int64, length uint32) ([]byte, core.CompressionType, error) {
	// There are two block layouts in the codebase:
	// Legacy writer format: [compType (1 byte)][checksum (4 bytes)][compressed payload]
	// V2 block writer format: [compressed payload][trailer (13 bytes)]
	// trailer: checksum u32 | uncompLen u32 | compLen u32 | compType u8
	const trailerSize = 13

	if length < 5 { // Must at least hold legacy compFlag + checksum
		return nil, 0, fmt.Errorf("block length %d is too small to be valid (offset: %d): %w", length, offset, ErrCorrupted)
	}

	readBuffer := make([]byte, length)
	if _, err := s.file.ReadAt(readBuffer, offset); err != nil {
		return nil, 0, err
	}

	// Try legacy layout first: compType at byte 0, checksum at bytes [1:5]
	legacyCompType := readBuffer[0]
	legacyChecksum := binary.LittleEndian.Uint32(readBuffer[1:5])
	legacyCompressedPayload := readBuffer[5:]
	if crc32.ChecksumIEEE(legacyCompressedPayload) == legacyChecksum {
		return legacyCompressedPayload, core.CompressionType(legacyCompType), nil
	}

	// Try v2 layout: trailer at the end
	if length < trailerSize {
		return nil, 0, fmt.Errorf("block length %d is too small to include trailer (offset: %d): %w", length, offset, ErrCorrupted)
	}
	trailer := readBuffer[len(readBuffer)-trailerSize:]
	storedChecksum := binary.LittleEndian.Uint32(trailer[0:4])
	compType := trailer[12]
	compressedPayload := readBuffer[:len(readBuffer)-trailerSize]
	if crc32.ChecksumIEEE(compressedPayload) != storedChecksum {
		return nil, 0, fmt.Errorf("checksum mismatch for block at offset %d: %w", offset, ErrCorrupted)
	}
	return compressedPayload, core.CompressionType(compType), nil
}

// decompressBlock takes compressed block data and returns the decompressed bytes.
func (s *SSTable) decompressBlock(data []byte, compressionType core.CompressionType, offset int64) ([]byte, error) {
	decompressionBuffer := core.BufferPool.Get()
	defer core.BufferPool.Put(decompressionBuffer)

	decompressor, err := GetCompressor(compressionType)
	if err != nil {
		return nil, fmt.Errorf("failed to get decompressor for block at offset %d: %w", offset, err)
	}

	decompressReader, err := decompressor.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress block at offset %d: %w", offset, err)
	}
	defer decompressReader.Close()

	if _, err := io.Copy(decompressionBuffer, decompressReader); err != nil {
		return nil, fmt.Errorf("failed to copy decompressed data to buffer: %w", err)
	}

	// Return a copy because the buffer will be reset and reused by the pool.
	decompressedCopy := make([]byte, len(decompressionBuffer.Bytes()))
	copy(decompressedCopy, decompressionBuffer.Bytes())
	return decompressedCopy, nil
}

// NewIterator creates an iterator for scanning entries within the SSTable.
// Supports range scans [startKey, endKey).
// Corresponds to FR4.7 and integrates with Block Cache (FR4.8).
// The semaphore `sem` is optional and used to limit concurrency during block reads, primarily for compaction.
func (s *SSTable) NewIterator(startKey, endKey []byte, sem chan struct{}, order types.SortOrder) (core.IteratorInterface[*core.IteratorNode], error) {
	// FR4.7: Create and return an instance of sstableIterator.
	// The iterator will handle seeking to the startKey using the index internally
	// and will use s.blockCache for reading blocks (FR4.8).
	// NewSSTableIterator is defined in iterator.go.
	if s.closed.Load() {
		return nil, ErrClosed
	}
	return NewSSTableIterator(s, startKey, endKey, sem, order)
}

// GetIndex returns the in-memory index of the SSTable.
// This is useful for testing or debugging purposes.
func (s *SSTable) GetIndex() *Index {
	if s.closed.Load() {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

// Close closes the SSTable file.
func (s *SSTable) Close() error {
	// Atomically check and set the closed flag to make Close idempotent.
	if !s.closed.CompareAndSwap(false, true) {
		return ErrClosed // Already closed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.file.Close()
		s.file = nil // Mark as nil to prevent further use
		return err
	}
	return nil // Should not be reached if closed flag is handled correctly, but safe to keep.
}

// MinKey returns the minimum key in the SSTable (FR4.4).
func (s *SSTable) MinKey() []byte {
	return s.minKey
}

// MaxKey returns the maximum key in the SSTable (FR4.4).
func (s *SSTable) MaxKey() []byte {
	return s.maxKey
}

// Size returns the file size of the SSTable in bytes (FR4.5).
func (s *SSTable) Size() int64 {
	return s.size
}

// ID returns the unique identifier of the SSTable.
func (s *SSTable) ID() uint64 {
	return s.id
}

// KeyCount returns the total number of entries in the SSTable.
func (s *SSTable) KeyCount() uint64 {
	return s.keyCount
}

// TombstoneCount returns the total number of tombstone entries in the SSTable.
func (s *SSTable) TombstoneCount() uint64 {
	return s.tombstoneCount
}

// FilePath returns the path to the SSTable file.
func (s *SSTable) FilePath() string {
	return s.filePath
}

// VerifyIntegrity checks the internal consistency of the SSTable metadata.
// This corresponds to parts of FR5.3.
// The deepCheck parameter controls whether to perform potentially expensive checks like full Bloom filter verification.
func (s *SSTable) VerifyIntegrity(deepCheck bool) []error {
	if s.closed.Load() {
		return []error{ErrClosed}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if deepCheck && s.file == nil {
		return []error{ErrClosed}
	}
	var errs []error

	// 1. Check MinKey <= MaxKey (if both are non-nil)
	if s.minKey != nil && s.maxKey != nil {
		if bytes.Compare(s.minKey, s.maxKey) > 0 {
			errs = append(errs, fmt.Errorf("SSTable ID %d: MinKey %s > MaxKey %s",
				s.id, string(s.minKey), string(s.maxKey)))
		}
	}

	// 2. Verify index entries are sorted by FirstKey (strictly increasing)
	if s.index != nil {
		for i := 0; i < len(s.index.entries)-1; i++ {
			// FirstKey should not be nil for valid index entries
			if s.index.entries[i].FirstKey == nil || s.index.entries[i+1].FirstKey == nil {
				errs = append(errs, fmt.Errorf("SSTable ID %d: Index entry %d or %d has nil FirstKey", s.id, i, i+1))
				continue
			}
			if bytes.Compare(s.index.entries[i].FirstKey, s.index.entries[i+1].FirstKey) >= 0 {
				errs = append(errs, fmt.Errorf("SSTable ID %d: Index not sorted correctly at entry %d. Key1: %s, Key2: %s",
					s.id, i, string(s.index.entries[i].FirstKey), string(s.index.entries[i+1].FirstKey)))
			}
		}
		// 3. Verify MinKey from metadata matches the first key in the first index entry (if index exists and is not empty)
		if len(s.index.entries) > 0 && s.minKey != nil && s.index.entries[0].FirstKey != nil {
			if !bytes.Equal(s.minKey, s.index.entries[0].FirstKey) {
				errs = append(errs, fmt.Errorf("SSTable ID %d: Stored MinKey %s does not match first key in index %s",
					s.id, string(s.minKey), string(s.index.entries[0].FirstKey)))
			}
		}

		// 4. Verify index entries' FirstKey against actual block data (Deep Check)
		if deepCheck {
			// This can be resource-intensive as it involves reading blocks.
			for i, indexEntry := range s.index.entries {
				block, err := s.readBlock(indexEntry.BlockOffset, indexEntry.BlockLength, nil) // Uses cache if available
				if err != nil {
					errs = append(errs, fmt.Errorf("SSTable ID %d: Failed to read block for index entry %d (offset %d): %w",
						s.id, i, indexEntry.BlockOffset, err))
					continue
				}
				blockIter := NewBlockIterator(block.getEntriesData())
				if blockIter.Next() {
					actualFirstKeyInBlock := blockIter.Key()
					if !bytes.Equal(indexEntry.FirstKey, actualFirstKeyInBlock) {
						errs = append(errs, fmt.Errorf("SSTable ID %d: Index entry %d FirstKey %s mismatch with actual block first key %s",
							s.id, i, string(indexEntry.FirstKey), string(actualFirstKeyInBlock)))
					}
				} else if blockIter.Error() != nil {
					errs = append(errs, fmt.Errorf("SSTable ID %d: Error iterating first entry of block for index entry %d: %w",
						s.id, i, blockIter.Error()))
				} else {
					// Block is empty, but index entry exists. This might be an issue depending on writer logic.
					// If writer ensures blocks are non-empty if indexed, this is an error.
					errs = append(errs, fmt.Errorf("SSTable ID %d: Index entry %d points to an empty or unreadable block", s.id, i))
				}
			}
		}

		// 5. Verify Bloom Filter integrity (Deep Check - no false negatives)
		if deepCheck {
			if bfErrs := s.VerifyBloomFilterIntegrity(); len(bfErrs) > 0 {
				errs = append(errs, bfErrs...)
			}
		}
	}
	return errs
}

// VerifyBloomFilterIntegrity performs a deep check of the Bloom filter
// by iterating all keys and ensuring no false negatives.
func (s *SSTable) VerifyBloomFilterIntegrity() []error {
	var errs []error
	if s.filter == nil { // Should not happen for a loaded SSTable
		return []error{fmt.Errorf("SSTable ID %d: Bloom filter is nil during integrity check", s.id)}
	}
	iter, _ := NewSSTableIterator(s, nil, nil, nil, types.Ascending) // Full scan without semaphore
	for iter.Next() {
		// key, _, _, _ := iter.At()
		cur, _ := iter.At()
		key := cur.Key
		if !s.filter.Contains(key) {
			errs = append(errs, fmt.Errorf("SSTable ID %d: Bloom filter returned false negative for key %s", s.id, string(key)))
		}
	}
	if iter.Error() != nil {
		errs = append(errs, fmt.Errorf("SSTable ID %d: Error during bloom filter integrity scan: %w", s.id, iter.Error()))
	}
	return errs
}
