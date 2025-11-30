package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

// Minimal block-oriented SSTable writer.
// This implementation is intentionally small for initial integration and tests.
const (
	footerMagic            = "NBIDXv2\000"
	defaultTargetBlockSize = 32 * 1024
)

type CompressionType uint8

const (
	CompressionNone CompressionType = 0
)

type BlockWriterOptions struct {
	TargetBlockSize int
	Compression     CompressionType
}

type blockIndexEntry struct {
	firstKey  []byte
	offset    uint64
	compLen   uint32
	uncompLen uint32
}

type BlockWriter struct {
	f        *os.File
	path     string
	opts     BlockWriterOptions
	buf      []byte // uncompressed buffer for current block
	entries  int
	index    []blockIndexEntry
	firstKey []byte
	offset   uint64
	// keys holds copies of every key written to this SSTable so we can
	// build a Bloom filter at Close time.
	keys [][]byte
}

func NewBlockWriter(path string, opts BlockWriterOptions) (*BlockWriter, error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	if opts.TargetBlockSize <= 0 {
		opts.TargetBlockSize = defaultTargetBlockSize
	}
	bw := &BlockWriter{f: f, path: path, opts: opts}
	bw.buf = make([]byte, 0, opts.TargetBlockSize)
	bw.offset = 0
	return bw, nil
}

func (bw *BlockWriter) Add(key []byte, value []byte) error {
	// Serialize entry as uvarint keylen + key + uvarint vallen + value
	k := putUvarint(nil, uint64(len(key)))
	bw.buf = append(bw.buf, k...)
	bw.buf = append(bw.buf, key...)
	v := putUvarint(nil, uint64(len(value)))
	bw.buf = append(bw.buf, v...)
	bw.buf = append(bw.buf, value...)

	if bw.entries == 0 {
		bw.firstKey = append([]byte(nil), key...)
	}
	bw.entries++

	// Keep a copy of the key for Bloom filter construction at Close().
	kcopy := make([]byte, len(key))
	copy(kcopy, key)
	bw.keys = append(bw.keys, kcopy)

	if len(bw.buf) >= bw.opts.TargetBlockSize {
		if err := bw.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (bw *BlockWriter) Close() error {
	// flush remaining
	if bw.entries > 0 {
		if err := bw.flushBlock(); err != nil {
			bw.f.Close()
			return err
		}
	}
	// write index block
	indexOffset, err := bw.f.Seek(0, io.SeekCurrent)
	if err != nil {
		bw.f.Close()
		return err
	}
	var indexBuf []byte
	for _, e := range bw.index {
		l := putUvarint(nil, uint64(len(e.firstKey)))
		indexBuf = append(indexBuf, l...)
		indexBuf = append(indexBuf, e.firstKey...)
		var off [8]byte
		binary.LittleEndian.PutUint64(off[:], e.offset)
		indexBuf = append(indexBuf, off[:]...)
		var ci [4]byte
		binary.LittleEndian.PutUint32(ci[:], e.compLen)
		indexBuf = append(indexBuf, ci[:]...)
		binary.LittleEndian.PutUint32(ci[:], e.uncompLen)
		indexBuf = append(indexBuf, ci[:]...)
	}
	if _, err := bw.f.Write(indexBuf); err != nil {
		bw.f.Close()
		return err
	}
	// bloom filter block (serialize keys into a BloomFilter and write)
	bloomOffset, bloomLen := int64(0), int32(0)
	if len(bw.keys) > 0 {
		bf, berr := NewBloomFilter(uint64(len(bw.keys)), 0.01)
		if berr == nil {
			for _, k := range bw.keys {
				bf.Add(k)
			}
			bloomData := bf.Bytes()
			bloomOffset, err = bw.f.Seek(0, io.SeekCurrent)
			if err != nil {
				bw.f.Close()
				return err
			}
			n, err := bw.f.Write(bloomData)
			if err != nil {
				bw.f.Close()
				return err
			}
			bloomLen = int32(n)
		}
	}

	// meta block (minimal)
	metaOffset, err := bw.f.Seek(0, io.SeekCurrent)
	if err != nil {
		bw.f.Close()
		return err
	}
	meta := []byte("{\"format_version\":2,\"created_by\":\"TagIndexManager2\"}\n")
	if _, err := bw.f.Write(meta); err != nil {
		bw.f.Close()
		return err
	}
	// footer (fixed 64 bytes)
	footer := make([]byte, 64)
	// Layout:
	// 0:8   indexOffset (uint64)
	// 8:12  indexLen (uint32)
	// 12:20 bloomOffset (uint64)
	// 20:24 bloomLen (uint32)
	// 24:32 metaOffset (uint64)
	// 32:36 metaLen (uint32)
	// 36:40 format_version (uint32)
	// 40:56 reserved/padding
	// 56:64 footerMagic (8 bytes)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(len(indexBuf)))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(bloomLen))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(metaOffset))
	binary.LittleEndian.PutUint32(footer[32:36], uint32(len(meta)))
	binary.LittleEndian.PutUint32(footer[36:40], 2) // format_version
	copy(footer[56:64], []byte(footerMagic))
	if _, err := bw.f.Write(footer); err != nil {
		bw.f.Close()
		return err
	}
	if err := bw.f.Sync(); err != nil {
		bw.f.Close()
		return err
	}
	if err := bw.f.Close(); err != nil {
		return err
	}
	// atomically rename
	if err := os.Rename(bw.f.Name(), bw.path); err != nil {
		return err
	}
	return nil
}

// Finish is an alias to Close to satisfy writer-like interfaces.
func (bw *BlockWriter) Finish() error {
	return bw.Close()
}

// Abort attempts to clean up a partially-written temporary file.
func (bw *BlockWriter) Abort() error {
	// Close underlying file if open
	if bw.f != nil {
		_ = bw.f.Close()
		bw.f = nil
	}
	// Remove temporary file (path + ".tmp")
	tmp := bw.path + ".tmp"
	// Best-effort removal
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// FilePath returns the final path that this writer will publish on Finish().
func (bw *BlockWriter) FilePath() string {
	return bw.path
}

// CurrentSize returns the number of bytes written so far (data blocks + trailers).
func (bw *BlockWriter) CurrentSize() int64 {
	return int64(bw.offset)
}

func (bw *BlockWriter) flushBlock() error {
	// Parse entries from the temporary buffer (format: uvarint keylen + key + uvarint vallen + value)
	var entries []struct {
		key []byte
		val []byte
	}
	r := bytes.NewReader(bw.buf)
	for r.Len() > 0 {
		klen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		k := make([]byte, klen)
		if _, err := io.ReadFull(r, k); err != nil {
			return err
		}
		vlen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		v := make([]byte, vlen)
		if _, err := io.ReadFull(r, v); err != nil {
			return err
		}
		entries = append(entries, struct {
			key []byte
			val []byte
		}{key: append([]byte(nil), k...), val: append([]byte(nil), v...)})
	}

	// Build block data with restart points and entry encoding expected by BlockIterator
	var blockBuf bytes.Buffer
	restartInterval := 16
	var restartOffsets []uint32
	var previousKey []byte
	entriesSinceRestart := 0

	for _, e := range entries {
		if entriesSinceRestart == 0 {
			// record restart offset
			restartOffsets = append(restartOffsets, uint32(blockBuf.Len()))
		}

		// compute shared prefix with previousKey
		shared := 0
		if entriesSinceRestart != 0 {
			maxShared := len(e.key)
			if len(previousKey) < maxShared {
				maxShared = len(previousKey)
			}
			for i := 0; i < maxShared; i++ {
				if e.key[i] != previousKey[i] {
					break
				}
				shared++
			}
		}
		if entriesSinceRestart == 0 {
			shared = 0 // restart point
		}

		unshared := len(e.key) - shared
		// write shared_len, unshared_len, value_len (uvarint)
		blockBuf.Write(putUvarint(nil, uint64(shared)))
		blockBuf.Write(putUvarint(nil, uint64(unshared)))
		blockBuf.Write(putUvarint(nil, uint64(len(e.val))))
		// entry_type (use PutEvent 'E')
		blockBuf.WriteByte(byte(core.EntryTypePutEvent))
		// point_id (0)
		blockBuf.Write(putUvarint(nil, uint64(0)))
		// write unshared key and value
		blockBuf.Write(e.key[shared:])
		blockBuf.Write(e.val)

		previousKey = append(previousKey[:0], e.key...)
		entriesSinceRestart++
		if entriesSinceRestart >= restartInterval {
			entriesSinceRestart = 0
		}
	}

	// append restart offsets
	for _, ro := range restartOffsets {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], ro)
		blockBuf.Write(tmp[:])
	}
	// append num restarts
	var tmpNum [4]byte
	binary.LittleEndian.PutUint32(tmpNum[:], uint32(len(restartOffsets)))
	blockBuf.Write(tmpNum[:])

	comp := blockBuf.Bytes()
	compLen := uint32(len(comp))
	uncompLen := uint32(len(comp))

	// write block
	n, err := bw.f.Write(comp)
	if err != nil {
		return err
	}
	blockOffset := bw.offset
	bw.offset += uint64(n)
	// write trailer: checksum u32, uncomp u32, comp u32, compType u8
	checksum := crc32.ChecksumIEEE(comp)
	var tri [13]byte
	binary.LittleEndian.PutUint32(tri[0:4], checksum)
	binary.LittleEndian.PutUint32(tri[4:8], uncompLen)
	binary.LittleEndian.PutUint32(tri[8:12], compLen)
	tri[12] = byte(CompressionNone)
	if _, err := bw.f.Write(tri[:]); err != nil {
		return err
	}
	bw.offset += uint64(len(tri))
	// record index entry
	bw.index = append(bw.index, blockIndexEntry{firstKey: append([]byte(nil), bw.firstKey...), offset: blockOffset, compLen: compLen, uncompLen: uncompLen})
	// reset buffer
	bw.buf = bw.buf[:0]
	bw.entries = 0
	bw.firstKey = nil
	return nil
}

// helpers
func putUvarint(dst []byte, v uint64) []byte {
	if dst == nil {
		dst = make([]byte, 0, 10)
	}
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

// End of file
