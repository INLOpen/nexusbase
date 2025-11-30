package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type BlockReader struct {
	f     *os.File
	index []blockIndexEntry
}

func OpenBlockReader(path string) (*BlockReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// read footer (64 bytes from EOF)
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < 64 {
		f.Close()
		return nil, io.ErrUnexpectedEOF
	}
	if _, err := f.Seek(-64, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	footer := make([]byte, 64)
	if _, err := io.ReadFull(f, footer); err != nil {
		f.Close()
		return nil, err
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int(binary.LittleEndian.Uint32(footer[8:12]))
	// read index
	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	indexBuf := make([]byte, indexLen)
	if _, err := io.ReadFull(f, indexBuf); err != nil {
		f.Close()
		return nil, err
	}
	idx := make([]blockIndexEntry, 0)
	off := 0
	for off < len(indexBuf) {
		v, n := binary.Uvarint(indexBuf[off:])
		if n <= 0 {
			f.Close()
			return nil, io.ErrUnexpectedEOF
		}
		off += n
		if off+int(v) > len(indexBuf) {
			f.Close()
			return nil, io.ErrUnexpectedEOF
		}
		firstKey := make([]byte, v)
		copy(firstKey, indexBuf[off:off+int(v)])
		off += int(v)
		if off+8+4+4 > len(indexBuf) {
			f.Close()
			return nil, io.ErrUnexpectedEOF
		}
		offset := binary.LittleEndian.Uint64(indexBuf[off : off+8])
		off += 8
		compLen := binary.LittleEndian.Uint32(indexBuf[off : off+4])
		off += 4
		uncompLen := binary.LittleEndian.Uint32(indexBuf[off : off+4])
		off += 4
		idx = append(idx, blockIndexEntry{firstKey: firstKey, offset: offset, compLen: compLen, uncompLen: uncompLen})
	}
	br := &BlockReader{f: f, index: idx}
	return br, nil
}

func (br *BlockReader) Close() error {
	return br.f.Close()
}

func (br *BlockReader) Get(key []byte) ([]byte, error) {
	// binary search index by firstKey; find last entry with firstKey <= key
	lo, hi := 0, len(br.index)-1
	var foundBlock int
	for lo <= hi {
		mid := (lo + hi) / 2
		if string(br.index[mid].firstKey) <= string(key) {
			foundBlock = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	// read block at index[foundBlock]
	e := br.index[foundBlock]
	if _, err := br.f.Seek(int64(e.offset), io.SeekStart); err != nil {
		return nil, err
	}
	comp := make([]byte, e.compLen)
	if _, err := io.ReadFull(br.f, comp); err != nil {
		return nil, err
	}
	// read trailer
	tri := make([]byte, 13)
	if _, err := io.ReadFull(br.f, tri); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint32(tri[0:4])
	_ = binary.LittleEndian.Uint32(tri[4:8]) // uncompLen (not used by reader)
	// verify checksum
	if crc32.ChecksumIEEE(comp) != checksum {
		return nil, errors.New("block checksum mismatch")
	}

	// Construct a Block from the raw block payload and use its Find method
	// (which understands restart-point encoding) to locate the key.
	blk := NewBlock(comp)
	val, _, _, err := blk.Find(key)
	if err != nil {
		return nil, err
	}
	// return a copy
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

// simple iterator not implemented yet
