package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
)

// TestChecksumTamper writes a small SSTable with the BlockWriter, then corrupts
// a byte inside the first data block to trigger a checksum mismatch on read.
func TestChecksumTamper(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "corrupt.sst")
	bw, err := NewBlockWriter(path, BlockWriterOptions{TargetBlockSize: 64, Compression: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string][]byte{
		"k1": []byte("v1"),
		"k2": []byte("v2"),
	}
	for k, v := range entries {
		if err := bw.Add([]byte(k), v); err != nil {
			bw.Close()
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	// Open file and read footer to locate first block offset
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// read footer
	fi, _ := f.Stat()
	if fi.Size() < 64 {
		t.Fatalf("file too small")
	}
	if _, err := f.Seek(-64, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	footer := make([]byte, 64)
	if _, err := io.ReadFull(f, footer); err != nil {
		t.Fatal(err)
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int(binary.LittleEndian.Uint32(footer[8:12]))
	// read index
	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	indexBuf := make([]byte, indexLen)
	if _, err := io.ReadFull(f, indexBuf); err != nil {
		t.Fatal(err)
	}
	// decode first entry to get block offset
	off := 0
	v, n := binary.Uvarint(indexBuf[off:])
	off += n
	firstKeyLen := int(v)
	off += firstKeyLen
	blockOffset := int64(binary.LittleEndian.Uint64(indexBuf[off : off+8]))
	// corrupt one byte inside block payload
	if _, err := f.Seek(blockOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(f, b); err != nil {
		t.Fatal(err)
	}
	// flip a bit
	b[0] ^= 0xFF
	if _, err := f.Seek(blockOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	// now open reader and expect an error when Get is called
	br, err := OpenBlockReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	_, err = br.Get([]byte("k1"))
	if err == nil {
		t.Fatalf("expected checksum error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum") && !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("expected checksum/corrupt error, got: %v", err)
	}
}

// TestVeryLargeValue ensures writer/reader handle a very large single value.
func TestVeryLargeValue(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "largeval.sst")
	bw, err := NewBlockWriter(path, BlockWriterOptions{TargetBlockSize: 64 * 1024, Compression: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	large := make([]byte, 2*1024*1024) // 2 MiB
	for i := range large {
		large[i] = byte(i % 256)
	}
	if err := bw.Add([]byte("bigkey"), large); err != nil {
		bw.Close()
		t.Fatal(err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	br, err := OpenBlockReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	got, err := br.Get([]byte("bigkey"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(large) {
		t.Fatalf("len mismatch got %d want %d", len(got), len(large))
	}
	// spot-check a few bytes
	if got[0] != large[0] || got[len(got)-1] != large[len(large)-1] {
		t.Fatalf("content mismatch at edges")
	}
}

// TestMixedFormatReads ensures legacy-format SSTables and v2 block SSTables can both be opened/read
func TestMixedFormatReads(t *testing.T) {
	d := t.TempDir()
	v2Path := filepath.Join(d, "v2.sst")
	// create a legacy-format sstable using existing sstable writer API (writer.go)
	writerOpts := core.SSTableWriterOptions{DataDir: d, ID: 1, Compressor: &compressors.NoCompressionCompressor{}, Logger: slog.Default(), BloomFilterFalsePositiveRate: 0.01}
	w, err := NewSSTableWriter(writerOpts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("lk"), []byte("lv"), core.EntryTypePutEvent, 1); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	// create v2 using BlockWriter
	bw, err := NewBlockWriter(v2Path, BlockWriterOptions{TargetBlockSize: 64, Compression: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := bw.Add([]byte("bk"), []byte("bv")); err != nil {
		bw.Close()
		t.Fatal(err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	// Open both using the higher-level OpenSSTable function (existing code should detect format)
	// Try legacy
	sstLegacy, err := LoadSSTable(LoadSSTableOptions{FilePath: w.FilePath(), ID: 1, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	defer sstLegacy.Close()
	val, _, err := sstLegacy.Get([]byte("lk"))
	if err != nil {
		t.Fatalf("legacy get: %v", err)
	}
	if !bytes.Equal(val, []byte("lv")) {
		t.Fatalf("legacy value mismatch")
	}
	// v2 via OpenSSTable
	br, err := OpenBlockReader(v2Path)
	if err != nil {
		t.Fatalf("open v2: %v", err)
	}
	defer br.Close()
	val2, err := br.Get([]byte("bk"))
	if err != nil {
		t.Fatalf("v2 get: %v", err)
	}
	if !bytes.Equal(val2, []byte("bv")) {
		t.Fatalf("v2 value mismatch")
	}
}
