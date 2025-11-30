package sstable

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestBlockFormatWriteLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")

	bw, err := NewBlockWriter(path, BlockWriterOptions{TargetBlockSize: 1024})
	if err != nil {
		t.Fatalf("new block writer: %v", err)
	}
	key := []byte("test-key")
	val := []byte("test-val")
	if err := bw.Add(key, val); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := bw.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}

	sst, err := LoadSSTable(LoadSSTableOptions{FilePath: path, ID: 1})
	if err != nil {
		t.Fatalf("load sstable: %v", err)
	}
	defer sst.Close()

	// Inspect index entries
	idx := sst.GetIndex()
	if idx == nil {
		t.Fatalf("index nil")
	}
	entries := idx.GetEntries()
	if len(entries) == 0 {
		t.Fatalf("no index entries found")
	}

	// Try an iterator over the whole table to surface parsed keys
	it, err := sst.NewIterator(nil, nil, nil, 0)
	if err != nil {
		t.Fatalf("new iterator: %v", err)
	}
	var found bool
	for it.Next() {
		node, _ := it.At()
		if bytes.Equal(node.Key, key) {
			if !bytes.Equal(node.Value, val) {
				t.Fatalf("iterator value mismatch: got %v want %v", node.Value, val)
			}
			found = true
			break
		}
	}
	if it.Error() != nil {
		t.Fatalf("iterator error: %v", it.Error())
	}
	if !found {
		t.Fatalf("key not found via iterator")
	}
}
