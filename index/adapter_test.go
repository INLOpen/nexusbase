package index

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/RoaringBitmap/roaring/roaring64"
)

func TestWriteTagIndexSSTable(t *testing.T) {
	dir := t.TempDir()

	// Prepare mapping: one key => bitmap
	key := indexer.EncodeTagIndexKey(1, 2)
	bm := roaring64.New()
	bm.Add(10)
	bm.Add(20)

	mapping := map[string]*roaring64.Bitmap{
		string(key): bm,
	}

	factory := func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
		return sstable.NewSSTableWriter(opts)
	}

	cfg := AdapterConfig{SSTableWriterFactory: factory, DataDir: dir}

	path, id, err := WriteTagIndexSSTable(cfg, mapping, 2)
	if err != nil {
		t.Fatalf("WriteTagIndexSSTable failed: %v", err)
	}

	t.Logf("sst path: %s", path)
	if _, err := os.Stat(path); err != nil {
		// Debug: list files under dir
		t.Logf("Listing files under %s for debugging:", dir)
		filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && d != nil {
				t.Log(p)
			}
			return nil
		})
		t.Fatalf("sst file not found at path %s: %v", path, err)
	}

	sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: path, ID: id})
	if err != nil {
		t.Fatalf("LoadSSTable failed: %v", err)
	}

	val, et, err := sst.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if et != core.EntryTypePutEvent {
		t.Fatalf("unexpected entry type: %v", et)
	}

	// Deserialize roaring bitmap
	bm2 := roaring64.New()
	if _, err := bm2.ReadFrom(bytes.NewReader(val)); err != nil {
		t.Fatalf("bitmap ReadFrom failed: %v", err)
	}

	if !bm.Equals(bm2) {
		t.Fatalf("bitmap mismatch; got %v want %v", bm2.String(), bm.String())
	}
}
