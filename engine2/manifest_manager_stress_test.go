//go:build longtest
// +build longtest

package engine2

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestManifestManagerHighContention spawns many goroutines calling AddEntry
// concurrently to exercise the background writer queue under contention.
func TestManifestManagerHighContention(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sstables", "manifest.bin")

	mgr, err := NewManifestManager(manifestPath)
	if err != nil {
		t.Fatalf("NewManifestManager failed: %v", err)
	}
	defer mgr.Close()

	var wg sync.WaitGroup
	var errFlag atomic.Uint32
	var idCounter uint64

	goroutines := 50
	perGoroutine := 200

	start := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gidx int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := atomic.AddUint64(&idCounter, 1)
				e := SSTableManifestEntry{
					ID:        id,
					FilePath:  fmt.Sprintf("%s/sst-%d-%d.sst", tmpDir, gidx, i),
					KeyCount:  uint64(id % 100),
					CreatedAt: time.Now().UTC(),
				}
				if err := mgr.AddEntry(e); err != nil {
					errFlag.Store(1)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if errFlag.Load() != 0 {
		t.Fatalf("one or more AddEntry calls failed")
	}

	expected := goroutines * perGoroutine
	got := mgr.ListEntries()
	if len(got) != expected {
		t.Fatalf("expected %d entries after concurrent adds, got %d (elapsed=%s)", expected, len(got), elapsed)
	}

	// verify persisted manifest matches in-memory
	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if len(loaded.Entries) != expected {
		t.Fatalf("persisted manifest entry count mismatch: got %d want %d", len(loaded.Entries), expected)
	}
}
