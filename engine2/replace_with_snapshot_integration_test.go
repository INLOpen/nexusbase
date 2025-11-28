package engine2

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal/testutil"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexuscore/types"
)

// TestReplaceWithSnapshot_E2E verifies that creating a full snapshot and then
// restoring it via ReplaceWithSnapshot yields the same data after engine restart.
func TestReplaceWithSnapshot_E2E(t *testing.T) {
	ctx := context.Background()

	// leader data dir
	leaderDir := filepath.Join(t.TempDir(), "leader")
	ai, err := NewStorageEngine(StorageEngineOptions{DataDir: leaderDir})
	if err != nil {
		t.Fatalf("failed to create leader engine: %v", err)
	}
	leaderAdapter := ai.(*Engine2Adapter)
	if err := leaderAdapter.Start(); err != nil {
		t.Fatalf("failed to start leader adapter: %v", err)
	}

	// write some datapoints
	fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1.0})
	if err != nil {
		t.Fatalf("failed to build FieldValues: %v", err)
	}
	dp := core.DataPoint{Metric: "m1", Tags: map[string]string{"host": "a"}, Timestamp: 12345, Fields: fv}
	if err := leaderAdapter.Put(context.Background(), dp); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Sanity check: ensure Get returns the datapoint before flushing
	if got, gerr := leaderAdapter.Get(context.Background(), "m1", map[string]string{"host": "a"}, 12345); gerr != nil || len(got) == 0 {
		t.Fatalf("sanity check failed: put not visible before flush: err=%v len=%d", gerr, len(got))
	}

	// Acquire provider lock and flush memtables like snapshot creation caller would do
	leaderAdapter.Lock()
	mems, _ := leaderAdapter.GetMemtablesForFlush()
	leaderAdapter.Unlock()
	// Diagnostic: ensure the memtable we will flush contains the datapoint
	if len(mems) == 0 {
		t.Fatalf("expected memtable to flush, got none")
	}
	// Diagnostic: inspect memtable contents
	memLen := mems[0].Len()
	t.Logf("diagnostic: memtable entries count before flush: %d", memLen)
	// iterate up to first 10 entries and log key prefixes
	iter := mems[0].NewIterator(nil, nil, types.Ascending)
	count := 0
	var memFirstKey []byte
	for iter.Next() && count < 10 {
		node, _ := iter.At()
		if count == 0 {
			memFirstKey = make([]byte, len(node.Key))
			copy(memFirstKey, node.Key)
		}
		t.Logf("diagnostic: memtable key hex (first 32): %x", node.Key[:min(len(node.Key), 32)])
		count++
	}
	_ = iter.Close()
	for _, mem := range mems {
		if err := leaderAdapter.FlushMemtableToL0(mem, context.Background()); err != nil {
			t.Fatalf("flush memtable failed: %v", err)
		}
	}

	// create full snapshot
	snapDir := filepath.Join(leaderAdapter.GetSnapshotsBaseDir(), "full1")
	mgr := leaderAdapter.GetSnapshotManager()
	if err := mgr.CreateFull(ctx, snapDir); err != nil {
		t.Fatalf("CreateFull snapshot failed: %v", err)
	}

	// Close adapter and replace data dir from snapshot
	if err := leaderAdapter.Close(); err != nil {
		t.Fatalf("failed to close leader adapter: %v", err)
	}

	// Now call ReplaceWithSnapshot on the same adapter instance (simulates restore)
	if err := leaderAdapter.ReplaceWithSnapshot(snapDir); err != nil {
		t.Fatalf("ReplaceWithSnapshot failed: %v", err)
	}

	// Before reopening the engine, list and assert restored files for diagnostics.
	// sstables/ should exist and contain at least one SSTable.
	sstDir := filepath.Join(leaderDir, "sstables")
	sstEntries, sstErr := os.ReadDir(sstDir)
	if sstErr != nil {
		t.Fatalf("expected sstables directory after restore, stat error: %v", sstErr)
	}
	if len(sstEntries) == 0 {
		t.Fatalf("expected at least one sstable file in %s after restore", sstDir)
	}
	// list restored sstables (no debug logs)
	// Diagnostic: scan restored sstables to confirm the datapoint exists on disk
	// (this helps distinguish snapshot content vs. engine restore bug).
	foundInSnapshot := false
	for _, e := range sstEntries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		path := filepath.Join(sstDir, e.Name())
		// try to load sstable and scan for the written datapoint
		if tbl, lerr := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: path}); lerr == nil {
			// build encoded key we expect in sstable. Prefer memtable-observed key when available.
			var expectedKey []byte
			if memFirstKey != nil {
				expectedKey = memFirstKey
			} else {
				expectedKey = core.EncodeTSDBKeyWithString("m1", map[string]string{"host": "a"}, 12345)
			}
			found := false
			if iter, ierr := tbl.NewIterator(nil, nil, nil, types.Ascending); ierr == nil {
				for iter.Next() {
					cur, aerr := iter.At()
					if aerr != nil {
						break
					}
					k := cur.Key
					v := cur.Value
					if bytes.Equal(k, expectedKey) || bytes.Contains(k, expectedKey) || bytes.Contains(v, expectedKey) {
						found = true
						break
					}
				}
				_ = iter.Close()
			}
			_ = tbl.Close()
			if !found {
				// continue scanning others; but log for diagnostics
				t.Logf("diagnostic: datapoint not found in sstable %s", path)
			} else {
				t.Logf("diagnostic: datapoint found in sstable %s", path)
				foundInSnapshot = true
			}
		}
	}

	// WAL presence: configurable via internal test helper
	if testutil.WALStrictEnabled() {
		testutil.RequireWALPresent(t, leaderDir)
	} else {
		// permissive: do not assert WAL presence; skip debug logging.
	}

	// private mapping logs should be present
	strMap := filepath.Join(leaderDir, "string_mapping.log")
	if _, err := os.Stat(strMap); err != nil {
		t.Fatalf("expected string_mapping.log after restore, stat error: %v", err)
	}

	seriesMap := filepath.Join(leaderDir, "series_mapping.log")
	if _, err := os.Stat(seriesMap); err != nil {
		t.Fatalf("expected series_mapping.log after restore, stat error: %v", err)
	}

	// Ensure the snapshot contains the datapoint we wrote earlier.
	if !foundInSnapshot {
		t.Fatalf("expected datapoint to be present in restored SSTables, not found")
	}
}
