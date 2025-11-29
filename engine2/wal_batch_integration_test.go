package engine2

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// Integration test: ensure `WALSyncBatch` mode performs one fsync per
// logical commit when used via Engine2 adapter lifecycle.
func TestEngine2_WALSyncBatchIntegration(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:            tmp,
		WALSyncMode:        core.WALSyncBatch,
		WALFlushIntervalMs: 0,
		WALBatchSize:       0,
		WALMaxSegmentSize:  128,
	}

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	// Install testing-only counter so we can observe Sync() calls.
	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	commits := 6
	for c := 0; c < commits; c++ {
		// Create a single logical commit with multiple entries so the WAL
		// may emit multiple payloads during encoding; batch mode should
		// still only perform one Sync() per AppendBatch.
		batch := make([]core.WALEntry, 0, 8)
		for i := 0; i < 8; i++ {
			batch = append(batch, core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: make([]byte, 64)})
		}
		if err := e.wal.AppendBatch(batch); err != nil {
			t.Fatalf("AppendBatch failed: %v", err)
		}
	}

	got := e.wal.TestingOnlySyncCount.Load()
	if got != uint64(commits) {
		t.Fatalf("expected %d Sync() calls for batch commits via engine, got %d", commits, got)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("adapter Close failed: %v", err)
	}
}
