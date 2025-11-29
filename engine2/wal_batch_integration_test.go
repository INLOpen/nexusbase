package engine2

import (
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

	e, a := setupEngineForWALTest(t, opts)

	commits := 6
	appendBatchCommits(t, e, commits, 8)

	got := e.wal.TestingOnlySyncCount.Load()
	if got != uint64(commits) {
		t.Fatalf("expected %d Sync() calls for batch commits via engine, got %d", commits, got)
	}

	closeAdapter(t, a)
}
