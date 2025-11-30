package engine2

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// setupEngineForWALTest creates an Engine2 and adapter with the provided
// options, initializes the testing-only sync counter, starts the adapter,
// and returns both engine and adapter. It fatals on errors.
func setupEngineForWALTest(t *testing.T, opts StorageEngineOptions) (*Engine2, *Engine2Adapter) {
	t.Helper()

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	return e, a
}

// setupEngineAndAdapterNoStart creates an Engine2 and adapter but does not
// start the adapter. This allows tests to configure test-only fields on the
// adapter before calling Start().
func setupEngineAndAdapterNoStart(t *testing.T, opts StorageEngineOptions) (*Engine2, *Engine2Adapter) {
	t.Helper()

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	return e, a
}

// appendEntries appends `n` simple put entries to the engine's WAL.
func appendEntries(t *testing.T, e *Engine2, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		if err := e.wal.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
}

// appendBatchCommits appends `commits` batches, each containing `per` entries
// and will fatal on errors.
func appendBatchCommits(t *testing.T, e *Engine2, commits, per int) {
	t.Helper()

	for c := 0; c < commits; c++ {
		batch := make([]core.WALEntry, 0, per)
		for i := 0; i < per; i++ {
			batch = append(batch, core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: make([]byte, 64)})
		}
		if err := e.wal.AppendBatch(batch); err != nil {
			t.Fatalf("AppendBatch failed: %v", err)
		}
	}
}

// closeAdapter fatals if adapter Close returns error.
func closeAdapter(t *testing.T, a *Engine2Adapter) {
	t.Helper()
	if err := a.Close(); err != nil {
		t.Fatalf("adapter Close failed: %v", err)
	}
}
