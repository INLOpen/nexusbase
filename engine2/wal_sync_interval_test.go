package engine2

import (
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

func TestEngine2Adapter_WALSyncInterval(t *testing.T) {
	tmp := t.TempDir()

	// Use a deterministic interval so we can assert exact counts.
	intervalMs := 25
	opts := StorageEngineOptions{
		DataDir:            tmp,
		WALSyncMode:        core.WALSyncInterval,
		WALFlushIntervalMs: intervalMs,
		WALBatchSize:       1,
	}

	e, a := setupEngineAndAdapterNoStart(t, opts)

	// Expose a test-only notify channel to observe ticks deterministically.
	intervals := 8
	a.TestingOnlyWALSyncNotify = make(chan struct{}, intervals)
	// non-blocking send is fine because we provide a buffered channel.
	a.TestingOnlyWALSyncNotifyBlocking = false

	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	// Expect `intervals` notifications, with a per-tick timeout.
	perTickTimeout := time.Duration(intervalMs+10) * time.Millisecond
	for i := 0; i < intervals; i++ {
		select {
		case <-a.TestingOnlyWALSyncNotify:
			// ok
		case <-time.After(perTickTimeout):
			t.Fatalf("timeout waiting for WAL sync tick %d", i)
		}
	}

	// Drain any remaining notifications (should be none) and assert
	// the WAL's TestingOnlySyncCount matches the tick count.
	time.Sleep(2 * time.Millisecond)
	final := e.wal.TestingOnlySyncCount.Load()
	expected := uint64(intervals)
	if final != expected {
		t.Fatalf("expected %d periodic WAL.Sync() calls, got %d", expected, final)
	}

	closeAdapter(t, a)
}
