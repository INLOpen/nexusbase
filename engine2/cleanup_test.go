package engine2

import (
	"testing"
	"time"
)

// TestPeriodicCleanerStopsOnClose verifies that when the adapter is created
// with a periodic tmp-file cleaner enabled, calling Close() cancels the
// cleaner and returns promptly.
func TestPeriodicCleanerStopsOnClose(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                        tmp,
		TmpFileCleanupIntervalSeconds:  1, // 1s interval for test
		TmpFileCleanupThresholdSeconds: 0, // allow defaulting
	}

	ai, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	a := ai.(*Engine2Adapter)

	if a.cleanupCancel == nil {
		t.Fatalf("expected cleanupCancel to be set when interval > 0")
	}

	// Give the goroutine a short moment to start
	time.Sleep(50 * time.Millisecond)

	// Call Close() and ensure it returns promptly (within 2s).
	done := make(chan struct{})
	go func() {
		_ = a.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatalf("Close() timed out; periodic cleaner may be blocked")
	}

	if a.cleanupCancel != nil {
		t.Fatalf("expected cleanupCancel to be cleared after Close()")
	}
}
