package wal

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

// Test that per-payload syncs only occur when SyncMode == WALSyncAlways,
// that Sync() control requests trigger a sync for interval mode, and that
// disabled mode never syncs.
func TestWALSyncModes(t *testing.T) {
	tests := []struct {
		name                 string
		mode                 core.WALSyncMode
		expectPerPayloadSync bool
	}{
		{"always", core.WALSyncAlways, true},
		{"interval", core.WALSyncInterval, false},
		{"disabled", core.WALSyncDisabled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{
				Dir:                dir,
				SyncMode:           tt.mode,
				CommitMaxDelay:     1 * time.Millisecond,
				CommitMaxBatchSize: 1,
			}

			w, _, err := Open(opts)
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			// Install testing counter
			var ctr atomic.Uint64
			w.TestingOnlySyncCount = &ctr

			// append N single-entry batches; with CommitMaxBatchSize=1 each append
			// should produce exactly N payload writes.
			const N = 10
			for i := 0; i < N; i++ {
				e := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v")}
				if err := w.Append(e); err != nil {
					t.Fatalf("Append failed: %v", err)
				}
			}

			perPayloadSyncs := ctr.Load()
			if tt.expectPerPayloadSync {
				if perPayloadSyncs == 0 {
					t.Fatalf("expected per-payload syncs for mode %s, got 0", tt.mode)
				}
			} else {
				if perPayloadSyncs != 0 {
					t.Fatalf("expected no per-payload syncs for mode %s, got %d", tt.mode, perPayloadSyncs)
				}
			}

			// Interval mode should perform a sync when explicit Sync() is called.
			if tt.mode == core.WALSyncInterval {
				if err := w.Sync(); err != nil {
					t.Fatalf("Sync call failed: %v", err)
				}
				if ctr.Load() == 0 {
					t.Fatalf("expected Sync() to invoke a segment sync in interval mode")
				}
			}

			// Close
			if err := w.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}
		})
	}
}
