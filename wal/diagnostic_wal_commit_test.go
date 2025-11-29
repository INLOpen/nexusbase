package wal

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

// TestWALDiagCommit writes many small WALEntries to the WAL to exercise commit and notify codepaths.
// This test is diagnostic-only and should be run explicitly. It writes N entries as fast as possible.
func TestWALDiagCommit(t *testing.T) {
	tmp := t.TempDir()
	walDir := filepath.Join(tmp, "wal")
	opts := Options{Dir: walDir}
	// Allow overriding the commit batching delay via environment variable
	// `WAL_COMMIT_MAX_DELAY_MS` (milliseconds). If unset, test leaves the
	// options unchanged and the WAL uses its defaults.
	if v := os.Getenv("WAL_COMMIT_MAX_DELAY_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			opts.CommitMaxDelay = time.Duration(ms) * time.Millisecond
		}
	}
	w, _, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer func() { _ = w.Close(); _ = os.RemoveAll(tmp) }()

	count := 5000
	start := time.Now()
	for i := 0; i < count; i++ {
		e := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v")}
		if err := w.Append(e); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	dur := time.Since(start)
	t.Logf("Wrote %d entries in %s (avg %f ms)", count, dur, float64(dur.Milliseconds())/float64(count))
}
