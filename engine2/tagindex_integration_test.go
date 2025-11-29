package engine2

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
)

// Integration test: start engine with a non-zero index compaction interval
// and assert TagIndexManager does not log the zero-interval warning.
func TestTagIndexManager_NoZeroCompactionInterval(t *testing.T) {
	tmp := t.TempDir()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opts := GetBaseOptsForTest(t, "test_")
	opts.DataDir = tmp
	opts.Logger = logger
	opts.SSTableCompressor = &compressors.NoCompressionCompressor{}
	// set a positive index compaction interval which should be propagated
	opts.IndexCompactionIntervalSeconds = 37

	ai := setupStorageEngineStart(t, opts)
	a := ai.(*Engine2Adapter)
	// allow a small window for synchronous startup logging
	time.Sleep(50 * time.Millisecond)
	out := buf.String()
	if strings.Contains(out, "Invalid IndexCompactionIntervalSeconds") || strings.Contains(out, "interval_seconds=0") {
		t.Fatalf("Found zero-interval warning in logs: %s", out)
	}
	// cleanup
	closeAdapter(t, a)
}
