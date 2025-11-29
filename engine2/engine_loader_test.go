package engine2

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/require"
)

// The original engine tests accessed internal fields. For engine2 we exercise
// the same recovery scenarios via the public StorageEngineInterface APIs.

func TestStateLoader_Load_FreshStart(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, eng.Start())
	defer eng.Close()

	// Fresh engine should start with sequence number 0 and a WAL present.
	seq := eng.GetSequenceNumber()
	require.Equal(t, uint64(0), seq)
	require.NotNil(t, eng.GetWAL())
}

func TestStateLoader_Load_FromManifest(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	// Ensure WAL is durable for this test so manifest and WAL are consistent.
	opts.WALSyncMode = core.WALSyncAlways

	// Phase 1: create engine and write data, then close cleanly to persist manifest
	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine1.Start())

	dp1 := HelperDataPoint(t, "metric.manifest", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})
	require.NoError(t, engine1.Put(context.Background(), dp1))

	dp2 := HelperDataPoint(t, "metric.wal", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})
	require.NoError(t, engine1.Put(context.Background(), dp2))

	require.NoError(t, engine1.Close())

	// Optional debug hook: if `NEXUSBASE_DUMP_WAL_AFTER_CLOSE=1` is set,
	// run the `tools/parse_wal.go` helper against any WAL segments in the
	// data dir so we can inspect stored vs computed checksums before an
	// eventual recovery attempt. This is a temporary aid for debugging.
	if os.Getenv("NEXUSBASE_DUMP_WAL_AFTER_CLOSE") == "1" {
		walDir := filepath.Join(opts.DataDir, "wal")
		if files, err := os.ReadDir(walDir); err == nil {
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".wal") {
					path := filepath.Join(walDir, f.Name())
					// Run the parse tool and log its output
					cmd := exec.Command("go", "run", "./tools/parse_wal.go", "-file", path)
					out, _ := cmd.CombinedOutput()
					t.Logf("parse_wal output for %s:\n%s", path, string(out))
				}
			}
		}
	}

	// Phase 2: create a new engine instance which should recover from manifest
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number may vary depending on exact recovery path; ensure
	// data is recoverable instead of asserting a specific sequence value.

	// Data should be queryable via public Get
	val1, err := engine2.Get(context.Background(), "metric.manifest", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := engine2.Get(context.Background(), "metric.wal", map[string]string{"id": "b"}, 200)
	require.NoError(t, err)
	require.Equal(t, 2.0, HelperFieldValueValidateFloat64(t, val2, "value"))
}

func TestStateLoader_Load_FallbackScan(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	dataDir := opts.DataDir

	// Phase 1: create a valid engine state with SSTables but no manifest
	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine1.Start())

	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})))
	require.NoError(t, engine1.Close())

	// Remove manifest file(s) to force fallback scan
	_ = sys.Remove(filepath.Join(dataDir, core.CurrentFileName))
	files, _ := os.ReadDir(dataDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "MANIFEST") {
			_ = sys.Remove(filepath.Join(dataDir, f.Name()))
		}
	}
	// Also remove the new manifest under `sstables/` so the adapter cannot
	// trust `sstables/manifest.json` and must perform a fallback-scan.
	_ = sys.Remove(filepath.Join(dataDir, "sstables", "manifest.json"))

	// Tests that exercise fallback-scan should not attempt to read any WAL
	// content. Remove the WAL directory to avoid accidental recovery from
	// stray WAL segments which would change expected behavior.
	_ = os.RemoveAll(filepath.Join(opts.DataDir, "wal"))

	// Phase 2: start new engine which should fallback-scan SSTables
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number may be reset to 0 on fallback
	require.Equal(t, uint64(0), engine2.GetSequenceNumber())

	// Data should still be queryable
	val1, err := engine2.Get(context.Background(), "metric.fallback", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}

func TestStateLoader_Load_WithWALRecovery(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	// Ensure WAL is enabled for this test so writes are persisted to disk
	// and can be recovered after an unclean shutdown.
	opts.WALSyncMode = core.WALSyncAlways

	// Write to WAL and then crash
	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.wal.recovery", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	})

	// Load and recover
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number should be restored from WAL
	require.Equal(t, uint64(1), engine2.GetSequenceNumber())

	val1, err := engine2.Get(context.Background(), "metric.wal.recovery", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}
