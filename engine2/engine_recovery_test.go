package engine2

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/require"
)

func TestWALRecovery_Successful(t *testing.T) {
	var tempDir string
	if os.Getenv("PRESERVE_TEST_DIR") != "" {
		tempDir = filepath.Join(os.TempDir(), "nexus_test_wal")
		_ = os.RemoveAll(tempDir)
		require.NoError(t, os.MkdirAll(tempDir, 0755))
		// PRESERVE_TEST_DIR enabled - using a stable temp dir for diagnostics.
	} else {
		tempDir = t.TempDir()
	}
	opts := GetBaseOptsForTest(t, "test")
	opts.DataDir = tempDir
	// Ensure WAL writes are persisted so crash simulation produces on-disk
	// WAL segments that the recovery procedure will replay.
	opts.WALSyncMode = core.WALSyncAlways

	dp1 := HelperDataPoint(t, "metric.a", map[string]string{"host": "server1"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "metric.b", map[string]string{"host": "server2"}, 200, map[string]interface{}{"value": 20.0})
	dp3_event := HelperDataPoint(t, "app.log", map[string]string{"level": "error"}, 300, map[string]interface{}{"status": int64(500), "msg": "failed to connect"})
	dp4_todelete := HelperDataPoint(t, "metric.c", map[string]string{"host": "server3"}, 400, map[string]interface{}{"value": 40.0})
	series_todelete := HelperDataPoint(t, "metric.d", map[string]string{"host": "server4"}, 500, map[string]interface{}{"value": 50.0})

	// Phase 1: create data then crash
	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), dp1))
		require.NoError(t, e.Put(context.Background(), dp2))
		require.NoError(t, e.Put(context.Background(), dp3_event))
		require.NoError(t, e.Put(context.Background(), dp4_todelete))
		require.NoError(t, e.Put(context.Background(), series_todelete))

		require.NoError(t, e.Delete(context.Background(), dp4_todelete.Metric, dp4_todelete.Tags, dp4_todelete.Timestamp))
		require.NoError(t, e.DeleteSeries(context.Background(), series_todelete.Metric, series_todelete.Tags))
	})

	// Phase 2: start an engine to recover
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number should be 7 (5 puts + 1 delete + 1 deleteSeries)
	require.Equal(t, uint64(7), engine2.GetSequenceNumber())

	// Verify recovered points
	val1, err := engine2.Get(context.Background(), dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err)
	require.Equal(t, 10.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := engine2.Get(context.Background(), dp2.Metric, dp2.Tags, dp2.Timestamp)
	require.NoError(t, err)
	require.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, val2, "value"))

	val3, err := engine2.Get(context.Background(), dp3_event.Metric, dp3_event.Tags, dp3_event.Timestamp)
	require.NoError(t, err)
	require.Equal(t, int64(500), HelperFieldValueValidateInt64(t, val3, "status"))
	require.Equal(t, "failed to connect", HelperFieldValueValidateString(t, val3, "msg"))

	// Deleted point should return not found
	_, err = engine2.Get(context.Background(), dp4_todelete.Metric, dp4_todelete.Tags, dp4_todelete.Timestamp)
	require.ErrorIs(t, err, sstable.ErrNotFound)

	// Deleted series should not appear in tag index
	keys, _ := engine2.GetSeriesByTags(series_todelete.Metric, series_todelete.Tags)
	require.Equal(t, 0, len(keys))

	// Verify metric-level presence for touched metrics
	k1, _ := engine2.GetSeriesByTags(dp1.Metric, dp1.Tags)
	k2, _ := engine2.GetSeriesByTags(dp2.Metric, dp2.Tags)
	k3, _ := engine2.GetSeriesByTags(dp3_event.Metric, dp3_event.Tags)
	require.Equal(t, 1, len(k1))
	require.Equal(t, 1, len(k2))
	require.Equal(t, 1, len(k3))

	// No SSTables should be present after WAL recovery (only memtables)
	sstDir := filepath.Join(opts.DataDir, "sst")
	files, _ := os.ReadDir(sstDir)
	require.Empty(t, files)

	// Sanity: ensure levels manager didn't create persistent tables
	// (we don't access internals; file-system check is sufficient)
	_ = sort.Ints // avoid unused import complaint if sort isn't used elsewhere
}
