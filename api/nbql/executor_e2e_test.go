package nbql

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corenbql "github.com/INLOpen/nexuscore/nbql"
)

// getBaseOptsForE2ETest provides basic engine options for end-to-end testing.
func getBaseOptsForE2ETest(t *testing.T) engine2.StorageEngineOptions {
	t.Helper()
	// NOTE: The field names in StorageEngineOptions have been updated.
	// This function now uses the new field names based on engine/engine.go.
	return engine2.StorageEngineOptions{
		TargetSSTableSize:            1 * 1024 * 1024, // 1MB
		MemtableThreshold:            1 * 1024 * 1024, // 1MB
		MaxL0Files:                   4,
		LevelsTargetSizeMultiplier:   10,
		MaxLevels:                    7,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  core.WALSyncDisabled,
		WALMaxSegmentSize:            1 * 1024 * 1024, // 1MB
		Clock:                        clock.SystemClockDefault,
		CompactionIntervalSeconds:    1,
		BlockCacheCapacity:           64 * 1024 * 1024, // 64MB
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
	}
}

// HelperDataPoint creates a simple data point for testing.
func HelperDataPoint(t *testing.T, metric string, tags map[string]string, timestamp int64, fields map[string]interface{}) core.DataPoint {
	t.Helper()
	// NOTE: The core.NewDataPoint function signature has changed.
	// We now create the struct and assign fields manually.
	fieldValues, err := core.NewFieldValuesFromMap(fields)
	require.NoError(t, err)
	return core.DataPoint{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Fields:    fieldValues,
	}
}

// HelperFieldValueValidateFloat64 extracts and validates a float64 field.
func HelperFieldValueValidateFloat64(t *testing.T, fv core.FieldValues, fieldName string) float64 {
	t.Helper()
	// NOTE: The structure of FieldValues now holds core.PointValue structs.
	pointVal, ok := fv[fieldName]
	require.True(t, ok, "field %s not found", fieldName)
	floatVal, ok := pointVal.ValueFloat64()
	require.True(t, ok, "field %s is not a float64", fieldName)
	return floatVal
}

func TestExecutor_E2E_SnapshotAndRestore(t *testing.T) {
	ctx := context.Background()

	// --- Phase 1: Setup a source engine and create a snapshot ---
	sourceDir := t.TempDir()
	sourceOpts := getBaseOptsForE2ETest(t)
	sourceOpts.DataDir = sourceDir

	sourceEngine := setupEngineStart(t, sourceOpts)

	// Put some data into the source engine
	dp1 := HelperDataPoint(t, "e2e.snap", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "e2e.snap", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, sourceEngine.Put(ctx, dp1))
	require.NoError(t, sourceEngine.Put(ctx, dp2))

	sourceExecutor := NewExecutor(sourceEngine, clock.SystemClockDefault)

	// Execute SNAPSHOT command
	snapshotCmd, err := corenbql.Parse("SNAPSHOT")
	require.NoError(t, err)
	result, err := sourceExecutor.Execute(ctx, snapshotCmd)
	require.NoError(t, err)

	resMap, ok := result.(map[string]interface{})
	require.True(t, ok, "Result should be a map")
	snapshotPath, ok := resMap["path"].(string)
	require.True(t, ok, "Result should contain a path string")
	require.DirExists(t, snapshotPath)

	require.NoError(t, sourceEngine.Close())

	// --- Phase 2: Setup a destination engine and restore ---
	destDir := t.TempDir()
	destOpts := getBaseOptsForE2ETest(t)
	destOpts.DataDir = destDir

	destEngine := setupEngineStart(t, destOpts)

	dp3 := HelperDataPoint(t, "e2e.other", map[string]string{"id": "c"}, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, destEngine.Put(ctx, dp3))

	destExecutor := NewExecutor(destEngine, clock.SystemClockDefault)

	// Execute RESTORE command
	restoreQuery := fmt.Sprintf("RESTORE FROM '%s' WITH OVERWRITE", filepath.ToSlash(snapshotPath))
	restoreCmd, err := corenbql.Parse(restoreQuery)
	require.NoError(t, err)
	_, err = destExecutor.Execute(ctx, restoreCmd)
	require.NoError(t, err)

	// --- Phase 3: Restart engine and verify state ---
	restartedEngine := setupEngineStart(t, destOpts)
	defer restartedEngine.Close()

	// Verification
	val1, err := restartedEngine.Get(ctx, dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp1) should be found after restore")
	assert.Equal(t, 10.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := restartedEngine.Get(ctx, dp2.Metric, dp2.Tags, dp2.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp2) should be found after restore")
	assert.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, val2, "value"))

	_, err = restartedEngine.Get(ctx, dp3.Metric, dp3.Tags, dp3.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Original data from destination engine should be gone after restore")
}

func TestExecutor_E2E_RemoveSeries(t *testing.T) {
	ctx := context.Background()
	var err error

	// --- Phase 1: Setup engine and ingest data ---
	dataDir := t.TempDir()
	opts := getBaseOptsForE2ETest(t)
	opts.DataDir = dataDir

	eng := setupEngineStart(t, opts)
	defer eng.Close()

	// Data for the series to be removed
	dp1 := HelperDataPoint(t, "e2e.remove", map[string]string{"host": "a", "dc": "us-east"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "e2e.remove", map[string]string{"host": "a", "dc": "us-east"}, 200, map[string]interface{}{"value": 20.0})

	// Data for another series that should NOT be removed
	dp3 := HelperDataPoint(t, "e2e.remove", map[string]string{"host": "b", "dc": "us-east"}, 300, map[string]interface{}{"value": 30.0})

	require.NoError(t, eng.Put(ctx, dp1))
	require.NoError(t, eng.Put(ctx, dp2))
	require.NoError(t, eng.Put(ctx, dp3))

	// --- Phase 2: Verify initial state ---
	_, err = eng.Get(ctx, dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err, "Data point 1 should exist before removal")
	_, err = eng.Get(ctx, dp3.Metric, dp3.Tags, dp3.Timestamp)
	require.NoError(t, err, "Data point 3 should exist before removal")

	// --- Phase 3: Execute REMOVE SERIES command ---
	executor := NewExecutor(eng, clock.SystemClockDefault)
	removeQuery := `REMOVE SERIES "e2e.remove" TAGGED (host="a", dc="us-east")`
	removeCmd, err := corenbql.Parse(removeQuery)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, removeCmd)
	require.NoError(t, err)

	// Check the response
	res, ok := result.(ManipulateResponse)
	require.True(t, ok, "Result should be a ManipulateResponse")
	assert.Equal(t, ResponseOK, res.Status)
	assert.Equal(t, uint64(1), res.RowsAffected)

	// --- Phase 4: Verify final state ---
	// The removed series should be gone
	_, err = eng.Get(ctx, dp1.Metric, dp1.Tags, dp1.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Data point 1 should be gone after removal")
	_, err = eng.Get(ctx, dp2.Metric, dp2.Tags, dp2.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Data point 2 should be gone after removal")

	// The other series should still exist
	val3, err := eng.Get(ctx, dp3.Metric, dp3.Tags, dp3.Timestamp)
	require.NoError(t, err, "Data point 3 should still exist after removal")
	assert.Equal(t, 30.0, HelperFieldValueValidateFloat64(t, val3, "value"))
}

func TestExecutor_E2E_RemovePoint(t *testing.T) {
	ctx := context.Background()
	var err error

	// --- Phase 1: Setup engine and ingest data ---
	dataDir := t.TempDir()
	opts := getBaseOptsForE2ETest(t)
	opts.DataDir = dataDir

	eng := setupEngineStart(t, opts)
	defer eng.Close()

	// Data points for the same series at different timestamps
	dp1 := HelperDataPoint(t, "e2e.remove.point", map[string]string{"host": "c"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "e2e.remove.point", map[string]string{"host": "c"}, 200, map[string]interface{}{"value": 20.0}) // This one will be removed
	dp3 := HelperDataPoint(t, "e2e.remove.point", map[string]string{"host": "c"}, 300, map[string]interface{}{"value": 30.0})

	require.NoError(t, eng.Put(ctx, dp1))
	require.NoError(t, eng.Put(ctx, dp2))
	require.NoError(t, eng.Put(ctx, dp3))

	// --- Phase 2: Verify initial state ---
	_, err = eng.Get(ctx, dp1.Metric, dp1.Tags, 100)
	require.NoError(t, err, "Data point at t=100 should exist before removal")
	_, err = eng.Get(ctx, dp2.Metric, dp2.Tags, 200)
	require.NoError(t, err, "Data point at t=200 should exist before removal")

	// --- Phase 3: Execute REMOVE FROM ... AT ... command ---
	executor := NewExecutor(eng, clock.SystemClockDefault)
	removeQuery := `REMOVE FROM "e2e.remove.point" TAGGED (host="c") AT 200`
	removeCmd, err := corenbql.Parse(removeQuery)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, removeCmd)
	require.NoError(t, err)

	res, ok := result.(ManipulateResponse)
	require.True(t, ok, "Result should be a ManipulateResponse")
	assert.Equal(t, ResponseOK, res.Status)
	assert.Equal(t, uint64(1), res.RowsAffected)

	// --- Phase 4: Verify final state ---
	// The other points should still exist
	_, err = eng.Get(ctx, dp1.Metric, dp1.Tags, 100)
	require.NoError(t, err, "Data point at t=100 should still exist")

	// The removed point should be gone
	_, err = eng.Get(ctx, dp2.Metric, dp2.Tags, 200)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Data point at t=200 should be gone after removal")

	// The other point should still exist
	_, err = eng.Get(ctx, dp3.Metric, dp3.Tags, 300)
	require.NoError(t, err, "Data point at t=300 should still exist")
}

func TestExecutor_E2E_RemoveRange(t *testing.T) {
	ctx := context.Background()
	var err error

	// --- Phase 1: Setup engine and ingest data ---
	dataDir := t.TempDir()
	opts := getBaseOptsForE2ETest(t)
	opts.DataDir = dataDir

	eng := setupEngineStart(t, opts)
	defer eng.Close()

	// Data points for the same series at different timestamps
	metric := "e2e.remove.range"
	tags := map[string]string{"host": "d"}
	dp1 := HelperDataPoint(t, metric, tags, 100, map[string]interface{}{"value": 10.0}) // Before range
	dp2 := HelperDataPoint(t, metric, tags, 200, map[string]interface{}{"value": 20.0}) // Start of range (inclusive)
	dp3 := HelperDataPoint(t, metric, tags, 300, map[string]interface{}{"value": 30.0}) // Inside range
	dp4 := HelperDataPoint(t, metric, tags, 400, map[string]interface{}{"value": 40.0}) // End of range (inclusive)
	dp5 := HelperDataPoint(t, metric, tags, 500, map[string]interface{}{"value": 50.0}) // After range

	require.NoError(t, eng.Put(ctx, dp1))
	require.NoError(t, eng.Put(ctx, dp2))
	require.NoError(t, eng.Put(ctx, dp3))
	require.NoError(t, eng.Put(ctx, dp4))
	require.NoError(t, eng.Put(ctx, dp5))

	// --- Phase 2: Verify initial state ---
	_, err = eng.Get(ctx, metric, tags, 300)
	require.NoError(t, err, "Data point at t=300 should exist before removal")

	// --- Phase 3: Execute REMOVE FROM ... FROM ... TO ... command ---
	executor := NewExecutor(eng, clock.SystemClockDefault)
	removeQuery := `REMOVE FROM "e2e.remove.range" TAGGED (host="d") FROM 200 TO 400`
	removeCmd, err := corenbql.Parse(removeQuery)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, removeCmd)
	require.NoError(t, err)

	res, ok := result.(ManipulateResponse)
	require.True(t, ok, "Result should be a ManipulateResponse")
	assert.Equal(t, ResponseOK, res.Status)
	assert.Equal(t, uint64(1), res.RowsAffected)

	// --- Phase 4: Verify final state ---
	_, err = eng.Get(ctx, metric, tags, 100)
	require.NoError(t, err, "Data point at t=100 should still exist")
	_, err = eng.Get(ctx, metric, tags, 200)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Data point at t=200 should be gone after removal")
	_, err = eng.Get(ctx, metric, tags, 400)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Data point at t=400 should be gone after removal")
	_, err = eng.Get(ctx, metric, tags, 500)
	require.NoError(t, err, "Data point at t=500 should still exist")
}
