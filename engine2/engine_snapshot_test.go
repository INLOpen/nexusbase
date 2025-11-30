package engine2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_SnapshotAndRestore(t *testing.T) {
	ctx := context.Background()

	// --- Phase 1: Setup a source engine and create a snapshot ---
	sourceDir := t.TempDir()
	sourceOpts := GetBaseOptsForTest(t, "test")
	// Ensure WAL is durable so the snapshot's copied WAL directory contains
	// well-formed WAL segments suitable for later restore/replay.
	sourceOpts.WALSyncMode = core.WALSyncAlways
	sourceOpts.DataDir = sourceDir

	sourceEngine := setupStorageEngineStart(t, sourceOpts)

	// Put some data into the source engine
	dp1 := HelperDataPoint(t, "metric.snap", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 10.0})
	dp2 := HelperDataPoint(t, "metric.snap", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, sourceEngine.Put(ctx, dp1))
	require.NoError(t, sourceEngine.Put(ctx, dp2))

	// Create the snapshot
	snapshotPath, err := sourceEngine.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.DirExists(t, snapshotPath)
	require.FileExists(t, filepath.Join(snapshotPath, "CURRENT"))

	// Verify snapshot contents (simple check)
	files, err := os.ReadDir(filepath.Join(snapshotPath, "sst"))
	require.NoError(t, err)
	assert.NotEmpty(t, files, "Snapshot sst directory should contain files")

	// Close the source engine
	require.NoError(t, sourceEngine.Close())

	// --- Phase 2: Setup a destination engine with different data ---
	destDir := t.TempDir()
	destOpts := GetBaseOptsForTest(t, "test")
	destOpts.WALSyncMode = core.WALSyncAlways
	destOpts.DataDir = destDir

	destEngine := setupStorageEngineStart(t, destOpts)

	// Put some conflicting/different data
	dp3 := HelperDataPoint(t, "metric.other", map[string]string{"id": "c"}, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, destEngine.Put(ctx, dp3))

	// --- Phase 3: Restore from the snapshot ---
	err = destEngine.RestoreFromSnapshot(ctx, snapshotPath, true) // Use overwrite=true
	require.NoError(t, err)

	// After restore, create a new engine instance to load the restored state.
	restartedEngine := setupStorageEngineStart(t, destOpts)
	defer restartedEngine.Close()

	// --- Phase 4: Verification ---
	// 1. Original data from the snapshot should exist.
	val1, err := restartedEngine.Get(ctx, dp1.Metric, dp1.Tags, dp1.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp1) should be found after restore")
	assert.Equal(t, 10.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := restartedEngine.Get(ctx, dp2.Metric, dp2.Tags, dp2.Timestamp)
	require.NoError(t, err, "Data from snapshot (dp2) should be found after restore")
	assert.Equal(t, 20.0, HelperFieldValueValidateFloat64(t, val2, "value"))

	// 2. Data that was in the destination engine before restore should be gone.
	_, err = restartedEngine.Get(ctx, dp3.Metric, dp3.Tags, dp3.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Original data from destination engine should be gone after restore")
}

func TestEngine_RestoreFromSnapshot_NoOverwrite(t *testing.T) {
	ctx := context.Background()

	// --- Phase 1: Create a snapshot ---
	sourceDir := t.TempDir()
	sourceOpts := GetBaseOptsForTest(t, "test")
	sourceOpts.WALSyncMode = core.WALSyncAlways
	sourceOpts.DataDir = sourceDir
	sourceEngine := setupStorageEngineStart(t, sourceOpts)
	require.NoError(t, sourceEngine.Put(ctx, HelperDataPoint(t, "metric.a", nil, 1, map[string]interface{}{"value": 1.0})))
	snapshotPath, err := sourceEngine.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, sourceEngine.Close())

	// --- Phase 2: Attempt to restore to a non-empty DB without overwrite ---
	destDir := t.TempDir()
	destOpts := GetBaseOptsForTest(t, "test")
	destOpts.DataDir = destDir
	destEngine := setupStorageEngineStart(t, destOpts)
	defer destEngine.Close()

	// Make the destination non-empty
	dpDest := HelperDataPoint(t, "metric.dest", nil, 2, map[string]interface{}{"value": 2.0})
	require.NoError(t, destEngine.Put(ctx, dpDest))

	// Action: Attempt restore
	err = destEngine.RestoreFromSnapshot(ctx, snapshotPath, false) // overwrite=false

	// Verification
	require.Error(t, err, "Expected restore to fail on non-empty DB without overwrite flag")
	assert.Contains(t, err.Error(), "database is not empty")

	// Verify original data in destination is untouched
	val, getErr := destEngine.Get(ctx, dpDest.Metric, dpDest.Tags, dpDest.Timestamp)
	require.NoError(t, getErr, "Original data in destination should be untouched after failed restore")
	assert.Equal(t, 2.0, HelperFieldValueValidateFloat64(t, val, "value"))
}

func TestEngine_RestoreFromSnapshot_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	// --- Helper to create a minimal valid snapshot for corruption tests ---
	createValidSnapshot := func(t *testing.T) (snapshotPath string, cleanup func()) {
		t.Helper()
		sourceDir := t.TempDir()
		sourceOpts := GetBaseOptsForTest(t, "test")
		sourceOpts.WALSyncMode = core.WALSyncAlways
		sourceOpts.DataDir = sourceDir
		sourceEngine := setupStorageEngineStart(t, sourceOpts)

		// Put one point to make the snapshot non-trivial
		require.NoError(t, sourceEngine.Put(ctx, HelperDataPoint(t, "metric.a", nil, 1, map[string]interface{}{"value": 1.0})))

		path, err := sourceEngine.CreateSnapshot(ctx)
		require.NoError(t, err)
		require.NoError(t, sourceEngine.Close())

		return path, func() { os.RemoveAll(sourceDir) }
	}

	// setupDestEngine is a helper to create a fresh destination engine for each sub-test.
	setupDestEngine := func(t *testing.T) (StorageEngineInterface, func()) {
		t.Helper()
		destDir := t.TempDir()
		destOpts := GetBaseOptsForTest(t, "test")
		destOpts.DataDir = destDir
		destEngine := setupStorageEngineStart(t, destOpts)
		return destEngine, func() { destEngine.Close() }
	}

	t.Run("SnapshotPathDoesNotExist", func(t *testing.T) {
		destEngine, cleanup := setupDestEngine(t)
		defer cleanup()

		nonExistentPath := filepath.Join(t.TempDir(), "non-existent-snapshot")
		err := destEngine.RestoreFromSnapshot(ctx, nonExistentPath, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path does not exist")
	})

	t.Run("SnapshotPathIsAFile", func(t *testing.T) {
		destEngine, cleanup := setupDestEngine(t)
		defer cleanup()

		snapshotPath, snapCleanup := createValidSnapshot(t)
		defer snapCleanup()

		// Replace the snapshot directory with a file of the same name
		require.NoError(t, os.RemoveAll(snapshotPath))
		require.NoError(t, os.WriteFile(snapshotPath, []byte("i am a file"), 0644))

		err := destEngine.RestoreFromSnapshot(ctx, snapshotPath, true)
		require.Error(t, err)
		// The error comes from snapshot.readManifestFromDir trying to read a file inside the "directory"
		assert.Contains(t, err.Error(), "failed to read CURRENT file")
	})

	t.Run("SnapshotMissingCurrentFile", func(t *testing.T) {
		destEngine, cleanup := setupDestEngine(t)
		defer cleanup()

		snapshotPath, snapCleanup := createValidSnapshot(t)
		defer snapCleanup()

		// Delete the CURRENT file
		require.NoError(t, sys.Remove(filepath.Join(snapshotPath, "CURRENT")))

		err := destEngine.RestoreFromSnapshot(ctx, snapshotPath, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CURRENT file")
	})

	t.Run("SnapshotManifestIsCorrupted", func(t *testing.T) {
		destEngine, cleanup := setupDestEngine(t)
		defer cleanup()

		snapshotPath, snapCleanup := createValidSnapshot(t)
		defer snapCleanup()

		// Find the real manifest file and corrupt it
		currentBytes, err := os.ReadFile(filepath.Join(snapshotPath, "CURRENT"))
		require.NoError(t, err)
		manifestFileName := string(currentBytes)
		manifestPath := filepath.Join(snapshotPath, manifestFileName)

		require.NoError(t, os.WriteFile(manifestPath, []byte("this is not a valid manifest"), 0644))

		err = destEngine.RestoreFromSnapshot(ctx, snapshotPath, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid binary manifest magic number")
	})
}
