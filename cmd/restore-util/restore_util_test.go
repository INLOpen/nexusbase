package main

import (
	"context"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/sstable"
)

func TestRestoreUtil(t *testing.T) {
	baseDir := t.TempDir()

	originalDataDir := filepath.Join(baseDir, "original_data")
	restoredDataDir := filepath.Join(baseDir, "restored_data")

	// --- 1. Setup: Create an original database and a snapshot ---
	opts := engine2.StorageEngineOptions{
		DataDir:                      originalDataDir,
		MemtableThreshold:            1024,
		IndexMemtableThreshold:       1024,
		CompactionIntervalSeconds:    3600, // Disable auto compaction
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		WALSyncMode:                  core.WALSyncAlways,
		Logger:                       slog.Default(),
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	origEngine := setupEngineStart(t, opts)

	// Add some data
	metric := "restore.test"
	tags := map[string]string{"id": "123"}
	ts := time.Now().UnixNano()
	val := 99.9

	point, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
	if err != nil {
		t.Fatalf("Failed to create data point: %v", err)
	}

	t.Log(*point)

	if err := origEngine.Put(context.Background(), *point); err != nil {
		t.Fatalf("Failed to put data point in original engine: %v", err)
	}

	// Create snapshot
	createdSnapshotPath, err := origEngine.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	if err := origEngine.Close(); err != nil {
		t.Fatalf("Failed to close original engine: %v", err)
	}

	// --- 2. Execution: Run the restore utility logic ---
	err = run(createdSnapshotPath, restoredDataDir, slog.Default())
	if err != nil {
		t.Fatalf("restore-util run() failed: %v", err)
	}

	// --- 3. Verification: Open the restored database and check data ---
	restoredOpts := engine2.StorageEngineOptions{
		DataDir:                      restoredDataDir,
		MemtableThreshold:            1024,
		IndexMemtableThreshold:       1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		WALSyncMode:                  core.WALSyncAlways,
		Logger:                       slog.Default(),
	}
	restoredEngine := setupEngineStart(t, restoredOpts)
	defer restoredEngine.Close()

	// Verify the data exists
	// Query for the specific point to verify by providing a precise time range.
	iter, err := restoredEngine.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: ts, EndTime: ts})
	if err != nil {
		t.Fatalf("Query from restored engine failed: %v", err)
	}
	defer iter.Close()

	if !iter.Next() {
		t.Fatalf("Expected one result from query, but got none")
	}

	item, err := iter.At()
	if err != nil {
		t.Fatalf("Iterator.At() failed: %v", err)
	}
	retrievedVal, ok := item.Fields["value"].ValueFloat64()
	if !ok {
		t.Fatalf("Field 'value' not found or not a float64 in restored data")
	}
	if math.Abs(retrievedVal-val) > 1e-9 {
		t.Errorf("Data mismatch in restored engine: got %f, want %f", retrievedVal, val)
	}
	iter.Put(item)

	// --- 4. Test error case: target directory not empty ---
	// The restoredDataDir is now not empty. Running again should fail.
	err = run(createdSnapshotPath, restoredDataDir, slog.Default())
	if err == nil {
		t.Fatal("Expected error when target directory is not empty, but got nil")
	}
	expectedErrStr := "already exists and is not empty"
	if !strings.Contains(err.Error(), expectedErrStr) {
		t.Errorf("Error message mismatch: got '%s', want to contain '%s'", err.Error(), expectedErrStr)
	}
}
