package engine2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/require"
)

func init() {
	// sys.SetDebugMode(false)
}

// testDataPoint is a helper struct for defining test data points in this file.
type testDataPoint struct {
	metric    string
	tags      map[string]string
	timestamp int64
	value     float64
}

func TestStorageEngine_WALRecovery_CrashSimulation(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		BlockCacheCapacity:           10,
		MaxL0Files:                   4,
		TargetSSTableSize:            2048,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		Metrics:                      NewEngineMetrics(false, "wal_crash_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		CompactionIntervalSeconds:    3600,
	}

	// Create WAL directly using central WAL to avoid legacy writer differences.
	{
		walPath := filepath.Join(tempDir, "wal")
		wopts := wal.Options{
			Dir:                 walPath,
			SyncMode:            opts.WALSyncMode,
			MaxSegmentSize:      opts.WALMaxSegmentSize,
			PreallocateSegments: opts.WALPreallocateSegments,
			PreallocSize:        opts.WALPreallocSize,
			Logger:              opts.Logger,
		}
		w, _, err := wal.Open(wopts)
		require.NoError(t, err)
		// append three put events
		for i := 1; i <= 3; i++ {
			dp := HelperDataPoint(t, "wal.metric", map[string]string{"id": fmt.Sprintf("%d", i)}, int64(i*1000), map[string]interface{}{"value": float64(i)})
			key := core.EncodeTSDBKeyWithString(dp.Metric, dp.Tags, dp.Timestamp)
			fv, ferr := core.NewFieldValuesFromMap(map[string]interface{}{"value": float64(i)})
			require.NoError(t, ferr)
			vb, verr := fv.Encode()
			require.NoError(t, verr)
			e := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: vb}
			require.NoError(t, w.Append(e))
		}
		require.NoError(t, w.Close())
	}

	engine2 := setupStorageEngineStart(t, opts)
	defer engine2.Close()

	for i := 1; i <= 3; i++ {
		retrievedValue, err := engine2.Get(context.Background(), "wal.metric", map[string]string{"id": fmt.Sprintf("%d", i)}, int64(i*1000))
		if err != nil {
			t.Errorf("engine2.Get failed for id %d after WAL recovery: %v", i, err)
		}
		if val, ok := retrievedValue["value"].ValueFloat64(); !ok || val != float64(i) {
			t.Errorf("engine2.Get retrieved value mismatch for id %d: got %f, want %f", i, val, float64(i))
		}
	}

	// Note: do not inspect concrete internals here; rely on public API checks above.
}

func TestStorageEngine_WALRecovery_WithDeletes(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		Metrics:                      NewEngineMetrics(false, "wal_delete_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	// write WAL directly with central WAL format
	{
		walPath := filepath.Join(tempDir, "wal")
		wopts := wal.Options{
			Dir:                 walPath,
			SyncMode:            opts.WALSyncMode,
			MaxSegmentSize:      opts.WALMaxSegmentSize,
			PreallocateSegments: opts.WALPreallocateSegments,
			PreallocSize:        opts.WALPreallocSize,
			Logger:              opts.Logger,
		}
		w, _, err := wal.Open(wopts)
		require.NoError(t, err)

		// metric.keep put
		dp := HelperDataPoint(t, "metric.keep", map[string]string{"id": "keep"}, 1000, map[string]interface{}{"value": 1.0})
		k := core.EncodeTSDBKeyWithString(dp.Metric, dp.Tags, dp.Timestamp)
		fv, ferr := core.NewFieldValuesFromMap(map[string]interface{}{"value": 1.0})
		require.NoError(t, ferr)
		vb, verr := fv.Encode()
		require.NoError(t, verr)
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k, Value: vb}))

		// point put then point delete
		dp2 := HelperDataPoint(t, "metric.point.delete", map[string]string{"id": "point_del"}, 2000, map[string]interface{}{"value": 2.0})
		k2 := core.EncodeTSDBKeyWithString(dp2.Metric, dp2.Tags, dp2.Timestamp)
		fv2, ferr2 := core.NewFieldValuesFromMap(map[string]interface{}{"value": 2.0})
		require.NoError(t, ferr2)
		vb2, verr2 := fv2.Encode()
		require.NoError(t, verr2)
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k2, Value: vb2}))
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypeDelete, Key: k2, Value: nil}))

		// series put then delete series (timestamp -1)
		dp3 := HelperDataPoint(t, "metric.series.delete", map[string]string{"id": "series_del"}, 3000, map[string]interface{}{"value": 3.0})
		k3 := core.EncodeTSDBKeyWithString(dp3.Metric, dp3.Tags, dp3.Timestamp)
		fv3, ferr3 := core.NewFieldValuesFromMap(map[string]interface{}{"value": 3.0})
		require.NoError(t, ferr3)
		vb3, verr3 := fv3.Encode()
		require.NoError(t, verr3)
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k3, Value: vb3}))
		seriesKeyDel := core.EncodeTSDBKeyWithString("metric.series.delete", map[string]string{"id": "series_del"}, -1)
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypeDelete, Key: seriesKeyDel, Value: nil}))

		// range puts and range delete marker
		dp4 := HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 4000, map[string]interface{}{"value": 4.0})
		k4 := core.EncodeTSDBKeyWithString(dp4.Metric, dp4.Tags, dp4.Timestamp)
		fv4, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 4.0})
		vb4, _ := fv4.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k4, Value: vb4}))

		dp5 := HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 5000, map[string]interface{}{"value": 5.0})
		k5 := core.EncodeTSDBKeyWithString(dp5.Metric, dp5.Tags, dp5.Timestamp)
		fv5, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 5.0})
		vb5, _ := fv5.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k5, Value: vb5}))

		dp6 := HelperDataPoint(t, "metric.range.delete", map[string]string{"id": "range_del"}, 6000, map[string]interface{}{"value": 6.0})
		k6 := core.EncodeTSDBKeyWithString(dp6.Metric, dp6.Tags, dp6.Timestamp)
		fv6, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 6.0})
		vb6, _ := fv6.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k6, Value: vb6}))

		// range tombstone marker: encode __range_delete_end into fields for start=4500
		fvRange := make(core.FieldValues)
		if pv, perr := core.NewPointValue(5500); perr == nil {
			fvRange["__range_delete_end"] = pv
			enc, e := fvRange.Encode()
			require.NoError(t, e)
			kr := core.EncodeTSDBKeyWithString("metric.range.delete", map[string]string{"id": "range_del"}, 4500)
			require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypeDelete, Key: kr, Value: enc}))
		}

		require.NoError(t, w.Close())
	}

	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()

	if _, err := engine2.Get(context.Background(), "metric.keep", map[string]string{"id": "keep"}, 1000); err != nil {
		t.Errorf("Expected to find kept data point, but got error: %v", err)
	}

	if _, err := engine2.Get(context.Background(), "metric.point.delete", map[string]string{"id": "point_del"}, 2000); err == nil {
		t.Errorf("Expected point-deleted data to be NotFound, but got no error")
	}

	if _, err := engine2.Get(context.Background(), "metric.series.delete", map[string]string{"id": "series_del"}, 3000); !errors.Is(err, sstable.ErrNotFound) {
		t.Errorf("Expected series-deleted data to be NotFound, but got error: %v", err)
	}

	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 4000); err != nil {
		t.Errorf("Expected to find data before range delete, but got error: %v", err)
	}
	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 5000); !errors.Is(err, sstable.ErrNotFound) {
		t.Errorf("Expected range-deleted data to be NotFound, but got error: %v", err)
	}
	if _, err := engine2.Get(context.Background(), "metric.range.delete", map[string]string{"id": "range_del"}, 6000); err != nil {
		t.Errorf("Expected to find data after range delete, but got error: %v", err)
	}
}

func TestStorageEngine_WALRecovery_AdvancedCorruption(t *testing.T) {
	setupWALWithData := func(t *testing.T, dir string, entries []testDataPoint) (StorageEngineOptions, string) {
		t.Helper()
		opts := GetBaseOptsForTest(t, "test")
		opts.DataDir = dir
		opts.WALSyncMode = core.WALSyncAlways

		// Create WAL directory and write entries directly using the central WAL
		walPath := filepath.Join(dir, "wal")
		wopts := wal.Options{
			Dir:                 walPath,
			SyncMode:            opts.WALSyncMode,
			MaxSegmentSize:      opts.WALMaxSegmentSize,
			PreallocateSegments: opts.WALPreallocateSegments,
			PreallocSize:        opts.WALPreallocSize,
			Logger:              opts.Logger,
		}
		w, _, err := wal.Open(wopts)
		if err != nil {
			t.Fatalf("failed to create central WAL for test: %v", err)
		}

		// Append provided entries as WALEntry Put events
		for _, entry := range entries {
			key := core.EncodeTSDBKeyWithString(entry.metric, entry.tags, entry.timestamp)
			fv, ferr := core.NewFieldValuesFromMap(map[string]interface{}{"value": entry.value})
			if ferr != nil {
				_ = w.Close()
				t.Fatalf("failed to build FieldValues for test entry: %v", ferr)
			}
			vb, verr := fv.Encode()
			if verr != nil {
				_ = w.Close()
				t.Fatalf("failed to encode FieldValues for test entry: %v", verr)
			}
			e := core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: vb}
			if err := w.Append(e); err != nil {
				_ = w.Close()
				t.Fatalf("failed to append WALEntry to central WAL: %v", err)
			}
		}

		if err := w.Close(); err != nil {
			t.Fatalf("failed to close central WAL after setup: %v", err)
		}

		return opts, walPath
	}

	testEntries := []testDataPoint{
		{"metric.1", map[string]string{"id": "a"}, 1000, 1.0},
		{"metric.2", map[string]string{"id": "b"}, 2000, 2.0},
		{"metric.3", map[string]string{"id": "c"}, 3000, 3.0},
	}

	t.Run("CorruptedHeader", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)

		segmentPath := filepath.Join(walPath, "00000001.wal")
		if err := corruptSegmentMagic(segmentPath, 0xDEADBEEF); err != nil {
			t.Fatalf("Failed to corrupt WAL segment magic: %v", err)
		}

		eng2, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng2.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}
		if !strings.Contains(err.Error(), "invalid magic number") {
			t.Errorf("Expected error message to contain 'invalid magic number', but got: %v", err)
		}
	})

	t.Run("TruncatedRecord", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)
		segmentPath := filepath.Join(walPath, "00000001.wal")

		// Truncate inside the second record body (offset 5 into the body)
		if err := truncateInsideRecord(segmentPath, 1, 5); err != nil {
			t.Fatalf("Failed to truncate WAL segment file: %v", err)
		}

		eng, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}

		if !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(err.Error(), "unexpected EOF") && !strings.Contains(err.Error(), "failed to read") {
			t.Errorf("Expected an unexpected EOF or read error, but got: %v", err)
		}

	})

	t.Run("InvalidRecordLength", func(t *testing.T) {
		tempDir := t.TempDir()
		opts, walPath := setupWALWithData(t, tempDir, testEntries)

		segmentPath := filepath.Join(walPath, "00000001.wal")

		if err := setRecordLength(segmentPath, 1, 0xFFFFFFFF); err != nil {
			t.Fatalf("Failed to set invalid record length: %v", err)
		}

		eng, err := NewStorageEngine(opts)
		if err != nil {
			t.Fatal("NewStorageEngine should not have failed on corrupted WAL header, but it did")
		}

		if err = eng.Start(); err == nil {
			t.Fatal("NewStorageEngine should have failed on corrupted WAL header, but it succeeded")
		}
		if !strings.Contains(err.Error(), "exceeds sanity limit") {
			t.Errorf("Expected error to contain 'exceeds sanity limit', but got: %v", err)
		}
	})
}

func TestStorageEngine_Recovery_CorruptedWALWithValidManifest(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_manifest_inconsistent_"),
		WALSyncMode:                  core.WALSyncAlways,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	engine1, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Phase 1: NewStorageEngine failed: %v", err)
	}
	if err = engine1.Start(); err != nil {
		t.Fatalf("Phase 1: Failed to start setup engine: %v", err)
	}

	pointInManifest := testDataPoint{"metric.stable", map[string]string{"state": "manifest"}, 1000, 100.0}
	if err := engine1.Put(context.Background(), HelperDataPoint(t, pointInManifest.metric, pointInManifest.tags, pointInManifest.timestamp, map[string]interface{}{"value": pointInManifest.value})); err != nil {
		t.Fatalf("Phase 1: Put failed: %v", err)
	}

	if err := engine1.Close(); err != nil {
		t.Fatalf("Phase 1: Clean close failed: %v", err)
	}

	opts.WALSyncMode = core.WALSyncAlways

	// Create a WAL directly using the central WAL implementation instead of
	// using the legacy `crashEngine` helper. This ensures the on-disk layout
	// matches the expectations of the central WAL and our corruption helpers.
	walDir := filepath.Join(opts.DataDir, "wal")
	wopts := wal.Options{
		Dir:                 walDir,
		SyncMode:            opts.WALSyncMode,
		MaxSegmentSize:      opts.WALMaxSegmentSize,
		PreallocateSegments: opts.WALPreallocateSegments,
		PreallocSize:        opts.WALPreallocSize,
		Logger:              opts.Logger,
		// Skip recovering existing segments when opening here; the test
		// explicitly corrupts the latest segment later. By starting
		// recovery at MaxUint64 we avoid reading potentially truncated
		// segments that the test intentionally manipulates.
		StartRecoveryIndex: math.MaxUint64,
	}
	w, _, err := wal.Open(wopts)
	if err != nil {
		t.Fatalf("Phase 2: failed to open central WAL for test: %v", err)
	}
	pointInWAL := testDataPoint{"metric.new", map[string]string{"state": "wal"}, 2000, 200.0}
	if err := func() error {
		dp := HelperDataPoint(t, pointInWAL.metric, pointInWAL.tags, pointInWAL.timestamp, map[string]interface{}{"value": pointInWAL.value})
		key := core.EncodeTSDBKeyWithString(dp.Metric, dp.Tags, dp.Timestamp)
		fv, ferr := core.NewFieldValuesFromMap(map[string]interface{}{"value": pointInWAL.value})
		if ferr != nil {
			return ferr
		}
		vb, verr := fv.Encode()
		if verr != nil {
			return verr
		}
		return w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: vb})
	}(); err != nil {
		_ = w.Close()
		t.Fatalf("Phase 2: Put failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Phase 2: closing wal failed: %v", err)
	}

	files, err := os.ReadDir(walDir)
	require.NoError(t, err)
	require.NotEmpty(t, files, "WAL directory should not be empty after phase 2")

	var latestSegmentName string
	var latestIndex uint64
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".wal") {
			idxStr := strings.TrimSuffix(f.Name(), ".wal")
			idx, _ := strconv.ParseUint(idxStr, 10, 64)
			if idx > latestIndex {
				latestIndex = idx
				latestSegmentName = f.Name()
			}
		}
	}
	require.NotEmpty(t, latestSegmentName, "Could not find latest WAL segment to corrupt")

	segmentToCorruptPath := filepath.Join(walDir, latestSegmentName)

	if err := corruptSegmentMagic(segmentToCorruptPath, 0xDEADBEEF); err != nil {
		t.Fatalf("Phase 3: Failed to corrupt WAL segment magic: %v", err)
	}

	eng3, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("Phase 4:  NewStorageEngine failed: %v", err)
	}
	if err = eng3.Start(); err == nil {
		t.Fatal("Phase 4: Start should have failed due to corrupted WAL, but it succeeded")
	}
	if !(strings.Contains(err.Error(), "invalid magic number") || strings.Contains(err.Error(), "unexpected EOF")) {
		t.Errorf("Expected error to contain 'invalid magic number' or 'unexpected EOF', but got: %v", err)
	}
}

func TestStorageEngine_WALRecovery_TagIndex(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_tag_index_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	// write WAL directly using central WAL API
	{
		walPath := filepath.Join(tempDir, "wal")
		wopts := wal.Options{
			Dir:                 walPath,
			SyncMode:            opts.WALSyncMode,
			MaxSegmentSize:      opts.WALMaxSegmentSize,
			PreallocateSegments: opts.WALPreallocateSegments,
			PreallocSize:        opts.WALPreallocSize,
			Logger:              opts.Logger,
		}
		w, _, err := wal.Open(wopts)
		require.NoError(t, err)

		metric1 := "cpu.usage"
		tags1 := map[string]string{"host": "serverA", "region": "us-east"}
		dp1 := HelperDataPoint(t, metric1, tags1, 1000, map[string]interface{}{"value": 50.0})
		k1 := core.EncodeTSDBKeyWithString(dp1.Metric, dp1.Tags, dp1.Timestamp)
		fv1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.0})
		vb1, _ := fv1.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k1, Value: vb1}))

		metric2 := "memory.free"
		tags2 := map[string]string{"host": "serverB", "region": "us-west"}
		dp2 := HelperDataPoint(t, metric2, tags2, 2000, map[string]interface{}{"value": 1024.0})
		k2 := core.EncodeTSDBKeyWithString(dp2.Metric, dp2.Tags, dp2.Timestamp)
		fv2, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 1024.0})
		vb2, _ := fv2.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k2, Value: vb2}))

		// delete series (timestamp -1)
		seriesDelKey := core.EncodeTSDBKeyWithString(metric1, tags1, -1)
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypeDelete, Key: seriesDelKey, Value: nil}))

		require.NoError(t, w.Close())
	}

	opts.Metrics = NewEngineMetrics(false, "wal_tag_index_recovery_")
	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()
	metric1 := "cpu.usage"
	tags1 := map[string]string{"host": "serverA", "region": "us-east"}

	retrievedKeys1, err := engine2.GetSeriesByTags(metric1, tags1)
	if err != nil || len(retrievedKeys1) != 0 {
		t.Errorf("Expected GetSeriesByTags to find 0 series for a deleted series after WAL recovery, but got %d keys: %v", len(retrievedKeys1), retrievedKeys1)
	}

	iter, err := engine2.Query(context.Background(), core.QueryParams{Metric: metric1, Tags: tags1, StartTime: 0, EndTime: 2000})
	if err != nil {
		t.Fatalf("Query for deleted series failed after WAL recovery: %v", err)
	}
	defer iter.Close()

	if iter.Next() {
		t.Errorf("Expected Query to return no data for a deleted series after WAL recovery, but it did. Iterator error: %v", iter.Error())
	}

	seriesKey1Str := string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverA", "region": "us-east"}))
	retrievedKeys1, err = engine2.GetSeriesByTags(metric1, tags1)
	if err != nil || len(retrievedKeys1) != 0 {
		t.Errorf("Expected series %x to NOT be found in tag index after WAL recovery, got %x", seriesKey1Str, retrievedKeys1)
	}

	metric2 := "memory.free"
	tags2 := map[string]string{"host": "serverB", "region": "us-west"}
	seriesKey2Str := string(core.EncodeSeriesKeyWithString(metric2, tags2))
	retrievedKeys2, err := engine2.GetSeriesByTags(metric2, tags2)
	if err != nil || len(retrievedKeys2) != 1 || retrievedKeys2[0] != seriesKey2Str {
		t.Errorf("Expected series %x to be found in tag index after WAL recovery, got %x", seriesKey2Str, retrievedKeys2)
	}
}

func TestStorageEngine_WALRecovery_RangeTombstones(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "wal_range_tombstone_"),
		WALSyncMode:                  core.WALSyncAlways,
		WALBatchSize:                 1,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
	}

	// write WAL directly using central WAL API
	{
		walPath := filepath.Join(tempDir, "wal")
		wopts := wal.Options{
			Dir:                 walPath,
			SyncMode:            opts.WALSyncMode,
			MaxSegmentSize:      opts.WALMaxSegmentSize,
			PreallocateSegments: opts.WALPreallocateSegments,
			PreallocSize:        opts.WALPreallocSize,
			Logger:              opts.Logger,
		}
		w, _, err := wal.Open(wopts)
		require.NoError(t, err)

		metric := "sensor.temp"
		tags := map[string]string{"location": "room1"}
		ts1 := int64(1000)
		ts2 := int64(2000)
		ts3 := int64(3000)

		dp1 := HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 10.0})
		k1 := core.EncodeTSDBKeyWithString(dp1.Metric, dp1.Tags, dp1.Timestamp)
		fv1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 10.0})
		vb1, _ := fv1.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k1, Value: vb1}))

		dp2 := HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0})
		k2 := core.EncodeTSDBKeyWithString(dp2.Metric, dp2.Tags, dp2.Timestamp)
		fv2, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 2.0})
		vb2, _ := fv2.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k2, Value: vb2}))

		dp3 := HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0})
		k3 := core.EncodeTSDBKeyWithString(dp3.Metric, dp3.Tags, dp3.Timestamp)
		fv3, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 3.0})
		vb3, _ := fv3.Encode()
		require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: k3, Value: vb3}))

		// range delete marker at ts2 -> ts2
		fvRange := make(core.FieldValues)
		if pv, perr := core.NewPointValue(ts2); perr == nil {
			fvRange["__range_delete_end"] = pv
			enc, e := fvRange.Encode()
			require.NoError(t, e)
			kr := core.EncodeTSDBKeyWithString(metric, tags, ts2)
			require.NoError(t, w.Append(core.WALEntry{EntryType: core.EntryTypeDelete, Key: kr, Value: enc}))
		}

		require.NoError(t, w.Close())
	}

	opts.Metrics = NewEngineMetrics(false, "wal_range_tombstone_recovery_")
	engine2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine (engine2) for recovery failed: %v", err)
	}
	if err = engine2.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	defer engine2.Close()
	metric := "sensor.temp"
	tags := map[string]string{"location": "room1"}

	_, err = engine2.Get(context.Background(), metric, tags, 2000)
	if err != sstable.ErrNotFound {
		t.Errorf("Expected range-deleted point at %d to be ErrNotFound after WAL recovery, got %v", 2000, err)
	}

	retrievedKeys, err := engine2.GetSeriesByTags(metric, tags)
	if err != nil || len(retrievedKeys) != 1 || retrievedKeys[0] != string(core.EncodeSeriesKeyWithString(metric, tags)) {
		t.Errorf("Expected series %s to be found in tag index after WAL recovery (despite range delete), got %v", string(core.EncodeSeriesKeyWithString(metric, tags)), retrievedKeys)
	}

	if _, err = engine2.Get(context.Background(), metric, tags, 1000); err != nil {
		t.Errorf("Expected point at %d to be found, got %v", 1000, err)
	}
	if _, err = engine2.Get(context.Background(), metric, tags, 3000); err != nil {
		t.Errorf("Expected point at %d to be found, got %v", 3000, err)
	}
}
