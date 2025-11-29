package engine2

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteSeries(t *testing.T) {
	// Use a real engine to exercise DeleteSeries behavior
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "delseries_tsdb_"),
	}
	eng := setupStorageEngineStart(t, opts)
	defer eng.Close()
	var err error

	metric := "test.delete.series"
	tags := map[string]string{"env": "dev"}

	// Put some data first
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano(), map[string]interface{}{"value": 1.0})))
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, time.Now().UnixNano()+1, map[string]interface{}{"value": 2.0})))

	// Call DeleteSeries
	err = eng.DeleteSeries(context.Background(), metric, tags)
	require.NoError(t, err)

	// Verify series is marked as deleted by trying to get a point.
	_, err = eng.Get(context.Background(), metric, tags, time.Now().UnixNano())
	require.ErrorIs(t, err, sstable.ErrNotFound)

	// Verify that GetSeriesByTags no longer finds the series key.
	retrievedKeys, err := eng.GetSeriesByTags(metric, tags)
	require.NoError(t, err)
	assert.Empty(t, retrievedKeys)

	// Verify that a Query for the series returns no data points.
	iter, err := eng.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: time.Now().UnixNano() * 2})
	require.NoError(t, err)
	defer iter.Close()
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Error())
}

func TestDeletesByTimeRange(t *testing.T) {
	tempDir := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:                      tempDir,
		MemtableThreshold:            1024 * 1024,
		CompactionIntervalSeconds:    3600,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		Metrics:                      NewEngineMetrics(false, "delrange_tsdb_"),
	}
	eng := setupStorageEngineStart(t, opts)
	defer eng.Close()
	var err error

	metric := "test.range.delete"
	tags := map[string]string{"loc": "lab"}

	startTime := int64(100)
	endTime := int64(200)

	// Put points inside and outside the range
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, startTime-1, map[string]interface{}{"value": 1.0})))
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, startTime, map[string]interface{}{"value": 2.0})))
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, endTime, map[string]interface{}{"value": 3.0})))
	require.NoError(t, eng.Put(context.Background(), HelperDataPoint(t, metric, tags, endTime+1, map[string]interface{}{"value": 4.0})))

	// Call DeletesByTimeRange
	err = eng.DeletesByTimeRange(context.Background(), metric, tags, startTime, endTime)
	require.NoError(t, err)

	// Verify points inside the range are deleted
	_, err = eng.Get(context.Background(), metric, tags, startTime)
	assert.ErrorIs(t, err, sstable.ErrNotFound)
	_, err = eng.Get(context.Background(), metric, tags, endTime)
	assert.ErrorIs(t, err, sstable.ErrNotFound)

	// Verify points outside the range still exist
	_, err = eng.Get(context.Background(), metric, tags, startTime-1)
	assert.NoError(t, err)
	_, err = eng.Get(context.Background(), metric, tags, endTime+1)
	assert.NoError(t, err)

	// Test invalid time range
	err = eng.DeletesByTimeRange(context.Background(), metric, tags, 200, 100)
	assert.Error(t, err)
}
