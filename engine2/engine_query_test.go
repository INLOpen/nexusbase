package engine2

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupShimForTest creates a storageEngineShim for testing query behavior.
func setupEngineForTest(t *testing.T) StorageEngineInterface {
	t.Helper()
	opts := GetBaseOptsForTest(t, "test")
	eng := setupStorageEngineStart(t, opts)
	return eng
}

func TestStorageEngine_Query_Order(t *testing.T) {
	eng := setupEngineForTest(t)
	ctx := context.Background()
	metric := "test.query.order"
	tags := map[string]string{"host": "testhost"}
	baseTime := time.Now().Truncate(time.Minute)

	fields1, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 1.0})
	require.NoError(t, err)
	fields2, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 2.0})
	require.NoError(t, err)
	fields3, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 3.0})
	require.NoError(t, err)

	points := []core.DataPoint{
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(1 * time.Minute).UnixNano(), Fields: fields1},
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(2 * time.Minute).UnixNano(), Fields: fields2},
		{Metric: metric, Tags: tags, Timestamp: baseTime.Add(3 * time.Minute).UnixNano(), Fields: fields3},
	}

	// Put data using engine PutBatch
	require.NoError(t, eng.PutBatch(ctx, points))

	// Descending
	t.Run("Descending", func(t *testing.T) {
		params := core.QueryParams{
			Metric:    metric,
			Tags:      tags,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(5 * time.Minute).UnixNano(),
			Order:     types.Descending,
		}

		iter, err := eng.Query(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer iter.Close()

		var results []*core.QueryResultItem
		for iter.Next() {
			item, err := iter.At()
			require.NoError(t, err)
			results = append(results, item)
		}
		require.NoError(t, iter.Error())
		require.Len(t, results, 3)

		// Build expected timestamps in descending order and compare
		expectedDesc := []int64{points[2].Timestamp, points[1].Timestamp, points[0].Timestamp}
		for i := 0; i < len(expectedDesc); i++ {
			assert.Equal(t, expectedDesc[i], results[i].Timestamp)
		}
	})

	// Ascending
	t.Run("Ascending", func(t *testing.T) {
		params := core.QueryParams{
			Metric:    metric,
			Tags:      tags,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(5 * time.Minute).UnixNano(),
			Order:     types.Ascending,
		}

		iter, err := eng.Query(ctx, params)
		require.NoError(t, err)
		defer iter.Close()

		var results []*core.QueryResultItem
		for iter.Next() {
			item, err := iter.At()
			require.NoError(t, err)
			results = append(results, item)
		}
		require.NoError(t, iter.Error())
		require.Len(t, results, 3)

		expectedAsc := []int64{points[0].Timestamp, points[1].Timestamp, points[2].Timestamp}
		for i := 0; i < len(expectedAsc); i++ {
			assert.Equal(t, expectedAsc[i], results[i].Timestamp)
		}
	})
}
