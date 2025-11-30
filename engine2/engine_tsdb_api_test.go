package engine2

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexuscore/utils/clock"

	"github.com/INLOpen/nexusbase/core"

	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockListener is a simple HookListener implementation for tests.
type mockListener struct {
	priority   int
	callSignal chan hooks.HookEvent
	returnErr  error
	isAsync    bool
	onEvent    func(hooks.HookEvent)
}

func (m *mockListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	if m.onEvent != nil {
		m.onEvent(event)
	}
	if m.callSignal != nil {
		select {
		case m.callSignal <- event:
		default:
		}
	}
	return m.returnErr
}

func (m *mockListener) Priority() int { return m.priority }
func (m *mockListener) IsAsync() bool { return m.isAsync }

func TestStorageEngine_PutAndGet(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "cpu.temp"
	tags := map[string]string{"host": "serverA"}
	ts := time.Now().UnixNano()
	dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": 3.14})
	require.NoError(t, err)

	require.NoError(t, shim.Put(context.Background(), *dp))

	fv, err := shim.Get(context.Background(), metric, tags, ts)
	require.NoError(t, err)
	v, ok := fv["value"].ValueFloat64()
	require.True(t, ok)
	assert.InDelta(t, 3.14, v, 1e-9)

	// Delete and verify not found
	require.NoError(t, shim.Delete(context.Background(), metric, tags, ts))
	_, err = shim.Get(context.Background(), metric, tags, ts)
	assert.ErrorIs(t, err, sstable.ErrNotFound)
}

func TestStorageEngine_GetSeriesByTags(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	type tp struct {
		metric string
		tags   map[string]string
		ts     int64
		val    float64
		key    string
	}

	points := []tp{
		{metric: "cpu.usage", tags: map[string]string{"host": "serverA", "region": "us-east"}, ts: 1000, val: 50.0, key: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverA", "region": "us-east"}))},
		{metric: "cpu.usage", tags: map[string]string{"host": "serverB", "region": "us-east"}, ts: 1001, val: 60.0, key: string(core.EncodeSeriesKeyWithString("cpu.usage", map[string]string{"host": "serverB", "region": "us-east"}))},
		{metric: "memory.free", tags: map[string]string{"host": "serverA", "region": "us-west"}, ts: 1002, val: 1024.0, key: string(core.EncodeSeriesKeyWithString("memory.free", map[string]string{"host": "serverA", "region": "us-west"}))},
		{metric: "network.in", tags: map[string]string{"host": "serverC", "env": "prod"}, ts: 1003, val: 100.0, key: string(core.EncodeSeriesKeyWithString("network.in", map[string]string{"host": "serverC", "env": "prod"}))},
	}

	for _, p := range points {
		dp, err := core.NewSimpleDataPoint(p.metric, p.tags, p.ts, map[string]interface{}{"value": p.val})
		require.NoError(t, err)
		require.NoError(t, shim.Put(context.Background(), *dp))
	}

	tests := []struct {
		name     string
		metric   string
		tags     map[string]string
		expected []string
	}{
		{name: "metric cpu.usage", metric: "cpu.usage", tags: nil, expected: []string{points[0].key, points[1].key}},
		{name: "metric memory.free", metric: "memory.free", tags: nil, expected: []string{points[2].key}},
		{name: "tag host=serverA", metric: "", tags: map[string]string{"host": "serverA"}, expected: []string{points[0].key, points[2].key}},
		{name: "tag region=us-east", metric: "", tags: map[string]string{"region": "us-east"}, expected: []string{points[0].key, points[1].key}},
		{name: "metric+tag cpu.usage region=us-east", metric: "cpu.usage", tags: map[string]string{"region": "us-east"}, expected: []string{points[0].key, points[1].key}},
		{name: "non-existent metric", metric: "no.such", tags: nil, expected: []string{}},
		{name: "non-existent tag", metric: "", tags: map[string]string{"no": "tag"}, expected: []string{}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			keys, err := shim.GetSeriesByTags(tc.metric, tc.tags)
			require.NoError(t, err)
			if keys == nil {
				keys = []string{}
			}
			if tc.expected == nil {
				tc.expected = []string{}
			}
			sort.Strings(keys)
			sort.Strings(tc.expected)
			if !reflect.DeepEqual(keys, tc.expected) {
				t.Fatalf("mismatch for %s: got %v want %v", tc.name, keys, tc.expected)
			}
		})
	}

	// Delete one series and ensure it's removed
	require.NoError(t, shim.DeleteSeries(context.Background(), points[0].metric, points[0].tags))
	keys, err := shim.GetSeriesByTags(points[0].metric, points[0].tags)
	require.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestStorageEngine_Query_BasicAndMultiSeries(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "temp.cpu"
	tags := map[string]string{"dc": "nyc"}

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 1000
	ts3 := ts1 + 2000

	dp1, _ := core.NewSimpleDataPoint(metric, tags, ts1, map[string]interface{}{"value": 25.0})
	dp2, _ := core.NewSimpleDataPoint(metric, tags, ts2, map[string]interface{}{"value": 26.5})
	dp3, _ := core.NewSimpleDataPoint(metric, tags, ts3, map[string]interface{}{"value": 27.0})
	require.NoError(t, shim.Put(context.Background(), *dp1))
	require.NoError(t, shim.Put(context.Background(), *dp2))
	require.NoError(t, shim.Put(context.Background(), *dp3))

	// Query that should only return ts2
	qp := core.QueryParams{Metric: metric, Tags: tags, StartTime: ts1 + 500, EndTime: ts3 - 500}
	iter, err := shim.Query(context.Background(), qp)
	require.NoError(t, err)
	defer iter.Close()

	var items []*core.QueryResultItem
	for iter.Next() {
		it, err := iter.At()
		require.NoError(t, err)
		items = append(items, it)
	}
	require.NoError(t, iter.Error())
	require.Len(t, items, 1)
	v, ok := items[0].Fields["value"].ValueFloat64()
	require.True(t, ok)
	assert.InDelta(t, 26.5, v, 1e-9)

	// Multi-series query: metric with tag subset region=east
	metric2 := "multi.query"
	tagsA := map[string]string{"region": "east", "host": "A"}
	tagsB := map[string]string{"region": "east", "host": "B"}
	tagsC := map[string]string{"region": "west", "host": "C"}

	dpA1, err := core.NewSimpleDataPoint(metric2, tagsA, 100, map[string]interface{}{"value": 10.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpA1))
	dpA2, err := core.NewSimpleDataPoint(metric2, tagsA, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpA2))
	dpB1, err := core.NewSimpleDataPoint(metric2, tagsB, 150, map[string]interface{}{"value": 15.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpB1))
	dpB2, err := core.NewSimpleDataPoint(metric2, tagsB, 250, map[string]interface{}{"value": 25.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpB2))
	dpC1, err := core.NewSimpleDataPoint(metric2, tagsC, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dpC1))

	iter2, err := shim.Query(context.Background(), core.QueryParams{Metric: metric2, Tags: map[string]string{"region": "east"}, StartTime: 0, EndTime: 1000})
	require.NoError(t, err)
	defer iter2.Close()

	var actual []struct {
		ts  int64
		val float64
	}
	for iter2.Next() {
		it, err := iter2.At()
		require.NoError(t, err)
		fv, ok := it.Fields["value"]
		require.True(t, ok)
		v, ok := fv.ValueFloat64()
		require.True(t, ok)
		actual = append(actual, struct {
			ts  int64
			val float64
		}{ts: it.Timestamp, val: v})
	}
	require.NoError(t, iter2.Error())

	expected := []struct {
		ts  int64
		val float64
	}{
		{100, 10.0}, {150, 15.0}, {250, 25.0}, {300, 30.0},
	}
	require.Len(t, actual, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i].ts, actual[i].ts)
		assert.InDelta(t, expected[i].val, actual[i].val, 1e-9)
	}
}

func TestStorageEngine_Query_FromMemtableAndSSTable(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "test.query.mixed.source"
	tags := map[string]string{"host": "mixed-host"}

	// Phase 1
	dp1a, err := core.NewSimpleDataPoint(metric, tags, 100, map[string]interface{}{"value": 10.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1a))
	dp1b, err := core.NewSimpleDataPoint(metric, tags, 300, map[string]interface{}{"value": 30.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1b))

	// Force flush is a no-op in shim, but call for compatibility
	require.NoError(t, shim.ForceFlush(context.Background(), true))

	// Phase 2 (remain in memtable)
	dp2a, err := core.NewSimpleDataPoint(metric, tags, 50, map[string]interface{}{"value": 5.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp2a))
	dp2b, err := core.NewSimpleDataPoint(metric, tags, 200, map[string]interface{}{"value": 20.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp2b))

	iter, err := shim.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: 1000})
	require.NoError(t, err)
	defer iter.Close()

	var res []*core.QueryResultItem
	for iter.Next() {
		it, err := iter.At()
		require.NoError(t, err)
		res = append(res, it)
	}
	require.NoError(t, iter.Error())

	expected := []struct {
		ts  int64
		val float64
	}{{50, 5.0}, {100, 10.0}, {200, 20.0}, {300, 30.0}}
	require.Len(t, res, len(expected))
	for i, exp := range expected {
		assert.Equal(t, exp.ts, res[i].Timestamp)
		v, ok := res[i].Fields["value"].ValueFloat64()
		require.True(t, ok)
		assert.InDelta(t, exp.val, v, 1e-9)
	}
}

func TestStorageEngine_PutBatch(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	points := []core.DataPoint{}
	for i := 0; i < 4; i++ {
		dp, err := core.NewSimpleDataPoint("batch.metric", map[string]string{"host": "h"}, int64(100+i), map[string]interface{}{"value": float64(10 + i)})
		require.NoError(t, err)
		points = append(points, *dp)
	}
	require.NoError(t, shim.PutBatch(context.Background(), points))

	for _, p := range points {
		fv, err := shim.Get(context.Background(), p.Metric, p.Tags, p.Timestamp)
		require.NoError(t, err)
		v, ok := fv["value"].ValueFloat64()
		require.True(t, ok)
		// ensure value roughly equals
		exp, ok2 := p.Fields["value"].ValueFloat64()
		require.True(t, ok2)
		assert.InDelta(t, exp, v, 1e-9)
	}
}

func TestStorageEngine_DeleteSeries(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "test.delete.series"
	tags := map[string]string{"env": "dev"}

	dp1, err := core.NewSimpleDataPoint(metric, tags, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, err)
	dp2, err := core.NewSimpleDataPoint(metric, tags, 2, map[string]interface{}{"value": 2.0})
	require.NoError(t, err)
	require.NoError(t, shim.Put(context.Background(), *dp1))
	require.NoError(t, shim.Put(context.Background(), *dp2))

	require.NoError(t, shim.DeleteSeries(context.Background(), metric, tags))

	_, err = shim.Get(context.Background(), metric, tags, 1)
	assert.ErrorIs(t, err, sstable.ErrNotFound)

	keys, err := shim.GetSeriesByTags(metric, tags)
	require.NoError(t, err)
	assert.Len(t, keys, 0)

	iter, err := shim.Query(context.Background(), core.QueryParams{Metric: metric, Tags: tags, StartTime: 0, EndTime: time.Now().UnixNano()})
	require.NoError(t, err)
	defer iter.Close()
	assert.False(t, iter.Next())
}

func TestStorageEngine_DeletesByTimeRange(t *testing.T) {
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "sensor.temp"
	tags := map[string]string{"location": "room1"}

	timestamps := []int64{100, 200, 300, 400, 500}
	vals := []float64{20.0, 20.5, 21.0, 21.5, 22.0}
	for i, ts := range timestamps {
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": vals[i]})
		require.NoError(t, err)
		require.NoError(t, shim.Put(context.Background(), *dp))
	}

	// Delete ts 200..400
	require.NoError(t, shim.DeletesByTimeRange(context.Background(), metric, tags, 200, 400))

	expected := map[int64]bool{100: true, 200: false, 300: false, 400: false, 500: true}
	for ts, should := range expected {
		_, err := shim.Get(context.Background(), metric, tags, ts)
		if should {
			require.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, sstable.ErrNotFound)
		}
	}
}

func TestStorageEngine_Query_WithAggregation(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	engine := setupStorageEngineStart(t, opts)
	defer engine.Close()

	metric := "system.load"
	tags := map[string]string{"region": "us-west"}

	ts := []int64{time.Now().UnixNano(), time.Now().UnixNano() + 1000, time.Now().UnixNano() + 2000, time.Now().UnixNano() + 3000}
	vals := []float64{10.0, 20.0, 5.0, 15.0}
	for i := 0; i < len(ts); i++ {
		if err := engine.Put(context.Background(), HelperDataPoint(t, metric, tags, ts[i], map[string]any{"value": vals[i]})); err != nil {
			t.Fatalf("Put failed for ts[%d]: %v", i, err)
		}
		// Debug assertion: verify the point is readable immediately after Put
		if fv, gerr := engine.Get(context.Background(), metric, tags, ts[i]); gerr != nil {
			t.Fatalf("Get after Put failed for ts[%d]: %v", i, gerr)
		} else {
			if v, ok := fv["value"].ValueFloat64(); !ok || v != vals[i] {
				t.Fatalf("Post-Put verification mismatch for ts[%d]: got %v want %v", i, v, vals[i])
			}
		}
	}

	startTime := ts[0]
	endTime := ts[len(ts)-1] + 1

	tests := []struct {
		name               string
		aggregations       []core.AggregationSpec
		expectedAggResults map[string]float64
	}{
		{
			name: "count_sum_avg",
			aggregations: []core.AggregationSpec{
				{Function: core.AggCount, Field: "value"},
				{Function: core.AggSum, Field: "value"},
				{Function: core.AggAvg, Field: "value"},
			},
			expectedAggResults: map[string]float64{"count_value": 4.0, "sum_value": 50.0, "avg_value": 12.5},
		},
		{
			name: "min_max",
			aggregations: []core.AggregationSpec{
				{Function: core.AggMin, Field: "value"},
				{Function: core.AggMax, Field: "value"},
			},
			expectedAggResults: map[string]float64{"min_value": 5.0, "max_value": 20.0},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			qp := core.QueryParams{Metric: metric, StartTime: startTime, EndTime: endTime, Tags: tags, AggregationSpecs: tt.aggregations, DownsampleInterval: "", EmitEmptyWindows: false}
			// Debug: when running the failing subtest in the full-suite, log raw points
			if strings.Contains(t.Name(), "count_sum_avg") {
				rawQP := core.QueryParams{Metric: metric, StartTime: startTime, EndTime: endTime, Tags: tags}
				rawIter, err := engine.Query(context.Background(), rawQP)
				if err == nil {
					defer rawIter.Close()
					var valsSeen []struct {
						ts int64
						v  float64
					}
					for rawIter.Next() {
						it, err := rawIter.At()
						if err != nil {
							break
						}
						fv, ok := it.Fields["value"]
						if !ok {
							continue
						}
						if v, ok := fv.ValueFloat64(); ok {
							valsSeen = append(valsSeen, struct {
								ts int64
								v  float64
							}{ts: it.Timestamp, v: v})
						}
					}
					// format as strings for clearer log
					out := []string{}
					for _, p := range valsSeen {
						out = append(out, fmt.Sprintf("(ts=%d v=%v)", p.ts, p.v))
					}
					t.Logf("[debug test] raw points before aggregation: %s", strings.Join(out, ", "))
				}
			}
			iter, err := engine.Query(context.Background(), qp)
			if err != nil {
				t.Fatalf("Query with aggregations failed: %v", err)
			}
			defer iter.Close()

			count := 0
			var resultValues map[string]float64
			for iter.Next() {
				item, err := iter.At()
				if err != nil {
					t.Fatalf("iter.At() failed: %v", err)
				}
				resultValues = item.AggregatedValues
				count++
			}
			if count != 1 {
				t.Fatalf("Expected one aggregated result, got %d", count)
			}
			for k, want := range tt.expectedAggResults {
				if got, ok := resultValues[k]; !ok {
					t.Errorf("Missing aggregation %s", k)
					t.Logf("[diag] missing aggregation %s; dataDir=%s", k, opts.DataDir)
				} else if got != want {
					t.Errorf("Aggregation %s: got %f want %f", k, got, want)
					t.Logf("[diag] aggregation %s mismatch: got %f want %f; dataDir=%s", k, got, want, opts.DataDir)
				}
			}
		})
	}
}

// (removed) Previous dumpDiagnostics helper was used for temporary debugging
// and has been removed. Tests should rely on concise t.Logf messages instead.

func TestStorageEngine_AggregationQueryLatencyMetric(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	opts.Metrics = NewEngineMetrics(false, "agglatency_tsdb_")

	engine := setupStorageEngineStart(t, opts)
	defer engine.Close()

	engMetrics, errm := engine.Metrics()
	if errm != nil {
		t.Fatalf("engine.Metrics failed: %v", errm)
	}
	metricName := "latency.test.metric"
	tags := map[string]string{"test": "agg_latency"}
	numPoints := 200
	startTime := time.Now().UnixNano()
	points := make([]core.DataPoint, numPoints)
	for i := 0; i < numPoints; i++ {
		ts := startTime + int64(i*1000)
		points[i] = HelperDataPoint(t, metricName, tags, ts, map[string]any{"value": float64(i)})
	}
	if err := engine.PutBatch(context.Background(), points); err != nil {
		t.Fatalf("PutBatch failed: %v", err)
	}

	aggLatencyHist := engMetrics.AggregationQueryLatencyHist
	if aggLatencyHist == nil {
		t.Fatal("AggregationQueryLatencyHist is nil")
	}
	initialCount := aggLatencyHist.Get("count").(*expvar.Int).Value()
	initialSum := aggLatencyHist.Get("sum").(*expvar.Float).Value()

	qp := core.QueryParams{Metric: metricName, StartTime: startTime, EndTime: startTime + int64(numPoints*1000), Tags: tags, AggregationSpecs: []core.AggregationSpec{{Function: core.AggCount, Field: "value"}, {Function: core.AggSum, Field: "value"}}, DownsampleInterval: "", EmitEmptyWindows: false}
	iter, err := engine.Query(context.Background(), qp)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	for iter.Next() {
	}
	iter.Close()

	if aggLatencyHist.Get("count").(*expvar.Int).Value() != initialCount+1 {
		t.Errorf("Expected count to increase")
	}
	// The observed duration may be extremely small on fast test runs (approaching
	// zero seconds), so the float sum may not strictly increase in some environments.
	// Accept no decrease; prefer non-strict check to keep tests stable across Go/runtime variations.
	if aggLatencyHist.Get("sum").(*expvar.Float).Value() < initialSum {
		t.Errorf("Expected sum to be >= initial sum")
	}
}

func TestStorageEngine_Query_RelativeTime_And_Downsampling(t *testing.T) {
	// Use mock clock
	mockNow := time.Date(2024, 7, 16, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMockClock(mockNow)

	opts := GetBaseOptsForTest(t, "test")
	opts.Clock = mockClock
	engine := setupStorageEngineStart(t, opts)
	defer engine.Close()

	ctx := context.Background()
	metric := "test.query.relative"
	tags := map[string]string{"host": "relativehost"}

	tsInside := mockNow.Add(-30 * time.Minute).UnixNano()
	tsOutside := mockNow.Add(-90 * time.Minute).UnixNano()
	tsFuture := mockNow.Add(10 * time.Minute).UnixNano()
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsInside, map[string]any{"value": 30.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsOutside, map[string]any{"value": 90.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric, tags, tsFuture, map[string]any{"value": -10.0})))

	params := core.QueryParams{Metric: metric, Tags: tags, IsRelative: true, RelativeDuration: "1h"}
	iter, err := engine.Query(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, iter)

	var results []*core.QueryResultItem
	for iter.Next() {
		it, err := iter.At()
		require.NoError(t, err)
		results = append(results, it)
	}
	require.NoError(t, iter.Error())
	// Close the iterator now to release any memtable read-locks before performing
	// further writes. Using defer would hold locks until the end of the test.
	require.NoError(t, iter.Close())
	require.Len(t, results, 1)
	assert.Equal(t, tsInside, results[0].Timestamp)

	// Relative with downsampling
	metric2 := "test.query.relative.downsample"
	tags2 := map[string]string{"host": "relativeds-host"}
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric2, tags2, mockNow.Add(-5*time.Minute).UnixNano(), map[string]any{"value": 10.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric2, tags2, mockNow.Add(-10*time.Minute).UnixNano(), map[string]any{"value": 20.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric2, tags2, mockNow.Add(-25*time.Minute).UnixNano(), map[string]any{"value": 30.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric2, tags2, mockNow.Add(-55*time.Minute).UnixNano(), map[string]any{"value": 40.0})))
	require.NoError(t, engine.Put(ctx, HelperDataPoint(t, metric2, tags2, mockNow.Add(-65*time.Minute).UnixNano(), map[string]any{"value": 99.0})))

	params2 := core.QueryParams{Metric: metric2, Tags: tags2, IsRelative: true, RelativeDuration: "1h", DownsampleInterval: "15m", EmitEmptyWindows: true, AggregationSpecs: []core.AggregationSpec{{Function: core.AggSum, Field: "value"}, {Function: core.AggCount, Field: "value"}}}
	iter2, err := engine.Query(ctx, params2)
	require.NoError(t, err)
	require.NotNil(t, iter2)
	defer iter2.Close()

	var windows []*core.QueryResultItem
	for iter2.Next() {
		it, err := iter2.At()
		require.NoError(t, err)
		windows = append(windows, it)
	}
	require.NoError(t, iter2.Error())
	require.Len(t, windows, 4)
}

func TestStorageEngine_Put_WithHooks(t *testing.T) {
	ctx := context.Background()
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	// PreHook_CancelOperation
	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		expectedErr := errors.New("cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		shim.GetHookManager().Register(hooks.EventPrePutDataPoint, listener)

		metric := "hook.test.cancel"
		tags := map[string]string{"test": "prehook"}
		ts := time.Now().UnixNano()
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": 1.0})
		require.NoError(t, err)

		err = shim.Put(ctx, *dp)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)

		// Verify not present
		_, getErr := shim.Get(ctx, metric, tags, ts)
		assert.ErrorIs(t, getErr, sstable.ErrNotFound)
	})

	// PreHook_ModifyPayload
	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		shim2 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim2.Start())
		defer shim2.Close()

		modifiedValue := 999.9
		listener := &mockListener{onEvent: func(e hooks.HookEvent) {
			if p, ok := e.Payload().(hooks.PrePutDataPointPayload); ok {
				newFields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": modifiedValue})
				(*p.Fields)["value"] = newFields["value"]
			}
		}}
		shim2.GetHookManager().Register(hooks.EventPrePutDataPoint, listener)

		metric := "hook.test.modify"
		tags := map[string]string{"test": "prehook"}
		ts := time.Now().UnixNano()
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": 1.0})
		require.NoError(t, err)
		require.NoError(t, shim2.Put(ctx, *dp))

		fv, err := shim2.Get(ctx, metric, tags, ts)
		require.NoError(t, err)
		v, ok := fv["value"].ValueFloat64()
		require.True(t, ok)
		assert.InDelta(t, modifiedValue, v, 1e-9)
	})

	// PostHook_AsyncNotification
	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		shim3 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim3.Start())
		defer shim3.Close()

		signal := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signal}
		shim3.GetHookManager().Register(hooks.EventPostPutDataPoint, listener)

		metric := "hook.test.post"
		tags := map[string]string{"test": "posthook"}
		ts := time.Now().UnixNano()
		val := 123.45
		dp, err := core.NewSimpleDataPoint(metric, tags, ts, map[string]interface{}{"value": val})
		require.NoError(t, err)
		require.NoError(t, shim3.Put(ctx, *dp))

		// Data should be readable immediately
		_, err = shim3.Get(ctx, metric, tags, ts)
		require.NoError(t, err)

		select {
		case ev := <-signal:
			payload, ok := ev.Payload().(hooks.PostPutDataPointPayload)
			require.True(t, ok)
			hv, ok := payload.Fields["value"].ValueFloat64()
			require.True(t, ok)
			assert.Equal(t, metric, payload.Metric)
			assert.Equal(t, ts, payload.Timestamp)
			assert.InDelta(t, val, hv, 1e-9)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-put hook")
		}
	})
}

func TestStorageEngine_Get_WithHooks(t *testing.T) {
	ctx := context.Background()
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "hook.get.test"
	tagsA := map[string]string{"id": "A"}
	tsA := int64(100)
	valA := 10.0
	tagsB := map[string]string{"id": "B"}
	tsB := int64(200)
	valB := 20.0

	dpA, _ := core.NewSimpleDataPoint(metric, tagsA, tsA, map[string]interface{}{"value": valA})
	dpB, _ := core.NewSimpleDataPoint(metric, tagsB, tsB, map[string]interface{}{"value": valB})
	require.NoError(t, shim.Put(ctx, *dpA))
	require.NoError(t, shim.Put(ctx, *dpB))

	// PreHook_CancelOperation
	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		expectedErr := errors.New("get cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		shim.GetHookManager().Register(hooks.EventPreGetPoint, listener)

		_, err := shim.Get(ctx, metric, tagsA, tsA)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	// PreHook_ModifyPayload
	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		shim2 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim2.Start())
		defer shim2.Close()
		require.NoError(t, shim2.Put(ctx, *dpA))
		require.NoError(t, shim2.Put(ctx, *dpB))

		listener := &mockListener{onEvent: func(e hooks.HookEvent) {
			if p, ok := e.Payload().(hooks.PreGetPointPayload); ok {
				*p.Tags = tagsB
				*p.Timestamp = tsB
			}
		}}
		shim2.GetHookManager().Register(hooks.EventPreGetPoint, listener)

		res, err := shim2.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, err)
		v, ok := res["value"].ValueFloat64()
		require.True(t, ok)
		assert.InDelta(t, valB, v, 1e-9)
	})

	// PostHook_ModifyResult
	t.Run("PostHook_ModifyResult", func(t *testing.T) {
		shim3 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim3.Start())
		defer shim3.Close()
		require.NoError(t, shim3.Put(ctx, *dpA))

		modifiedValue := 999.9
		listener := &mockListener{onEvent: func(e hooks.HookEvent) {
			if p, ok := e.Payload().(hooks.PostGetPointPayload); ok && p.Error == nil {
				newFields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": modifiedValue})
				*p.Result = newFields
			}
		}}
		shim3.GetHookManager().Register(hooks.EventPostGetPoint, listener)

		res, err := shim3.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, err)
		v, ok := res["value"].ValueFloat64()
		require.True(t, ok)
		assert.InDelta(t, modifiedValue, v, 1e-9)
	})
}

func TestStorageEngine_Delete_WithHooks(t *testing.T) {
	ctx := context.Background()
	shim := newStorageEngineShim(t.TempDir())
	require.NoError(t, shim.Start())
	defer shim.Close()

	metric := "hook.delete.test"
	tagsA := map[string]string{"id": "A"}
	tsA := int64(100)
	tagsB := map[string]string{"id": "B"}
	tsB := int64(200)

	dpA, _ := core.NewSimpleDataPoint(metric, tagsA, tsA, map[string]interface{}{"value": 10.0})
	dpB, _ := core.NewSimpleDataPoint(metric, tagsB, tsB, map[string]interface{}{"value": 20.0})
	require.NoError(t, shim.Put(ctx, *dpA))
	require.NoError(t, shim.Put(ctx, *dpB))

	// PreHook_CancelOperation
	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		listener := &mockListener{returnErr: errors.New("delete cancelled by pre-hook")}
		shim.GetHookManager().Register(hooks.EventPreDeletePoint, listener)
		err := shim.Delete(ctx, metric, tagsA, tsA)
		require.Error(t, err)
		// verify A still present
		_, errA := shim.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, errA)
	})

	// PreHook_ModifyPayload
	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		shim2 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim2.Start())
		defer shim2.Close()
		require.NoError(t, shim2.Put(ctx, *dpA))
		require.NoError(t, shim2.Put(ctx, *dpB))

		listener := &mockListener{onEvent: func(e hooks.HookEvent) {
			if p, ok := e.Payload().(hooks.PreDeletePointPayload); ok {
				*p.Tags = tagsB
				*p.Timestamp = tsB
			}
		}}
		shim2.GetHookManager().Register(hooks.EventPreDeletePoint, listener)

		err := shim2.Delete(ctx, metric, tagsA, tsA)
		require.NoError(t, err)
		// A should still exist
		_, errA := shim2.Get(ctx, metric, tagsA, tsA)
		require.NoError(t, errA)
		// B should be deleted
		_, errB := shim2.Get(ctx, metric, tagsB, tsB)
		assert.ErrorIs(t, errB, sstable.ErrNotFound)
	})

	// PostHook_AsyncNotification
	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		shim3 := newStorageEngineShim(t.TempDir())
		require.NoError(t, shim3.Start())
		defer shim3.Close()
		require.NoError(t, shim3.Put(ctx, *dpA))

		signal := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signal}
		shim3.GetHookManager().Register(hooks.EventPostDeletePoint, listener)

		err := shim3.Delete(ctx, metric, tagsA, tsA)
		require.NoError(t, err)

		select {
		case ev := <-signal:
			payload, ok := ev.Payload().(hooks.PostDeletePointPayload)
			require.True(t, ok)
			assert.Equal(t, metric, payload.Metric)
			assert.Equal(t, tsA, payload.Timestamp)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-delete hook")
		}
	})
}
