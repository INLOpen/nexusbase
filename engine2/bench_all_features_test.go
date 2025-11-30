package engine2

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

// setupEngineForBench creates an Engine2 and adapter rooted at a temp dir
// for benchmarks. The cleanup func closes the engine.
func setupEngineForBench(tb testing.TB) (*Engine2, *Engine2Adapter, func()) {
	dataDir := tb.(*testing.B).TempDir()
	opts := StorageEngineOptions{DataDir: dataDir}
	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		tb.Fatalf("failed to create engine: %v", err)
	}
	a := NewEngine2Adapter(e)
	cleanup := func() {
		_ = e.Close()
	}
	return e, a, cleanup
}

// Benchmark: single Put operations
func BenchmarkEngine2_PutSingle(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": float64(i)})
		dp := core.DataPoint{Metric: "bench.put.single", Tags: map[string]string{"host": "h1"}, Timestamp: time.Now().UnixNano() + int64(i), Fields: fv}
		if err := a.Put(context.Background(), dp); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

}

// Benchmark: PutBatch operations with configurable batch size
func BenchmarkEngine2_PutBatch(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	batchSize := 100
	rnd := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]core.DataPoint, 0, batchSize)
		base := i * batchSize
		for j := 0; j < batchSize; j++ {
			fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": rnd.Float64()})
			dp := core.DataPoint{Metric: "bench.put.batch", Tags: map[string]string{"idx": "batch"}, Timestamp: time.Now().UnixNano() + int64(base+j), Fields: fv}
			batch = append(batch, dp)
		}
		if err := a.PutBatch(context.Background(), batch); err != nil {
			b.Fatalf("PutBatch failed: %v", err)
		}
	}
}

// Benchmark: Get operations (pre-populate a single series)
func BenchmarkEngine2_Get(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	// pre-populate
	seriesTags := map[string]string{"host": "g1"}
	ts := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": float64(i)})
		dp := core.DataPoint{Metric: "bench.get", Tags: seriesTags, Timestamp: ts + int64(i), Fields: fv}
		if err := a.Put(context.Background(), dp); err != nil {
			b.Fatalf("setup Put failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tgt := ts + int64(i%1000)
		if _, err := a.Get(context.Background(), "bench.get", seriesTags, tgt); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// Benchmark: Query operations across several series
func BenchmarkEngine2_Query(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	// pre-populate many series
	numSeries := 50
	pointsPerSeries := 200
	start := time.Now().UnixNano()
	for s := 0; s < numSeries; s++ {
		tags := map[string]string{"host": "srv", "id": fmt.Sprintf("%c", 'A'+s%26)}
		for p := 0; p < pointsPerSeries; p++ {
			fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": float64(p)})
			dp := core.DataPoint{Metric: "bench.query", Tags: tags, Timestamp: start + int64(p), Fields: fv}
			if err := a.Put(context.Background(), dp); err != nil {
				b.Fatalf("setup Put failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params := core.QueryParams{Metric: "bench.query", StartTime: start, EndTime: start + int64(pointsPerSeries)}
		it, err := a.Query(context.Background(), params)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		// iterate through results to exercise iterator path
		for it.Next() {
			// At() returns a pooled *QueryResultItem
			if _, err := it.At(); err != nil {
				b.Fatalf("iterator At failed: %v", err)
			}
		}
	}
}

// Benchmark: Delete and DeleteSeries simple coverage
func BenchmarkEngine2_DeleteAndSeries(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	tags := map[string]string{"host": "del"}
	ts := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": float64(i)})
		dp := core.DataPoint{Metric: "bench.del", Tags: tags, Timestamp: ts + int64(i), Fields: fv}
		if err := a.Put(context.Background(), dp); err != nil {
			b.Fatalf("setup Put failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.Delete(context.Background(), "bench.del", tags, ts+int64(i%1000)); err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
		if i%50 == 0 {
			if err := a.DeleteSeries(context.Background(), "bench.del", tags); err != nil {
				b.Fatalf("DeleteSeries failed: %v", err)
			}
		}
	}
}

// Integrated workload benchmark: mixes PutBatch/Get/Query/Delete to exercise
// a more realistic pipeline for the engine and adapter.
func BenchmarkEngine2_IntegratedWorkload(b *testing.B) {
	_, a, cleanup := setupEngineForBench(b)
	defer cleanup()

	// prepare series list
	series := make([]map[string]string, 100)
	for i := range series {
		series[i] = map[string]string{"host": "mix", "id": fmt.Sprintf("%c", 'A'+i%26)}
	}

	rnd := rand.New(rand.NewSource(12345))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// choose operation by weight
		r := rnd.Intn(100)
		idx := rnd.Intn(len(series))
		switch {
		case r < 60:
			// PutBatch
			batch := make([]core.DataPoint, 0, 20)
			baseTs := time.Now().UnixNano()
			for j := 0; j < 20; j++ {
				fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": rnd.Float64()})
				dp := core.DataPoint{Metric: "bench.mix", Tags: series[idx], Timestamp: baseTs + int64(j), Fields: fv}
				batch = append(batch, dp)
			}
			if err := a.PutBatch(context.Background(), batch); err != nil {
				b.Fatalf("PutBatch failed: %v", err)
			}
		case r < 80:
			// Get
			if _, err := a.Get(context.Background(), "bench.mix", series[idx], time.Now().UnixNano()); err != nil {
				// ignore not-found errors in mixed workload
			}
		case r < 95:
			// Query
			params := core.QueryParams{Metric: "bench.mix", StartTime: time.Now().Add(-time.Minute).UnixNano(), EndTime: time.Now().UnixNano()}
			it, err := a.Query(context.Background(), params)
			if err == nil {
				for it.Next() {
					_, _ = it.At()
				}
			}
		default:
			// Delete single point
			_ = a.Delete(context.Background(), "bench.mix", series[idx], time.Now().UnixNano())
		}
	}
}
