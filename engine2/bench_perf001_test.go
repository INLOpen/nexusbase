package engine2

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

// BenchmarkPerf001 is a conversion of the perf-client workload into an
// in-process benchmark exercising the Engine2Adapter PutBatch path. It
// creates a minimal Engine2 rooted at a temp dir and performs concurrent
// PutBatch requests.
func BenchmarkPerf001(b *testing.B) {
	// allow quick overriding via env vars if needed
	// Defaults
	totalPoints := 10000
	numSeries := 100
	batchSize := 1000
	concurrency := 10
	warmupIters := 1
	measuredIters := 3
	captureProfile := true

	// Allow quick overriding via environment variables for flexible runs
	if v := os.Getenv("PERF_POINTS"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			totalPoints = x
		}
	}
	if v := os.Getenv("PERF_SERIES"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			numSeries = x
		}
	}
	if v := os.Getenv("PERF_BATCHSIZE"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			batchSize = x
		}
	}
	if v := os.Getenv("PERF_CONCURRENCY"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			concurrency = x
		}
	}
	if v := os.Getenv("PERF_WARMUP"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x >= 0 {
			warmupIters = x
		}
	}
	if v := os.Getenv("PERF_ITERS"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			measuredIters = x
		}
	}
	if v := os.Getenv("PERF_CAPTURE_PROFILE"); v != "" {
		if v == "0" || v == "false" {
			captureProfile = false
		}
	}

	// Generate series tag definitions
	seriesDefs := make([]map[string]string, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesDefs[i] = map[string]string{"host": fmt.Sprintf("host-%d", i), "region": fmt.Sprintf("region-%d", i%5)}
	}

	// Create a temp data dir for the engine
	dataDir := b.TempDir()

	// Engine options: minimal configuration
	opts := StorageEngineOptions{DataDir: dataDir}
	eng, err := NewEngine2(context.Background(), opts)
	if err != nil {
		b.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	a := NewEngine2Adapter(eng)

	// We'll generate points on-the-fly per-batch to reduce memory usage.
	startTime := time.Now().UnixNano()

	// helper to create a batch of points deterministically
	makeBatch := func(startIdx, count int, rnd *rand.Rand) []core.DataPoint {
		batch := make([]core.DataPoint, 0, count)
		for k := 0; k < count; k++ {
			i := startIdx + k
			seriesIndex := i % numSeries
			fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": rnd.Float64() * 100})
			dp := core.DataPoint{Metric: "perf.test.metric", Tags: seriesDefs[seriesIndex], Timestamp: startTime + int64(i), Fields: fv}
			batch = append(batch, dp)
		}
		return batch
	}

	// warm-up iterations (not measured)
	for wi := 0; wi < warmupIters; wi++ {
		var wg sync.WaitGroup
		pointsPerWorker := totalPoints / concurrency
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			ws := w
			go func() {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(int64(time.Now().UnixNano()) + int64(ws)))
				workerStart := ws * pointsPerWorker
				workerEnd := (ws + 1) * pointsPerWorker
				if ws == concurrency-1 {
					workerEnd = totalPoints
				}
				for j := workerStart; j < workerEnd; j += batchSize {
					be := j + batchSize
					if be > workerEnd {
						be = workerEnd
					}
					batch := makeBatch(j, be-j, rnd)
					if err := a.PutBatch(context.Background(), batch); err != nil {
						b.Fatalf("warmup PutBatch failed: %v", err)
					}
				}
			}()
		}
		wg.Wait()
	}

	// Measured iterations: capture optional CPU profile on first measured iter
	profPath := ""
	heapPath := ""
	for iter := 0; iter < measuredIters; iter++ {
		// optionally capture CPU profile on first measured iteration
		if iter == 0 && captureProfile {
			profDir := filepath.Join(os.TempDir(), "nexusbase_bench_profiles")
			_ = os.MkdirAll(profDir, 0o755)
			profPath = filepath.Join(profDir, fmt.Sprintf("bench_perf001_cpu_iter%d.pprof", time.Now().Unix()))
			f, ferr := os.Create(profPath)
			if ferr == nil {
				_ = pprof.StartCPUProfile(f)
				// ensure profile stopped and file closed after iteration
				defer func() {
					pprof.StopCPUProfile()
					_ = f.Close()
				}()
			} else {
				b.Logf("failed to create profile file: %v", ferr)
			}
		}

		var wg sync.WaitGroup
		pointsPerWorker := totalPoints / concurrency
		var mu sync.Mutex
		latencies := make([]time.Duration, 0, totalPoints/batchSize)

		start := time.Now()
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			ws := w
			go func() {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(int64(time.Now().UnixNano()) + int64(ws)))
				workerStart := ws * pointsPerWorker
				workerEnd := (ws + 1) * pointsPerWorker
				if ws == concurrency-1 {
					workerEnd = totalPoints
				}
				for j := workerStart; j < workerEnd; j += batchSize {
					be := j + batchSize
					if be > workerEnd {
						be = workerEnd
					}
					batch := makeBatch(j, be-j, rnd)
					now := time.Now()
					if err := a.PutBatch(context.Background(), batch); err != nil {
						b.Fatalf("PutBatch failed: %v", err)
					}
					dur := time.Since(now)
					mu.Lock()
					latencies = append(latencies, dur)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		_ = time.Since(start)

		// stop CPU profile if we started one for this iteration
		if iter == 0 && captureProfile {
			// Stop CPU profile and write heap profile as well
			pprof.StopCPUProfile()
			b.Logf("CPU profile written to %s", profPath)
			// Write heap profile
			profDir := filepath.Join(os.TempDir(), "nexusbase_bench_profiles")
			heapPath = filepath.Join(profDir, fmt.Sprintf("bench_perf001_heap_iter%d.pprof", time.Now().Unix()))
			if err := WriteHeapProfile(heapPath); err != nil {
				b.Logf("failed to write heap profile: %v", err)
			} else {
				b.Logf("Heap profile written to %s", heapPath)
			}
		}

		// simple sanity: sort latencies and report a few percentiles via b.Log
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		if len(latencies) > 0 {
			p50 := latencies[len(latencies)*50/100]
			p90 := latencies[len(latencies)*90/100]
			p99 := latencies[len(latencies)*99/100]
			b.Logf("measured_iter=%d total_points=%d batches=%d p50=%v p90=%v p99=%v", iter, totalPoints, len(latencies), p50, p90, p99)
		}
	}
}
