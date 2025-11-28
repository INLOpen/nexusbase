package engine2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/stretchr/testify/require"
)

const localMaxFlushRetries = 3

// Test MoveToDLQ behavior
func Test_MoveToDLQ(t *testing.T) {
	t.Run("Success_WithData", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable2(1024, a.clk)
		// Use DataPoint-centric API for Memtable2
		pv, _ := core.NewPointValue("value1")
		dp := &core.DataPoint{Metric: "dlq.metric.1", Tags: nil, Timestamp: 100, Fields: core.FieldValues{"v": pv}}
		mem.Put(dp)

		err = a.MoveToDLQ(mem)
		require.NoError(t, err)

		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)

		// Ensure file is non-empty
		st, err := os.Stat(filepath.Join(a.GetDLQDir(), files[0].Name()))
		require.NoError(t, err)
		require.Greater(t, st.Size(), int64(0))
	})

	t.Run("Success_EmptyMemtable", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable2(1024, a.clk)

		err = a.MoveToDLQ(mem)
		require.NoError(t, err)

		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)

		st, err := os.Stat(filepath.Join(a.GetDLQDir(), files[0].Name()))
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())
	})

	t.Run("Failure_DLQDirNotConfigured", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		// Simulate not configured by clearing engine data root
		a.Engine2 = &Engine2{} // zero-value Engine2 -> GetDataRoot() should be empty

		mem := memtable.NewMemtable2(1024, a.clk)
		pv, _ := core.NewPointValue("val")
		dp := &core.DataPoint{Metric: "metric.key", Tags: nil, Timestamp: 1, Fields: core.FieldValues{"f": pv}}
		mem.Put(dp)

		err = a.MoveToDLQ(mem)
		require.Error(t, err)
	})
}

// Test processing immutable memtables with retries and DLQ behavior using adapter's FlushMemtableToL0
func Test_ProcessImmutableUsingAdapter(t *testing.T) {
	t.Run("Success_FirstTry", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable2(1024, a.clk)
		pv, _ := core.NewPointValue("v")
		dp := &core.DataPoint{Metric: "metric.test", Tags: nil, Timestamp: 12345, Fields: core.FieldValues{"v": pv}}
		mem.Put(dp)

		// Attempt flush: should succeed on first try
		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		require.NoError(t, err)

		// ensure SSTable exists
		files, err := os.ReadDir(filepath.Join(dir, "sstables"))
		require.NoError(t, err)
		require.Greater(t, len(files), 0)
	})

	t.Run("Success_AfterOneRetry", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		// Fail once, then succeed
		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(1)

		mem := memtable.NewMemtable2(1024, a.clk)
		pv, _ := core.NewPointValue("val")
		dp := &core.DataPoint{Metric: "metric.key", Tags: nil, Timestamp: 1, Fields: core.FieldValues{"f": pv}}
		mem.Put(dp)

		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		require.NoError(t, err)
		require.Equal(t, 1, mem.FlushRetries)
	})

	t.Run("Failure_MovesToDLQ", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		// Fail max times
		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(int32(localMaxFlushRetries))

		mem := memtable.NewMemtable2(1024, a.clk)
		pv, _ := core.NewPointValue("v")
		dp := &core.DataPoint{Metric: "metric.dlq", Tags: nil, Timestamp: 11111, Fields: core.FieldValues{"v": pv}}
		mem.Put(dp)

		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		// Should return error after exhausting retries
		require.Error(t, err)
		require.Equal(t, localMaxFlushRetries, mem.FlushRetries)

		// Explicitly move to DLQ as the legacy process would
		require.NoError(t, a.MoveToDLQ(mem))
		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)
	})

	t.Run("Failure_RequeuedOnShutdown", func(t *testing.T) {
		dir := t.TempDir()
		ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
		require.NoError(t, err)
		a := ai.(*Engine2Adapter)
		require.NoError(t, a.Start())
		defer a.Close()

		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(5) // fail more than max

		// Add notify + block channels so the test can deterministically wait
		// for the adapter to attempt a flush and then control when it returns
		// the simulated error. This avoids timing races.
		a.TestingOnlyFlushNotify = make(chan struct{}, 1)
		a.TestingOnlyFlushBlock = make(chan struct{})
		// Ensure test hooks are cleared when the test finishes to avoid
		// accidental interference with other tests.
		defer func() {
			a.TestingOnlyFlushNotify = nil
			a.TestingOnlyFlushBlock = nil
		}()

		mem := memtable.NewMemtable2(1024, a.clk)
		pv, _ := core.NewPointValue("v")
		dp := &core.DataPoint{Metric: "metric.shutdown", Tags: nil, Timestamp: 22222, Fields: core.FieldValues{"v": pv}}
		mem.Put(dp)

		// Simulate background processing with shutdown requeue
		var requeue []*memtable.Memtable2
		shutdownCh := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// simple loop that retries and respects shutdown signal
			for {
				select {
				default:
					err := a.FlushMemtableToL0(mem, context.Background())
					if err == nil {
						return
					}
					mem.FlushRetries++
					if mem.FlushRetries >= localMaxFlushRetries {
						// would move to DLQ in real engine; we stop here
						return
					}
					// continue retrying (no delay for test)
				case <-shutdownCh:
					// requeue at front
					requeue = append([]*memtable.Memtable2{mem}, requeue...)
					return
				}
			}
		}()

		// Wait for the adapter to signal that a simulated flush failure was
		// returned, then signal shutdown to exercise the requeue-on-shutdown
		// path deterministically.
		select {
		case <-a.TestingOnlyFlushNotify:
			// signal shutdown while the adapter is blocked
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for adapter flush attempt")
		}
		// signal shutdown and then allow the adapter to return its simulated
		// failure so the background goroutine can observe the shutdown and
		// requeue the memtable.
		close(shutdownCh)
		close(a.TestingOnlyFlushBlock)
		wg.Wait()

		require.Len(t, requeue, 1)
		require.Equal(t, mem, requeue[0])
	})
}

// processImmutableUsingAdapter attempts to flush the memtable using the adapter
// and performs retries up to maxRetries. It increments mem.FlushRetries on each failure.
func processImmutableUsingAdapter(a *Engine2Adapter, mem *memtable.Memtable2, maxRetries int) error {
	for {
		if err := a.FlushMemtableToL0(mem, context.Background()); err == nil {
			return nil
		}
		mem.FlushRetries++
		if mem.FlushRetries >= maxRetries {
			return fmt.Errorf("failed after max retries")
		}
	}
}
