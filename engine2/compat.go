package engine2

import (
	"context"
	"log/slog"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
)

// Compatibility aliases to ease migrating callers from `engine` -> `engine2`.
// These aliases point to the original `engine` package types so caller code
// can switch imports to `engine2` with minimal changes. This is a temporary
// migration aid and will be removed when `engine` is deleted.
// Legacy compatibility shims live here. To keep engine2 independent from the
// legacy `engine` package, engine2 provides local test helpers and option
// types so tests and callers can migrate without relying on `engine`.

func NewStorageEngine(opts StorageEngineOptions) (StorageEngineInterface, error) {
	// Construct an engine2-backed storage engine and adapt it to the
	// repository StorageEngineInterface so callers get an engine2-backed
	// StorageEngine with minimal changes to their callsites.
	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	// If a specific logger was provided, use it as the temporary global
	// default while constructing components that consult slog.Default().
	// Restore the previous default afterwards.
	var prevLogger *slog.Logger
	if opts.Logger != nil {
		prevLogger = slog.Default()
		slog.SetDefault(opts.Logger)
	}
	// Wrap Engine2 in the adapter which implements engine2.StorageEngineInterface
	a := NewEngine2AdapterWithHooks(e, opts.HookManager)
	// Note: do not auto-start the adapter here. Callers should invoke
	// `Start()` explicitly when they want WAL replay and background
	// managers to be initialized. Tests and callers that depended on the
	// previous automatic-start behavior should be updated to call
	// `Start()` manually.
	if prevLogger != nil {
		slog.SetDefault(prevLogger)
	}
	// If caller provided a custom clock or metrics in options, apply them
	// to the adapter so behaviors like relative-time queries and metrics
	// visibility match the caller's expectations (tests pass a mock clock).
	if opts.Clock != nil {
		a.clk = opts.Clock
	}
	if opts.Metrics != nil {
		a.metrics = opts.Metrics
	}
	// propagate SSTable block size from provided options so tests can control
	// how many blocks are produced during memtable flushes.
	if opts.SSTableDefaultBlockSize > 0 {
		a.sstableDefaultBlockSize = opts.SSTableDefaultBlockSize
	}
	return a, nil
}

// GetActiveSeriesSnapshot delegates to the legacy engine helper so callers that
// switch imports to `engine2` can still obtain an active series snapshot
// without importing the legacy package directly. This is a temporary shim
// during migration and will be removed when helper functions are ported.
func GetActiveSeriesSnapshot(e StorageEngineExternal) ([]string, error) {
	// If it's the engine2 adapter, extract active series directly.
	if a, ok := e.(*Engine2Adapter); ok {
		a.activeSeriesMu.RLock()
		defer a.activeSeriesMu.RUnlock()
		out := make([]string, 0, len(a.activeSeries))
		for k := range a.activeSeries {
			out = append(out, k)
		}
		return out, nil
	}
	return nil, nil
}

// GetBaseOptsForTest delegates the engine test helper to provide standard
// StorageEngineOptions for tests. Returning the alias type lets callers
// import `engine2` only during migration.
// GetBaseOptsForTest returns engine2 test options. A lightweight test helper
// so engine2 tests don't need to import the legacy `engine` package.
func GetBaseOptsForTest(t *testing.T, prefix string) StorageEngineOptions {
	t.Helper()
	// Allow opt-in persistent test dirs for easier debugging. If the
	// environment variable `NEXUSBASE_PERSIST_TESTDIR` is set, use a
	// persistent directory under that path instead of `t.TempDir()` so a
	// developer can re-run a single failing test and inspect WAL/sstable
	// files afterward. This only activates when the env var is present.
	dataDir := t.TempDir()
	/*
		if base := os.Getenv("NEXUSBASE_PERSIST_TESTDIR"); base != "" {
			// Create a path like <base>/<prefix><TestName>
			persistent := filepath.Join(base, prefix+t.Name())
			_ = os.MkdirAll(persistent, 0755)
			dataDir = persistent
		}
	*/

	// Note: sys debug mode is no longer enabled via environment variable.

	// Provide minimal, sensible defaults used by engine2 tests. The full
	// StorageEngineOptions type exists in `options.go` so tests can still set
	// named fields if needed.
	return StorageEngineOptions{
		DataDir:                      dataDir,
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		BlockCacheCapacity:           100,
		MaxL0Files:                   4,
		TargetSSTableSize:            1024 * 1024,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  core.WALSyncDisabled,
		// Disable WAL preallocation in tests by default to avoid platform-
		// specific prealloc behavior affecting small test segments.
		WALPreallocateSegments:    func() *bool { b := false; return &b }(),
		CompactionIntervalSeconds: 3600,
		Metrics:                   NewEngineMetrics(false, prefix),
	}
}

// HelperDataPoint creates a core.DataPoint for tests.
func HelperDataPoint(t *testing.T, metric string, tags map[string]string, ts int64, fields map[string]any) core.DataPoint {
	t.Helper()
	dp, err := core.NewSimpleDataPoint(metric, tags, ts, fields)
	if err != nil {
		t.Fatalf("Failed to create data point: %v", err)
	}
	return *dp
}
