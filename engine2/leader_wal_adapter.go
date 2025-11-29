package engine2

import (
	"fmt"
	"path/filepath"

	"github.com/INLOpen/nexusbase/wal"
)

// openLeaderWAL opens a WAL from the main `wal` package under the engine2 data root.
// This WAL is used to expose a `wal.WALInterface` to replication and snapshot code
// while engine2 keeps its own simple per-engine WAL file for replay.
// The function accepts optional engine metrics so the WAL implementation can
// update engine-level expvar counters (bytes/entries written).
func openLeaderWAL(engine *Engine2, metrics *EngineMetrics) (wal.WALInterface, error) {
	// If we have an existing engine instance with an opened WAL, prefer
	// to return a thin wrapper around that WAL instance rather than
	// creating a second independent WAL writing to the same directory.
	// Creating two WAL instances against the same directory can cause
	// concurrent writers and corrupt segment contents.
	if engine != nil && engine.wal != nil {
		// When the engine already has an opened WAL, return the concrete
		// WAL instance rather than wrapping it. Tests and other callers
		// sometimes type-assert to *wal.WAL to install testing-only hooks
		// (e.g. `TestingOnlyStreamerRegistered`). Returning the concrete
		// pointer preserves that ability while still exposing the WAL via
		// the wal.WALInterface.
		return engine.wal, nil
	}

	// Otherwise, fall back to opening a standalone WAL in the data root.
	dir := filepath.Join(engine.options.DataDir, "wal")
	opts := wal.Options{Dir: dir}
	if metrics != nil {
		opts.BytesWritten = metrics.WALBytesWrittenTotal
		opts.EntriesWritten = metrics.WALEntriesWrittenTotal
	}
	w, _, err := wal.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open leader WAL at %s: %w", dir, err)
	}
	return w, nil
}
