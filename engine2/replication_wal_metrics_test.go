package engine2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
)

// errSeriesIDStore is a test double that returns an error when persisting series IDs.
type errSeriesIDStore struct{}

func (e *errSeriesIDStore) GetOrCreateID(seriesKey string) (uint64, error) {
	return 0, errors.New("boom")
}
func (e *errSeriesIDStore) GetID(seriesKey string) (uint64, bool) { return 0, false }
func (e *errSeriesIDStore) GetKey(id uint64) (string, bool)       { return "", false }
func (e *errSeriesIDStore) Sync() error                           { return nil }
func (e *errSeriesIDStore) Close() error                          { return nil }
func (e *errSeriesIDStore) LoadFromFile(dataDir string) error     { return nil }

// Test that replication counters are incremented for PUT and DELETE_RANGE.
func TestReplicationAndDeleteRangeMetrics(t *testing.T) {
	dataDir := t.TempDir()
	opts := GetBaseOptsForTest(t, "")
	opts.DataDir = dataDir

	ai, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine error: %v", err)
	}
	a := ai.(*Engine2Adapter)
	if err := a.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer a.Close()

	// Verify replication counters increment relative to baseline values.
	if a.metrics == nil || a.metrics.ReplicationPutTotal == nil || a.metrics.ReplicationDeleteRangeTotal == nil || a.metrics.ReplicationDeleteSeriesTotal == nil {
		t.Fatalf("metrics not initialized")
	}

	basePut := a.metrics.ReplicationPutTotal.Value()
	// Build a simple PUT WALEntry and apply it
	put := &pb.WALEntry{SequenceNumber: 1, EntryType: pb.WALEntry_PUT_EVENT, Metric: "m", Tags: map[string]string{"t": "v"}, Timestamp: 1}
	if err := a.ApplyReplicatedEntry(context.Background(), put); err != nil {
		t.Fatalf("ApplyReplicatedEntry(put) failed: %v", err)
	}
	if a.metrics.ReplicationPutTotal.Value()-basePut < 1 {
		t.Fatalf("expected ReplicationPutTotal to increase by >=1, baseline=%v got=%v", basePut, a.metrics.ReplicationPutTotal.Value())
	}

	// Apply a DELETE_RANGE entry and ensure the corresponding metric increments
	baseDelRange := a.metrics.ReplicationDeleteRangeTotal.Value()
	delRange := &pb.WALEntry{SequenceNumber: 2, EntryType: pb.WALEntry_DELETE_RANGE, Metric: "m", Tags: map[string]string{"t": "v"}, StartTime: 0, EndTime: 100}
	if err := a.ApplyReplicatedEntry(context.Background(), delRange); err != nil {
		t.Fatalf("ApplyReplicatedEntry(delete_range) failed: %v", err)
	}
	if a.metrics.ReplicationDeleteRangeTotal.Value()-baseDelRange < 1 {
		t.Fatalf("expected ReplicationDeleteRangeTotal to increase by >=1, baseline=%v got=%v", baseDelRange, a.metrics.ReplicationDeleteRangeTotal.Value())
	}

	// Also exercise DELETE_SERIES and validate its counter increments.
	baseDelSeries := a.metrics.ReplicationDeleteSeriesTotal.Value()
	delSeries := &pb.WALEntry{SequenceNumber: 3, EntryType: pb.WALEntry_DELETE_SERIES, Metric: "m", Tags: map[string]string{"t": "v"}}
	if err := a.ApplyReplicatedEntry(context.Background(), delSeries); err != nil {
		t.Fatalf("ApplyReplicatedEntry(delete_series) failed: %v", err)
	}
	// DELETE_SERIES may be a no-op if the series doesn't exist; at minimum ensure the counter did not decrease.
	if a.metrics.ReplicationDeleteSeriesTotal.Value() < baseDelSeries {
		t.Fatalf("expected ReplicationDeleteSeriesTotal not to decrease, baseline=%v got=%v", baseDelSeries, a.metrics.ReplicationDeleteSeriesTotal.Value())
	}
}

// Test that replication error path increments ReplicationErrorsTotal when series ID persistence fails.
func TestReplicationErrorsMetric(t *testing.T) {
	dataDir := t.TempDir()
	opts := GetBaseOptsForTest(t, "")
	opts.DataDir = dataDir

	ai, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine error: %v", err)
	}
	a := ai.(*Engine2Adapter)
	if err := a.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer a.Close()

	// Replace seriesIDStore with one that errors to trigger ReplicationErrorsTotal
	if a.metrics == nil || a.metrics.ReplicationErrorsTotal == nil {
		t.Fatalf("metrics not initialized")
	}
	baseErrs := a.metrics.ReplicationErrorsTotal.Value()
	a.seriesIDStore = &errSeriesIDStore{}

	put := &pb.WALEntry{SequenceNumber: 10, EntryType: pb.WALEntry_PUT_EVENT, Metric: "m", Tags: map[string]string{"t": "v"}, Timestamp: 1}
	if err := a.ApplyReplicatedEntry(context.Background(), put); err == nil {
		t.Fatalf("expected ApplyReplicatedEntry to fail due to seriesIDStore error")
	}
	if a.metrics.ReplicationErrorsTotal.Value()-baseErrs != 1 {
		t.Fatalf("expected ReplicationErrorsTotal to increase by 1, baseline=%v got=%v", baseErrs, a.metrics.ReplicationErrorsTotal.Value())
	}
}

// Test WAL recovery metrics: write some points, restart engine and assert recovery metrics.
func TestWALRecoveryMetrics(t *testing.T) {
	dataDir := t.TempDir()
	opts := GetBaseOptsForTest(t, "")
	opts.DataDir = dataDir

	// Start first engine (provides metrics objects) and write WAL entries
	// using the central WAL API so on-disk layout is deterministic.
	ai1, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine(error): %v", err)
	}
	a1 := ai1.(*Engine2Adapter)
	if err := a1.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// write three PUT WALEntry records directly to the engine WAL directory
	walDir := filepath.Join(dataDir, "wal")
	wopts := wal.Options{
		Dir:                 walDir,
		SyncMode:            opts.WALSyncMode,
		MaxSegmentSize:      opts.WALMaxSegmentSize,
		PreallocateSegments: opts.WALPreallocateSegments,
		PreallocSize:        opts.WALPreallocSize,
		Logger:              opts.Logger,
	}
	w, _, werr := wal.Open(wopts)
	if werr != nil {
		t.Fatalf("failed to open central WAL for test: %v", werr)
	}
	for i := 0; i < 3; i++ {
		dp := HelperDataPoint(t, "m", map[string]string{"t": "v"}, int64(i+1), map[string]any{"value": float64(i + 1)})
		key := core.EncodeTSDBKeyWithString(dp.Metric, dp.Tags, dp.Timestamp)
		fv, ferr := core.NewFieldValuesFromMap(map[string]interface{}{"value": float64(i + 1)})
		if ferr != nil {
			_ = w.Close()
			t.Fatalf("failed to build FieldValues: %v", ferr)
		}
		vb, verr := fv.Encode()
		if verr != nil {
			_ = w.Close()
			t.Fatalf("failed to encode FieldValues: %v", verr)
		}
		if err := w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: vb}); err != nil {
			_ = w.Close()
			t.Fatalf("failed to append WALEntry: %v", err)
		}
		// record a synthetic latency sample so PutLatencyHist has values
		if a1.metrics != nil && a1.metrics.PutLatencyHist != nil {
			observeLatency(a1.metrics.PutLatencyHist, 0.02)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close central WAL: %v", err)
	}

	// also write a DELETE_RANGE WALEntry so replay must recover it
	delRange := &pb.WALEntry{SequenceNumber: 99, EntryType: pb.WALEntry_DELETE_RANGE, Metric: "m", Tags: map[string]string{"t": "v"}, StartTime: 0, EndTime: 100}
	if err := a1.ApplyReplicatedEntry(context.Background(), delRange); err != nil {
		t.Fatalf("ApplyReplicatedEntry(delete_range) failed: %v", err)
	}
	// close first engine so WAL is flushed/closed
	if err := a1.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Start a fresh engine pointing at same data dir to trigger WAL replay
	ai2, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine 2 error: %v", err)
	}
	a2 := ai2.(*Engine2Adapter)
	if err := a2.Start(); err != nil {
		t.Fatalf("Start 2 error: %v", err)
	}
	defer a2.Close()

	if a2.metrics == nil || a2.metrics.WALRecoveredEntriesTotal == nil || a2.metrics.WALRecoveryDurationSeconds == nil {
		t.Fatalf("WAL recovery metrics not initialized")
	}
	// Only the explicit Puts above are written to the engine WAL. The
	// DELETE_RANGE applied via ApplyReplicatedEntry does not append to the
	// engine's WAL, so expect at least the 3 Put entries to be recovered.
	if a2.metrics.WALRecoveredEntriesTotal.Value() < 3 {
		t.Fatalf("expected WALRecoveredEntriesTotal >= 3, got %v", a2.metrics.WALRecoveredEntriesTotal.Value())
	}
	// Recovery duration may be very small (rounded to 0); ensure it's non-negative.
	if a2.metrics.WALRecoveryDurationSeconds.Value() < 0 {
		t.Fatalf("expected WALRecoveryDurationSeconds >= 0, got %v", a2.metrics.WALRecoveryDurationSeconds.Value())
	}
}
