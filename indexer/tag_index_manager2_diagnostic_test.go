package indexer

import (
	"bytes"
	"sync"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

// TestFlushAndLoad_PreservesBitmapBytes writes a single key/value via
// TagIndexManager2.flushIndexMemtableToSSTable and verifies that the loaded
// SSTable returns the exact serialized bytes that were written.
func TestFlushAndLoad_PreservesBitmapBytes(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{DataDir: tempDir, MemtableThreshold: 1000}
	nextId := nextIDBuilder()
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.StringStore.LoadFromFile(tempDir)
	deps.SeriesIDStore.LoadFromFile(tempDir)

	tim, err := NewTagIndexManager2(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer tim.Stop()

	// Prepare key/value and bitmap
	keyID := mustGetStringID(deps.StringStore, "diag_key")
	valID := mustGetStringID(deps.StringStore, "diag_val")
	sid := uint64(42)
	mem := NewIndexMemtable()
	mem.Add(keyID, valID, sid)

	// Capture serialized bytes before flush
	bm := roaring64.New()
	bm.Add(sid)
	expectedBytes, err := bm.ToBytes()
	if err != nil {
		t.Fatalf("failed to serialize bitmap: %v", err)
	}

	sst, err := tim.flushIndexMemtableToSSTable(mem)
	if err != nil || sst == nil {
		t.Fatalf("flushIndexMemtableToSSTable failed: %v", err)
	}

	// Load via returned SSTable and Get the key
	key := EncodeTagIndexKey(keyID, valID)
	gotBytes, entryType, err := sst.Get(key)
	if err != nil {
		t.Fatalf("sst.Get failed: %v", err)
	}
	if entryType != core.EntryTypePutEvent {
		t.Fatalf("unexpected entry type: %v", entryType)
	}

	if !bytes.Equal(gotBytes, expectedBytes) {
		t.Fatalf("serialized bytes mismatch: expected %d bytes, got %d bytes", len(expectedBytes), len(gotBytes))
	}

	// As an extra check, ensure we can decode the returned bytes into a bitmap
	dec := roaring64.New()
	if _, err := dec.ReadFrom(bytes.NewReader(gotBytes)); err != nil {
		t.Fatalf("failed to decode returned bitmap: %v", err)
	}
	if !dec.Equals(bm) {
		t.Fatalf("bitmap content mismatch after decode")
	}
}
