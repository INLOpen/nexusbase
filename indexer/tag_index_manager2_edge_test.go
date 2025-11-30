package indexer

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

// TestTagIndexManager2_Concurrency stresses concurrent Add() calls and ensures
// all entries are visible after flushing.
func TestTagIndexManager2_Concurrency(t *testing.T) {
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

	// Prepare some common keys in string store
	_, _ = deps.StringStore.GetOrCreateID("concurrent")

	const goroutines = 50
	const perG = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				sid := uint64(1 + g*perG + i)
				// unique value per series ensures unique postings
				val := fmt.Sprintf("v-%d-%d", g, i)
				_, _ = deps.StringStore.GetOrCreateID(val)
				tim.Add(sid, map[string]string{"concurrent": val})
			}
		}(g)
	}
	wg.Wait()

	// Force flush all in-memory memtables to SSTables and persist
	tim.flushFinalMemtable()

	// Now query and ensure all series are present for their values
	total := goroutines * perG
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			sid := uint64(1 + g*perG + i)
			val := fmt.Sprintf("v-%d-%d", g, i)
			bm, err := tim.Query(map[string]string{"concurrent": val})
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if !bm.Equals(roaring64.BitmapOf(sid)) {
				t.Fatalf("expected bitmap for %s to contain %d, got %v", val, sid, bm)
			}
		}
	}

	// Sanity: ensure we added expected number of distinct series
	// Build a union of a few queries to ensure sets are not lost
	union := roaring64.New()
	for g := 0; g < 5; g++ {
		val := fmt.Sprintf("v-%d-0", g)
		b, _ := tim.Query(map[string]string{"concurrent": val})
		union.Or(b)
	}
	if union.GetCardinality() == 0 {
		t.Fatalf("unexpected empty union result, expected some series")
	}
	_ = total
}

// TestTagIndexManager2_DeletionHandling ensures deleted series are removed
// during compaction/merge.
func TestTagIndexManager2_DeletionHandling(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{DataDir: tempDir, MemtableThreshold: 100}
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

	// Prepare IDs
	aID, _ := deps.SeriesIDStore.GetOrCreateID("seriesA")
	bID, _ := deps.SeriesIDStore.GetOrCreateID("seriesB")
	cID, _ := deps.SeriesIDStore.GetOrCreateID("seriesC")
	_, _ = deps.StringStore.GetOrCreateID("region")
	_, _ = deps.StringStore.GetOrCreateID("us")

	// mark B deleted
	deps.DeletedSeriesMu.Lock()
	deps.DeletedSeries["seriesB"] = 1
	deps.DeletedSeriesMu.Unlock()

	tim, err := NewTagIndexManager2(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer tim.Stop()

	// Create an L0 file containing A,B,C for region=us
	mem := NewIndexMemtable()
	mem.Add(mustGetStringID(deps.StringStore, "region"), mustGetStringID(deps.StringStore, "us"), aID)
	mem.Add(mustGetStringID(deps.StringStore, "region"), mustGetStringID(deps.StringStore, "us"), bID)
	mem.Add(mustGetStringID(deps.StringStore, "region"), mustGetStringID(deps.StringStore, "us"), cID)
	sst, err := tim.flushIndexMemtableToSSTable(mem)
	if err != nil || sst == nil {
		t.Fatalf("failed to flush mem: %v", err)
	}
	tim.levelsManager.AddL0Table(sst)

	// Perform compaction to L1 which should filter deleted B
	if err := tim.compactIndexL0ToL1(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Inspect L1 table for key
	l1 := tim.levelsManager.GetTablesForLevel(1)
	if len(l1) != 1 {
		t.Fatalf("expected 1 L1 table, got %d", len(l1))
	}
	key := encodeTagIndexKey(mustGetStringID(deps.StringStore, "region"), mustGetStringID(deps.StringStore, "us"))
	valBytes, _, err := l1[0].Get(key)
	if err != nil {
		t.Fatalf("failed to get key from L1: %v", err)
	}
	bm := roaring64.New()
	if _, err := bm.ReadFrom(bytes.NewReader(valBytes)); err != nil {
		t.Fatalf("failed to decode bitmap: %v", err)
	}
	expected := roaring64.BitmapOf(aID, cID)
	if !bm.Equals(expected) {
		t.Fatalf("expected bitmap %v after deletion filtering, got %v", expected, bm)
	}
}

// TestTagIndexManager2_LargeScaleMerge creates many small L0 tables that
// overlap and ensures the merged L1 contains the union of series IDs.
func TestTagIndexManager2_LargeScaleMerge(t *testing.T) {
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

	// Prepare the key
	keyID := mustGetStringID(deps.StringStore, "k_large")
	valID := mustGetStringID(deps.StringStore, "v_large")

	// Create many small L0 tables, each containing a single series for the same key.
	// Ensure the SeriesIDStore has mappings for those series so compaction
	// doesn't drop them when looking up keys during cleaning.
	const tables = 30
	seriesIDs := make([]uint64, 0, tables)
	for i := 1; i <= tables; i++ {
		sid, err := deps.SeriesIDStore.GetOrCreateID(fmt.Sprintf("s-%d", i))
		if err != nil {
			t.Fatalf("failed to create series id: %v", err)
		}
		seriesIDs = append(seriesIDs, sid)
	}

	for _, sid := range seriesIDs {
		mem := NewIndexMemtable()
		mem.Add(keyID, valID, sid)
		sst, err := tim.flushIndexMemtableToSSTable(mem)
		if err != nil || sst == nil {
			t.Fatalf("failed to flush small mem: %v", err)
		}
		tim.levelsManager.AddL0Table(sst)
	}

	// Merge L0 -> L1
	if err := tim.compactIndexL0ToL1(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Expect a single L1 table and union of series 1..tables
	l1 := tim.levelsManager.GetTablesForLevel(1)
	if len(l1) != 1 {
		t.Fatalf("expected 1 L1 table after merge, got %d", len(l1))
	}
	key := EncodeTagIndexKey(keyID, valID)
	valBytes, _, err := l1[0].Get(key)
	if err != nil {
		t.Fatalf("failed to get key from merged L1: %v", err)
	}
	bm := roaring64.New()
	if _, err := bm.ReadFrom(bytes.NewReader(valBytes)); err != nil {
		t.Fatalf("failed to decode merged bitmap: %v", err)
	}
	expected := roaring64.New()
	for i := 1; i <= tables; i++ {
		expected.Add(uint64(i))
	}
	if !bm.Equals(expected) {
		t.Fatalf("merged bitmap mismatch: want %d entries, got %d", expected.GetCardinality(), bm.GetCardinality())
	}
}
