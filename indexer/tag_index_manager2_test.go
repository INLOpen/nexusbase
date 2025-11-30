package indexer

import (
	"sync"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

func TestTagIndexManager2_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:           tempDir,
		MemtableThreshold: 100,
	}

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

	tim1, err := NewTagIndexManager2(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager2 (1): %v", err)
	}

	addData := func(seriesID uint64, tags map[string]string) {
		if len(tags) > 0 {
			list := make([]string, 0, len(tags)*2)
			for k, v := range tags {
				list = append(list, k, v)
			}
			if ss, ok := deps.StringStore.(*StringStore); ok {
				_, _ = ss.AddStringsBatch(list)
			} else {
				for _, s := range list {
					_, _ = deps.StringStore.GetOrCreateID(s)
				}
			}
		}
		tim1.Add(seriesID, tags)
	}

	addData(1, map[string]string{"host": "serverA"})
	addData(2, map[string]string{"host": "serverB"})
	addData(3, map[string]string{"region": "us-east"})
	addData(1, map[string]string{"region": "us-east"})

	// Stop will flush
	tim1.Stop()

	tim2, err := NewTagIndexManager2(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager2 (2): %v", err)
	}
	defer tim2.Stop()

	if tim2.levelsManager.GetTotalTableCount() == 0 {
		t.Fatal("Expected index SSTables to be loaded from manifest, but none were found.")
	}

	bmA, err := tim2.Query(map[string]string{"host": "serverA"})
	if err != nil {
		t.Fatalf("Query for host=serverA failed: %v", err)
	}
	expectedA := roaring64.BitmapOf(1)
	if !bmA.Equals(expectedA) {
		t.Errorf("Bitmap for host=serverA mismatch. Got %v, want %v", bmA, expectedA)
	}

	bmRegion, err := tim2.Query(map[string]string{"region": "us-east"})
	if err != nil {
		t.Fatalf("Query for region=us-east failed: %v", err)
	}
	expectedRegion := roaring64.BitmapOf(1, 3)
	if !bmRegion.Equals(expectedRegion) {
		t.Errorf("Bitmap for region=us-east mismatch. Got %v, want %v", bmRegion, expectedRegion)
	}
}

func TestTagIndexManager2_AddEncoded(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:           tempDir,
		MemtableThreshold: 100,
	}

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
		t.Fatalf("Failed to create TagIndexManager2: %v", err)
	}
	defer tim.Stop()

	list := []string{"host", "serverA", "region", "us-east"}
	var encodedTags1 []core.EncodedSeriesTagPair
	if ss, ok := deps.StringStore.(*StringStore); ok {
		ids, _ := ss.AddStringsBatch(list)
		encodedTags1 = []core.EncodedSeriesTagPair{
			{KeyID: ids[0], ValueID: ids[1]},
			{KeyID: ids[2], ValueID: ids[3]},
		}
	} else {
		hostID, _ := deps.StringStore.GetOrCreateID("host")
		serverAID, _ := deps.StringStore.GetOrCreateID("serverA")
		regionID, _ := deps.StringStore.GetOrCreateID("region")
		usEastID, _ := deps.StringStore.GetOrCreateID("us-east")
		encodedTags1 = []core.EncodedSeriesTagPair{{KeyID: hostID, ValueID: serverAID}, {KeyID: regionID, ValueID: usEastID}}
	}

	seriesID1 := uint64(1)
	if err := tim.AddEncoded(seriesID1, encodedTags1); err != nil {
		t.Fatalf("AddEncoded failed: %v", err)
	}

	bm, err := tim.Query(map[string]string{"host": "serverA", "region": "us-east"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	expectedBitmap := roaring64.BitmapOf(seriesID1)
	if !bm.Equals(expectedBitmap) {
		t.Errorf("Bitmap mismatch after AddEncoded. Got %v, want %v", bm, expectedBitmap)
	}
}

func TestTagIndexManager2_Compaction(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:                   tempDir,
		MemtableThreshold:         2,
		MaxL0Files:                2,
		CompactionIntervalSeconds: 3600,
	}

	nextId := nextIDBuilder()
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.SeriesIDStore.LoadFromFile(tempDir)
	deps.StringStore.LoadFromFile(tempDir)

	tim, err := NewTagIndexManager2(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager2: %v", err)
	}
	defer tim.Stop()

	series1Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "1"}))
	series2Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "2"}))
	series3Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "3"}))
	deps.SeriesIDStore.GetOrCreateID(series1Key)
	deps.SeriesIDStore.GetOrCreateID(series2Key)
	deps.SeriesIDStore.GetOrCreateID(series3Key)

	addData := func(seriesID uint64, tags map[string]string) {
		if len(tags) > 0 {
			list := make([]string, 0, len(tags)*2)
			for k, v := range tags {
				list = append(list, k, v)
			}
			if ss, ok := deps.StringStore.(*StringStore); ok {
				_, _ = ss.AddStringsBatch(list)
			} else {
				for _, s := range list {
					_, _ = deps.StringStore.GetOrCreateID(s)
				}
			}
		}
		tim.Add(seriesID, tags)
	}

	// File 1
	addData(1, map[string]string{"host": "serverA"})
	addData(2, map[string]string{"region": "us-east"})
	tim.processImmutableIndexMemtables()

	// File 2
	addData(1, map[string]string{"region": "us-east"})
	addData(3, map[string]string{"host": "serverB"})
	tim.processImmutableIndexMemtables()

	if tim.levelsManager.GetTotalTableCount() != 2 {
		t.Fatalf("Expected 2 L0 tables before compaction, got %d", tim.levelsManager.GetTotalTableCount())
	}

	if err := tim.compactIndexL0ToL1(); err != nil {
		t.Fatalf("compactIndexL0ToL1 failed: %v", err)
	}

	if count := len(tim.levelsManager.GetTablesForLevel(0)); count != 0 {
		t.Errorf("Expected L0 to be empty after compaction, got %d tables", count)
	}
	l1Tables := tim.levelsManager.GetTablesForLevel(1)
	if len(l1Tables) != 1 {
		t.Fatalf("Expected 1 table in L1 after compaction, got %d", len(l1Tables))
	}

	bmRegion, err := tim.Query(map[string]string{"region": "us-east"})
	if err != nil {
		t.Fatalf("Query for region=us-east failed after compaction: %v", err)
	}
	expectedRegion := roaring64.BitmapOf(1, 2)
	if !bmRegion.Equals(expectedRegion) {
		t.Errorf("Bitmap for region=us-east mismatch after compaction. Got %v, want %v", bmRegion, expectedRegion)
	}
}
