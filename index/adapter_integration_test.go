package index

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

func nextIDBuilder() core.SSTableNextIDFactory {
	var mu sync.Mutex
	var v uint64
	return func() uint64 {
		mu.Lock()
		v++
		nv := v
		mu.Unlock()
		return nv
	}
}

func TestAdapterIntegration_RegisterAndQuery(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := indexer.TagIndexManagerOptions{DataDir: tempDir, MemtableThreshold: 1000}
	nextId := nextIDBuilder()

	deps := &indexer.TagIndexDependencies{
		StringStore:     indexer.NewStringStore(logger, nil),
		SeriesIDStore:   indexer.NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.StringStore.LoadFromFile(tempDir)
	deps.SeriesIDStore.LoadFromFile(tempDir)

	tim, err := indexer.NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("NewTagIndexManager failed: %v", err)
	}
	defer tim.Stop()

	// Prepare string IDs for tag key/value
	keyID, _ := deps.StringStore.GetOrCreateID("host")
	valID, _ := deps.StringStore.GetOrCreateID("serverA")

	// Build bitmap with series IDs
	bm := roaring64.New()
	bm.Add(1)
	bm.Add(3)

	mapping := map[string]*roaring64.Bitmap{
		string(indexer.EncodeTagIndexKey(keyID, valID)): bm,
	}

	// Adapter config with writer factory
	factory := func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
		return sstable.NewSSTableWriter(opts)
	}
	cfg := AdapterConfig{SSTableWriterFactory: factory, DataDir: tempDir}

	// Use the next ID from deps
	id := deps.SSTNextID()
	path, _, err := WriteTagIndexSSTable(cfg, mapping, id)
	if err != nil {
		t.Fatalf("WriteTagIndexSSTable failed: %v", err)
	}

	// Load SSTable and register into levels manager as L0 table
	sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: path, ID: id})
	if err != nil {
		t.Fatalf("LoadSSTable failed: %v", err)
	}
	if err := tim.GetLevelsManager().AddL0Table(sst); err != nil {
		t.Fatalf("AddL0Table failed: %v", err)
	}

	// Query via TagIndexManager
	res, err := tim.Query(map[string]string{"host": "serverA"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !res.Equals(bm) {
		t.Fatalf("bitmap mismatch: got %v want %v", res.String(), bm.String())
	}
}
