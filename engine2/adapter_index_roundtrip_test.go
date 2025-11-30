package engine2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/index"
)

func init() {
	// slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

// TestAdapterFlushWritesReadableIndex exercises the adapter flush path to
// ensure it writes an `index.idx` that the `index` package can read.
func TestAdapterFlushWritesReadableIndex(t *testing.T) {
	dir := t.TempDir()

	// create engine2 adapter via compatibility helper
	ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: dir})
	a := ai.(*Engine2Adapter)
	defer a.Close()

	// Put a few data points with non-empty FieldValues so chunk payloads are written
	basePts := []struct {
		metric string
		tags   map[string]string
		ts     int64
	}{
		{"metricA", map[string]string{"host": "a"}, 100},
		{"metricA", map[string]string{"host": "b"}, 200},
		{"metricB", map[string]string{"region": "us"}, 300},
	}
	for _, bp := range basePts {
		fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1})
		if err != nil {
			t.Fatalf("NewFieldValuesFromMap: %v", err)
		}
		p := core.DataPoint{Metric: bp.metric, Tags: bp.tags, Timestamp: bp.ts, Fields: fv}
		if err := a.Put(context.Background(), p); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Force a flush which should create an SSTable and write a populated index
	if err := a.ForceFlush(context.Background(), true); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	// Locate the written index file
	found, err := findIndexFile(dir)
	if err != nil {
		t.Fatalf("findIndexFile: %v", err)
	}

	f, err := os.Open(found)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer f.Close()

	if err := index.ValidateHeader(f); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}

	syms, err := readIndexSymbols(found)
	if err != nil {
		t.Fatalf("readIndexSymbols: %v", err)
	}
	if len(syms) == 0 {
		t.Fatalf("empty symbol table")
	}

	// Assert symbol table is lexicographically sorted (deterministic on-disk ordering)
	for i := 0; i < len(syms)-1; i++ {
		if syms[i] > syms[i+1] {
			t.Fatalf("symbol table not sorted: %q > %q", syms[i], syms[i+1])
		}
	}

	// Build an index map for exact symbol -> index assertions
	idxMap := make(map[string]int, len(syms))
	for i, s := range syms {
		// If duplicate symbols exist this will record the last index, but the
		// on-disk writer ensures unique symbol table entries.
		idxMap[s] = i
	}
	sseries, err := readIndexSeries(found)
	if err != nil {
		t.Fatalf("readIndexSeries: %v", err)
	}
	if len(sseries) == 0 {
		t.Fatalf("no series entries in index")
	}

	// Verify expected series label sets exist in the written index.
	expected := []map[string]string{
		{"__name__": "metricA", "host": "a"},
		{"__name__": "metricA", "host": "b"},
		{"__name__": "metricB", "region": "us"},
	}
	foundSeries := make([]bool, len(expected))

	for _, se := range sseries {
		// build labels map from label pair indices
		labels := make(map[string]string)
		for _, p := range se.LabelPairs {
			if int(p[0]) >= len(syms) || int(p[1]) >= len(syms) {
				continue
			}
			name := syms[int(p[0])]
			val := syms[int(p[1])]
			labels[name] = val
		}
		// Also assert label-pair indices point to the expected entries in the symbol table
		for _, p := range se.LabelPairs {
			if int(p[0]) >= len(syms) || int(p[1]) >= len(syms) {
				t.Fatalf("label pair index out of range: %+v", p)
			}
			name := syms[int(p[0])]
			val := syms[int(p[1])]
			if idxMap[name] != int(p[0]) {
				t.Fatalf("label name index mismatch: symbol %q has index %d but labelpair contains %d", name, idxMap[name], p[0])
			}
			if idxMap[val] != int(p[1]) {
				t.Fatalf("label value index mismatch: symbol %q has index %d but labelpair contains %d", val, idxMap[val], p[1])
			}
		}
		// check against expected sets
		for i, exp := range expected {
			if foundSeries[i] {
				continue
			}
			ok := true
			for k, v := range exp {
				if labels[k] != v {
					ok = false
					break
				}
			}
			if ok {
				foundSeries[i] = true
			}
		}
	}
	for i, ok := range foundSeries {
		if !ok {
			t.Fatalf("expected series %v not found in index", expected[i])
		}
	}

	// Verify chunks.dat exists and chunk payloads match ChunkMeta ranges using helper
	blockDir := filepath.Dir(found)
	for _, se := range sseries {
		for _, cm := range se.Chunks {
			payload, err := readChunkPayloadAt(blockDir, cm.DataRef)
			if err != nil {
				t.Fatalf("readChunkPayloadAt(%d): %v", cm.DataRef, err)
			}
			pMin, pMax, err := payloadTimestampRange(payload)
			if err != nil {
				t.Fatalf("payloadTimestampRange: %v", err)
			}
			if pMin != cm.Mint || pMax != cm.Maxt {
				t.Fatalf("chunk timestamp range mismatch: got [%d,%d] want [%d,%d]", pMin, pMax, cm.Mint, cm.Maxt)
			}
		}
	}
}
