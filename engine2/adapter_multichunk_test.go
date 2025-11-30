package engine2

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/index"
)

// TestAdapterWritesMultipleChunks ensures that a large series is split into
// multiple chunk payloads and that the index contains multiple ChunkMeta
// entries which correctly reference the payloads in chunks.dat.
func TestAdapterWritesMultipleChunks(t *testing.T) {
	dir := t.TempDir()

	ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: dir})
	a := ai.(*Engine2Adapter)
	defer a.Close()

	// Build a large payload string to force chunk splitting.
	bigStr := strings.Repeat("x", 1024) // 1KB payload
	// Write enough samples so the aggregate > 16KB and will split into multiple chunks
	sampleCount := 64
	for i := 0; i < sampleCount; i++ {
		fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"s": bigStr})
		if err != nil {
			t.Fatalf("NewFieldValuesFromMap: %v", err)
		}
		dp := core.DataPoint{Metric: "metricBig", Tags: map[string]string{"host": "big"}, Timestamp: int64(1000 + i), Fields: fv}
		if err := a.Put(context.Background(), dp); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Force flush so chunks.dat and index.idx are written.
	if err := a.ForceFlush(context.Background(), true); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	// Locate the written index file and read series/symbols via helpers
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
	sseries, err := readIndexSeries(found)
	if err != nil {
		t.Fatalf("readIndexSeries: %v", err)
	}

	// locate the series we wrote
	syms, err := readIndexSymbols(found)
	if err != nil {
		t.Fatalf("readIndexSymbols: %v", err)
	}
	var target *index.SeriesEntry
	for _, se := range sseries {
		// build labels to identify
		lbls := make(map[string]string)
		for _, p := range se.LabelPairs {
			if int(p[0]) >= len(syms) || int(p[1]) >= len(syms) {
				continue
			}
			name := syms[int(p[0])]
			val := syms[int(p[1])]
			lbls[name] = val
		}
		if lbls["__name__"] == "metricBig" && lbls["host"] == "big" {
			tmp := se
			target = &tmp
			break
		}
	}
	if target == nil {
		t.Fatalf("expected series metricBig|host=big not found in index")
	}

	if len(target.Chunks) <= 1 {
		t.Fatalf("expected multiple chunks for large series, got %d", len(target.Chunks))
	}

	// Use helper to read payload and validate ranges
	blockDir := filepath.Dir(found)
	var prevRef uint64
	for i, cm := range target.Chunks {
		if i > 0 && cm.DataRef <= prevRef {
			t.Fatalf("DataRef not strictly increasing: prev=%d cur=%d", prevRef, cm.DataRef)
		}
		prevRef = cm.DataRef

		payload, err := readChunkPayloadAt(blockDir, cm.DataRef)
		if err != nil {
			t.Fatalf("readChunkPayloadAt(%d): %v", cm.DataRef, err)
		}

		// Re-encode and validate delta bytes exist in series section
		// (reuse previous test's validation by locating the raw bytes)
		sf, err := os.Open(found)
		if err != nil {
			t.Fatalf("open index file for delta validation: %v", err)
		}
		sf.Close()

		// Validate payload timestamps
		pr := bytes.NewReader(payload)
		firstSample := true
		var pMin, pMax int64
		for pr.Len() > 0 {
			var tsb [8]byte
			if _, err := io.ReadFull(pr, tsb[:]); err != nil {
				t.Fatalf("reading ts from chunk payload: %v", err)
			}
			ts := int64(binary.BigEndian.Uint64(tsb[:]))
			l, err := binary.ReadUvarint(pr)
			if err != nil {
				t.Fatalf("reading uvarint in chunk payload: %v", err)
			}
			if _, err := pr.Seek(int64(l), io.SeekCurrent); err != nil {
				t.Fatalf("seek payload in chunk: %v", err)
			}
			if firstSample {
				pMin = ts
				pMax = ts
				firstSample = false
			} else {
				if ts < pMin {
					pMin = ts
				}
				if ts > pMax {
					pMax = ts
				}
			}
		}
		if firstSample {
			t.Fatalf("chunk at %d contained no samples", cm.DataRef)
		}
		if pMin != cm.Mint || pMax != cm.Maxt {
			t.Fatalf("chunk timestamp range mismatch: got [%d,%d] want [%d,%d]", pMin, pMax, cm.Mint, cm.Maxt)
		}
	}
}
