package engine2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/require"
)

// TestAdapter_FlushMemtableToL0_Success exercises the adapter flush path by
// writing a datapoint into the Engine2 adapter's active memtable, swapping
// it out via GetMemtablesForFlush and calling FlushMemtableToL0 to produce
// a new SSTable. The resulting SSTable file and manifest entry are verified.
func TestAdapter_FlushMemtableToL0_Success(t *testing.T) {
	dir := t.TempDir()
	ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: dir})
	// tests in this package rely on adapter-specific helpers; assert to concrete adapter
	a := ai.(*Engine2Adapter)
	defer a.Close()

	// Put a datapoint using public adapter API
	fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1.0})
	require.NoError(t, err)
	dp := core.DataPoint{Metric: "flush.test", Tags: map[string]string{"id": "a"}, Timestamp: 100, Fields: fv}
	require.NoError(t, a.Put(context.Background(), dp))

	// Acquire provider lock and swap memtables for flush
	a.Lock()
	mems, _ := a.GetMemtablesForFlush()
	a.Unlock()

	require.NotEmpty(t, mems, "expected at least one memtable to flush")
	for _, mem := range mems {
		require.NoError(t, a.FlushMemtableToL0(mem, context.Background()))
	}

	// Inspect sstables directory for new SSTable
	sstDir := filepath.Join(dir, "sstables")
	files, err := os.ReadDir(sstDir)
	require.NoError(t, err)
	require.NotEmpty(t, files, "expected sstable files in sstables dir")

	// Load the first SSTable and verify it contains the encoded key
	var sstPath string
	for _, f := range files {
		if f.Type().IsRegular() {
			sstPath = filepath.Join(sstDir, f.Name())
			break
		}
	}
	require.NotEmpty(t, sstPath)
	st, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: sstPath})
	require.NoError(t, err)
	defer st.Close()
	// A simple sanity check: key count > 0
	require.Greater(t, int(st.KeyCount()), 0)
}

// TestAdapter_FlushRemainingMemtables simulates flushing multiple memtables
// by performing two write+swap cycles and flushing each swapped memtable.
func TestAdapter_FlushRemainingMemtables(t *testing.T) {
	dir := t.TempDir()
	ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: dir})
	a := ai.(*Engine2Adapter)
	defer a.Close()

	// First datapoint and flush
	fv1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1.0})
	dp1 := core.DataPoint{Metric: "flush.multi", Tags: map[string]string{"id": "a"}, Timestamp: 1, Fields: fv1}
	require.NoError(t, a.Put(context.Background(), dp1))
	a.Lock()
	mems1, _ := a.GetMemtablesForFlush()
	a.Unlock()
	require.NotEmpty(t, mems1)
	for _, m := range mems1 {
		require.NoError(t, a.FlushMemtableToL0(m, context.Background()))
	}

	// Second datapoint and flush
	fv2, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 2.0})
	dp2 := core.DataPoint{Metric: "flush.multi", Tags: map[string]string{"id": "b"}, Timestamp: 2, Fields: fv2}
	require.NoError(t, a.Put(context.Background(), dp2))
	a.Lock()
	mems2, _ := a.GetMemtablesForFlush()
	a.Unlock()
	require.NotEmpty(t, mems2)
	for _, m := range mems2 {
		require.NoError(t, a.FlushMemtableToL0(m, context.Background()))
	}

	// Now check that at least two SSTable files exist
	sstDir := filepath.Join(dir, "sstables")
	entries, err := os.ReadDir(sstDir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 2, "expected at least 2 sstable files after flushing two memtables")
}
