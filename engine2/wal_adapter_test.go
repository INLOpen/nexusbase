package engine2

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/require"
)

func TestLeaderWAL_OpenAndPutRotate(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	// Create adapter via shared helper and obtain underlying engine
	ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: tmp})
	a := ai.(*Engine2Adapter)
	e := a.Engine2
	defer func() { _ = e.wal.Close(); _ = os.RemoveAll(tmp) }()

	w := a.GetWAL()
	require.NotNil(t, w)

	// Put a datapoint
	fv, ferr := core.NewFieldValuesFromMap(map[string]any{"v": 1})
	require.NoError(t, ferr)
	dp := core.DataPoint{Metric: "m1", Tags: map[string]string{"h": "A"}, Timestamp: time.Now().UnixNano(), Fields: fv}
	// Note: use adapter Put which writes to engine2 wal and to leader WAL
	require.NoError(t, a.Put(context.Background(), dp))

	// Rotate leader WAL to ensure entries are flushed to a segment
	require.NoError(t, w.Rotate())

	// Active segment index should be >= 1
	idx := w.ActiveSegmentIndex()
	require.GreaterOrEqual(t, idx, uint64(1))
}
