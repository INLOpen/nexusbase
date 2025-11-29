package engine2

import (
	"context"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestPutGetReplayForceFlush(t *testing.T) {
	dir := t.TempDir()

	ai, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	adapter := ai.(*Engine2Adapter)
	if errStart := adapter.Start(); errStart != nil {
		t.Fatalf("Start failed: %v", errStart)
	}
	defer adapter.Close()

	fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": 42})

	dp := core.DataPoint{Metric: "m1", Tags: map[string]string{"k": "v"}, Timestamp: 1000, Fields: fv}

	if err := adapter.Put(context.Background(), dp); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := adapter.Get(context.Background(), "m1", map[string]string{"k": "v"}, 1000)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil || len(got) == 0 {
		t.Fatalf("Get returned empty fields")
	}

	// close first engine to release WAL file handle
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// recreate engine to ensure WAL replay works
	ai2, err := NewStorageEngine(StorageEngineOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("NewStorageEngine reload failed: %v", err)
	}
	adapter2 := ai2.(*Engine2Adapter)
	if errStart := adapter2.Start(); errStart != nil {
		t.Fatalf("Start failed: %v", errStart)
	}
	defer adapter2.Close()

	got2, err := adapter2.Get(context.Background(), "m1", map[string]string{"k": "v"}, 1000)
	if err != nil {
		t.Fatalf("Get after replay failed: %v", err)
	}
	if got2 == nil || len(got2) == 0 {
		t.Fatalf("Get after replay returned empty fields")
	}

	// ForceFlush clears memtable
	if err := adapter2.ForceFlush(context.Background(), true); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}
	if _, err := adapter2.Get(context.Background(), "m1", map[string]string{"k": "v"}, 1000); err == nil {
		t.Fatalf("expected not found after ForceFlush, got nil error")
	}
}
