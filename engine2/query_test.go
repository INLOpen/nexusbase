package engine2

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

func TestQueryAndDeletes(t *testing.T) {
	dir := t.TempDir()
	a := setupStorageEngineStart(t, StorageEngineOptions{DataDir: dir})
	defer a.Close()
	ad := a

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// insert points
	for i := int64(0); i < 5; i++ {
		fv, _ := core.NewFieldValuesFromMap(map[string]interface{}{"v": i})
		dp := core.DataPoint{Metric: "m", Tags: map[string]string{"k": "v"}, Timestamp: 1000 + i, Fields: fv}
		if err := ad.Put(ctx, dp); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query time range
	qp := core.QueryParams{Metric: "m", StartTime: 1000, EndTime: 1004, Tags: map[string]string{"k": "v"}}
	it, err := ad.Query(ctx, qp)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	count := 0
	for it.Next() {
		itm, err := it.At()
		if err != nil {
			t.Fatalf("iterator At failed: %v", err)
		}
		if itm.Timestamp < 1000 || itm.Timestamp > 1004 {
			t.Fatalf("unexpected timestamp %d", itm.Timestamp)
		}
		count++
	}
	if count != 5 {
		t.Fatalf("expected 5 results, got %d", count)
	}

	// delete a point
	if err := ad.Delete(ctx, "m", map[string]string{"k": "v"}, 1002); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	// query again
	it2, _ := ad.Query(ctx, qp)
	cnt2 := 0
	for it2.Next() {
		itm, _ := it2.At()
		if itm.Timestamp == 1002 {
			t.Fatalf("deleted timestamp returned")
		}
		cnt2++
	}
	if cnt2 != 4 {
		t.Fatalf("expected 4 results after delete, got %d", cnt2)
	}

	// delete series
	if err := ad.DeleteSeries(ctx, "m", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("DeleteSeries failed: %v", err)
	}
	it3, _ := ad.Query(ctx, qp)
	if it3.Next() {
		t.Fatalf("expected no results after DeleteSeries")
	}
}
