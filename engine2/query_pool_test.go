package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// TestPooledQueryResultItemClearedOnPut ensures that when a pooled
// *core.QueryResultItem is returned via Put, its maps and fields are
// cleared so callers cannot accidentally retain data that will be
// reused by the pool. This test guards the ownership contract that
// callers must copy maps/fields if they intend to retain them after
// calling Put(item).
func TestPooledQueryResultItemClearedOnPut(t *testing.T) {
	// obtain a pooled item
	item := queryResultItemPool.Get().(*core.QueryResultItem)

	// initialize maps and fields
	item.Tags = map[string]string{"host": "server-01", "dc": "us-east"}
	item.AggregatedValues = map[string]float64{"avg": 42.0}

	fv := core.GetPooledFieldValues()
	pv, err := core.NewPointValue(3.14)
	if err != nil {
		t.Fatalf("failed to create point value: %v", err)
	}
	fv["value"] = pv
	item.Fields = fv

	// Call Put which should clear maps/fields and return item to pool
	it := &engine2QueryResultIterator{}
	it.Put(item)

	// After Put, Fields must be nil (Put sets item.Fields = nil)
	if item.Fields != nil {
		t.Fatalf("expected item.Fields to be nil after Put, got: %v", item.Fields)
	}

	// Tags map should be cleared (keys deleted)
	if len(item.Tags) != 0 {
		t.Fatalf("expected item.Tags to be empty after Put, got: %v", item.Tags)
	}

	// AggregatedValues should be nil
	if item.AggregatedValues != nil {
		t.Fatalf("expected item.AggregatedValues to be nil after Put, got: %v", item.AggregatedValues)
	}
}
