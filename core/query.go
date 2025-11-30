package core

import "github.com/INLOpen/nexuscore/types"

type AggregationFunc string

const (
	AggAvg   AggregationFunc = "avg"
	AggSum   AggregationFunc = "sum"
	AggMin   AggregationFunc = "min"
	AggMax   AggregationFunc = "max"
	AggCount AggregationFunc = "count"
	AggFirst AggregationFunc = "first"
	AggLast  AggregationFunc = "last"
)

func (agg AggregationFunc) String() string {
	return string(agg)
}

// AggregationSpec defines a single aggregation operation, like avg(latency).
type AggregationSpec struct {
	Function AggregationFunc
	Field    string // Field can be "*" for functions like count(*).
	Alias    string // Optional alias for the result column
}

// QueryParams defines the parameters for a query.
type QueryParams struct {
	Metric             string
	StartTime          int64
	EndTime            int64
	Tags               map[string]string
	AggregationSpecs   []AggregationSpec
	DownsampleInterval string
	IsRelative         bool   // New: Flag to indicate a relative time query
	RelativeDuration   string // New: Stores the duration string, e.g., "1h"
	Order              types.SortOrder
	Limit              int64
	EmitEmptyWindows   bool
	AfterKey           []byte // For cursor-based pagination
}

// QueryResultItem is a struct that holds a single result from a query.
type QueryResultItem struct {
	Metric           string
	Tags             map[string]string
	Timestamp        int64
	Fields           FieldValues
	IsAggregated     bool
	WindowStartTime  int64
	WindowEndTime    int64
	IsEvent          bool
	AggregatedValues map[string]float64
}

// QueryIterator defines the iterator surface for query results. It is
// non-breaking to add methods here if callers adopt the narrower
// `QueryIterator` type; implementations should provide `AtValue()` which
// returns a safe value copy of the current item.
type QueryIterator interface {
	IteratorPoolInterface[*QueryResultItem]
	// AtValue returns a value-copy of the current QueryResultItem which is
	// safe to retain after advancing the iterator.
	AtValue() (QueryResultItem, error)
	UnderlyingAt() (*IteratorNode, error)
}

// Ownership contract note:
// - Implementations of `IteratorPoolInterface[*QueryResultItem]` may return
//   a pooled `*QueryResultItem` from `At()`. That pooled item is backed by
//   per-iterator buffers and reusable maps (e.g., `Tags`, `Fields`,
//   `AggregatedValues`). Callers MUST NOT retain references to those maps or
//   the `*QueryResultItem` after calling `Put(item)` or advancing the
//   iterator. `Put(item)` will clear maps and return internal buffers to the
//   pool.
// - If a caller needs to keep a stable copy of the current item beyond the
//   iterator's lifecycle, they must either call `AtValue()` (which returns a
//   value-copy) or deep-copy the maps/FieldValues before returning the item
//   to the pool.

// QueryResultIteratorInterface is the original name kept for compatibility
// and now aliases the richer `QueryIterator`.
type QueryResultIteratorInterface = QueryIterator

func (it *QueryResultItem) TypeNode() string {
	return "QUERYITERATOR"
}
