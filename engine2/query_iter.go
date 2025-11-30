package engine2

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexuscore/types"
)

// KeyDecoder decodes a memtable key into metric name, tag map and timestamp.
// It returns ok=false when the key couldn't be decoded.
type KeyDecoder func(key []byte) (metric string, tags map[string]string, ts int64, ok bool)

// memQueryIterator streams results from a top-level memtable iterator.
// memQueryIterator streams results from a top-level memtable iterator.
// Ownership note:
//   - `At()` returns a pointer to an internal `core.QueryResultItem` (`it.curr`).
//     That pointer references memory owned by the iterator and will be overwritten
//     on the next `Next()` call. `Put()` is a no-op for this iterator. Callers
//     that need to retain result data beyond the iterator lifetime must use
//     `AtValue()` which returns a deep value-copy.
type memQueryIterator struct {
	params  core.QueryParams
	iter    core.IteratorInterface[*core.IteratorNode]
	decoder KeyDecoder

	curr core.QueryResultItem
	err  error
}

// getPooledIterator creates a new memQueryIterator backed by the provided
// top-level memtable. The decoder is used to translate memtable keys into
// (metric, tags, timestamp).
func getPooledIterator(m *memtable.Memtable2, params core.QueryParams, decs ...KeyDecoder) *memQueryIterator {
	// choose decoder: provided one or default string-based decoder
	var dec KeyDecoder
	if len(decs) > 0 && decs[0] != nil {
		dec = decs[0]
	} else {
		dec = func(seriesKey []byte) (string, map[string]string, int64, bool) {
			if m == nil {
				return "", nil, 0, false
			}
			// assume string-encoded series key
			if s, tags, err := core.ExtractMetricAndTagsFromSeriesKeyWithString(seriesKey); err == nil {
				// Trim any trailing NUL bytes that may be present in the encoded
				// seriesKey components so comparisons against query tag values
				// (which do not include NUL terminators) succeed.
				cleanTags := make(map[string]string, len(tags))
				for k, v := range tags {
					cleanK := strings.TrimRight(k, "\x00")
					cleanV := strings.TrimRight(v, "\x00")
					cleanTags[cleanK] = cleanV
				}
				cleanMetric := strings.TrimRight(s, "\x00")
				return cleanMetric, cleanTags, 0, true
			}
			return "", nil, 0, false
		}
	}
	// create underlying iterator (holds read lock until Close())
	iter := m.NewIterator(nil, nil, types.Ascending)
	return &memQueryIterator{params: params, iter: iter, decoder: dec, curr: core.QueryResultItem{}}
}

func (it *memQueryIterator) Next() bool {
	for it.iter.Next() {
		node, err := it.iter.At()
		if err != nil {
			it.err = err
			return false
		}
		k := node.Key
		if len(k) < 8 {
			// malformed key (need at least timestamp)
			continue
		}
		// split seriesKey and timestamp
		seriesKey := k[:len(k)-8]
		ts := int64(binary.BigEndian.Uint64(k[len(k)-8:]))
		if ts < it.params.StartTime || ts > it.params.EndTime {
			continue
		}
		// decode using provided decoder
		metric, tags, _, ok := it.decoder(seriesKey)
		if !ok {
			// unable to decode series key; skip
			continue
		}
		if metric != it.params.Metric {
			continue
		}
		// verify tag filters
		match := true
		for qk, qv := range it.params.Tags {
			if tv, ok := tags[qk]; !ok || tv != qv {
				fmt.Printf("[DBG memQuery] tag compare qk=%s qv=%q(len=%d) tv=%q(len=%d) ok=%v\n", qk, qv, len(qv), tv, len(tv), ok)
				match = false
				break
			}
		}
		if !match {
			continue
		}
		// only emit Put events with non-empty value
		if node.EntryType != core.EntryTypePutEvent || len(node.Value) == 0 {
			continue
		}
		// decode fields from bytes
		fv, derr := core.DecodeFieldsFromBytes(node.Value)
		if derr != nil {
			// skip malformed value
			continue
		}
		it.curr.Metric = metric
		it.curr.Tags = tags
		it.curr.Timestamp = ts
		it.curr.Fields = fv
		return true
	}
	if err := it.iter.Error(); err != nil {
		it.err = err
	}
	// When iteration is exhausted, close the underlying iterator to
	// release the memtable read lock. This avoids leaving a reader lock
	// held (which blocks PutRaw's write lock) when callers forget to
	// explicitly call Close(). Closing here is safe and idempotent.
	if it.iter != nil {
		_ = it.iter.Close()
		it.iter = nil
	}
	return false
}

func (it *memQueryIterator) At() (*core.QueryResultItem, error) {
	return &it.curr, it.err
}

func (it *memQueryIterator) AtValue() (core.QueryResultItem, error) {
	if it.err != nil {
		return core.QueryResultItem{}, it.err
	}
	out := core.QueryResultItem{Metric: it.curr.Metric, Timestamp: it.curr.Timestamp}
	if it.curr.Tags != nil {
		tags := make(map[string]string, len(it.curr.Tags))
		for k, v := range it.curr.Tags {
			tags[k] = v
		}
		out.Tags = tags
	}
	if it.curr.Fields != nil {
		fv := make(core.FieldValues, len(it.curr.Fields))
		for k, v := range it.curr.Fields {
			fv[k] = v
		}
		out.Fields = fv
	}
	return out, nil
}

func (it *memQueryIterator) Error() error { return it.err }

func (it *memQueryIterator) Close() error {
	if it.iter != nil {
		_ = it.iter.Close()
	}
	return nil
}

func (it *memQueryIterator) Put(v *core.QueryResultItem) {}

func (it *memQueryIterator) UnderlyingAt() (*core.IteratorNode, error) { return nil, nil }
