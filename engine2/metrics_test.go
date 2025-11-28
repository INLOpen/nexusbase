package engine2

import (
	"expvar"
	"math"
	"strconv"
	"testing"
)

// fakeFilter is a tiny test helper implementing filter.Filter so tests can
// exercise sstable.Contains without loading files.
type fakeFilter struct{ r bool }

func (f *fakeFilter) Contains(_ []byte) bool { return f.r }
func (f *fakeFilter) Bytes() []byte          { return nil }

// Test that observeLatency correctly updates the histogram fields.
func TestObserveLatencyHistogram(t *testing.T) {
	em := NewEngineMetrics(false, "test_")

	// Sanity: initial count should be 0
	if c := em.PutLatencyHist.Get("count"); c == nil {
		t.Fatalf("expected count field to exist")
	}

	observeLatency(em.PutLatencyHist, 0.02) // 20ms

	// count should be 1
	if em.PutLatencyHist.Get("count").(*expvar.Int).String() != "1" {
		t.Fatalf("expected count 1, got %s", em.PutLatencyHist.Get("count").(*expvar.Int).String())
	}

	// sum should be approximately 0.02
	sumStr := em.PutLatencyHist.Get("sum").(*expvar.Float).String()
	v, err := strconv.ParseFloat(sumStr, 64)
	if err != nil {
		t.Fatalf("failed to parse sum: %v", err)
	}
	if math.Abs(v-0.02) > 1e-9 {
		t.Fatalf("unexpected sum value, want ~0.02 got %v", v)
	}

	// bucket 0.025 should have been incremented
	if em.PutLatencyHist.Get("le_0.025").(*expvar.Int).String() != "1" {
		t.Fatalf("expected bucket le_0.025 == 1")
	}

	// le_inf should also be incremented
	if em.PutLatencyHist.Get("le_inf").(*expvar.Int).String() != "1" {
		t.Fatalf("expected le_inf == 1")
	}
}

// Test that RecomputeIngestionRatio computes a correct ratio.
func TestRecomputeIngestionRatio(t *testing.T) {
	em := NewEngineMetrics(false, "test_")
	em.PutTotal.Add(3)
	em.PutErrorsTotal.Add(1)
	em.RecomputeIngestionRatio()

	s := em.IngestionRatio.String()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("failed to parse ingestion ratio: %v", err)
	}
	if math.Abs(v-0.75) > 1e-9 {
		t.Fatalf("unexpected ingestion ratio, want 0.75 got %v", v)
	}
}

// Test that bloom-related expvar counters are present and writable.
func TestBloomMetricsExists(t *testing.T) {
	em := NewEngineMetrics(false, "test_")
	if em.BloomFilterChecksTotal == nil {
		t.Fatalf("BloomFilterChecksTotal should not be nil")
	}
	if em.BloomFilterFalsePositivesTotal == nil {
		t.Fatalf("BloomFilterFalsePositivesTotal should not be nil")
	}

	// Increment and verify behavior
	em.BloomFilterChecksTotal.Add(2)
	if em.BloomFilterChecksTotal.String() != "2" {
		t.Fatalf("expected BloomFilterChecksTotal == 2, got %s", em.BloomFilterChecksTotal.String())
	}
}
