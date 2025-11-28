package engine2

import "expvar"

// EngineMetricsLegacy mirrors the legacy `engine.EngineMetrics` structure so
// `engine2` can convert to/from a compatible representation without
// importing the legacy `engine` package directly. It purposely omits
// unexported hook functions which are not addressable across package
// boundaries.
type EngineMetricsLegacy struct {
	PublishedGlobally bool

	PutTotal              *expvar.Int
	PutErrorsTotal        *expvar.Int
	GetTotal              *expvar.Int
	QueryTotal            *expvar.Int
	QueryErrorsTotal      *expvar.Int
	DeleteTotal           *expvar.Int
	FlushTotal            *expvar.Int
	CompactionTotal       *expvar.Int
	CompactionErrorsTotal *expvar.Int
	SSTablesCreatedTotal  *expvar.Int
	SSTablesDeletedTotal  *expvar.Int

	FlushDataPointsFlushedTotal *expvar.Int
	FlushBytesFlushedTotal      *expvar.Int

	PutLatencyHist              *expvar.Map
	GetLatencyHist              *expvar.Map
	DeleteLatencyHist           *expvar.Map
	QueryLatencyHist            *expvar.Map
	RangeScanLatencyHist        *expvar.Map
	AggregationQueryLatencyHist *expvar.Map

	FlushLatencyHist      *expvar.Map
	CompactionLatencyHist *expvar.Map

	BloomFilterChecksTotal         *expvar.Int
	BloomFilterFalsePositivesTotal *expvar.Int

	WALBytesWrittenTotal   *expvar.Int
	WALEntriesWrittenTotal *expvar.Int

	WALRecoveryDurationSeconds *expvar.Float
	WALRecoveredEntriesTotal   *expvar.Int

	CompactionDataReadBytesTotal    *expvar.Int
	CompactionDataWrittenBytesTotal *expvar.Int
	CompactionTablesMergedTotal     *expvar.Int

	CacheHits   *expvar.Int
	CacheMisses *expvar.Int

	SeriesCreatedTotal *expvar.Int

	ActiveQueries         *expvar.Int
	CompactionsInProgress *expvar.Int

	ReplicationErrorsTotal       *expvar.Int
	ReplicationPutTotal          *expvar.Int
	ReplicationDeleteSeriesTotal *expvar.Int
	ReplicationDeleteRangeTotal  *expvar.Int

	PreallocateEnabled *expvar.Int

	PreallocSuccesses   *expvar.Int
	PreallocFailures    *expvar.Int
	PreallocUnsupported *expvar.Int
}

// ToEngine converts an engine2 EngineMetrics2 into a legacy-compatible
// `EngineMetricsLegacy` instance. This copies pointer references so the
// returned struct shares expvar variables (intended for tooling/interop
// without importing the legacy package).
func (m *EngineMetrics) ToEngine() *EngineMetricsLegacy {
	if m == nil {
		return nil
	}
	return &EngineMetricsLegacy{
		PublishedGlobally:     m.PublishedGlobally,
		PutTotal:              m.PutTotal,
		PutErrorsTotal:        m.PutErrorsTotal,
		GetTotal:              m.GetTotal,
		QueryTotal:            m.QueryTotal,
		QueryErrorsTotal:      m.QueryErrorsTotal,
		DeleteTotal:           m.DeleteTotal,
		FlushTotal:            m.FlushTotal,
		CompactionTotal:       m.CompactionTotal,
		CompactionErrorsTotal: m.CompactionErrorsTotal,

		SSTablesCreatedTotal: m.SSTablesCreatedTotal,
		SSTablesDeletedTotal: m.SSTablesDeletedTotal,

		FlushDataPointsFlushedTotal: m.FlushDataPointsFlushedTotal,
		FlushBytesFlushedTotal:      m.FlushBytesFlushedTotal,

		PutLatencyHist:              m.PutLatencyHist,
		GetLatencyHist:              m.GetLatencyHist,
		DeleteLatencyHist:           m.DeleteLatencyHist,
		QueryLatencyHist:            m.QueryLatencyHist,
		RangeScanLatencyHist:        m.RangeScanLatencyHist,
		AggregationQueryLatencyHist: m.AggregationQueryLatencyHist,

		FlushLatencyHist:      m.FlushLatencyHist,
		CompactionLatencyHist: m.CompactionLatencyHist,

		BloomFilterChecksTotal:         m.BloomFilterChecksTotal,
		BloomFilterFalsePositivesTotal: m.BloomFilterFalsePositivesTotal,

		WALBytesWrittenTotal:   m.WALBytesWrittenTotal,
		WALEntriesWrittenTotal: m.WALEntriesWrittenTotal,

		WALRecoveryDurationSeconds: m.WALRecoveryDurationSeconds,
		WALRecoveredEntriesTotal:   m.WALRecoveredEntriesTotal,

		CompactionDataReadBytesTotal:    m.CompactionDataReadBytesTotal,
		CompactionDataWrittenBytesTotal: m.CompactionDataWrittenBytesTotal,
		CompactionTablesMergedTotal:     m.CompactionTablesMergedTotal,

		CacheHits:   m.CacheHits,
		CacheMisses: m.CacheMisses,

		SeriesCreatedTotal: m.SeriesCreatedTotal,

		ActiveQueries:         m.ActiveQueries,
		CompactionsInProgress: m.CompactionsInProgress,

		ReplicationErrorsTotal:       m.ReplicationErrorsTotal,
		ReplicationPutTotal:          m.ReplicationPutTotal,
		ReplicationDeleteSeriesTotal: m.ReplicationDeleteSeriesTotal,
		ReplicationDeleteRangeTotal:  m.ReplicationDeleteRangeTotal,

		PreallocateEnabled: m.PreallocateEnabled,

		PreallocSuccesses:   m.PreallocSuccesses,
		PreallocFailures:    m.PreallocFailures,
		PreallocUnsupported: m.PreallocUnsupported,
	}
}

// FromEngine builds an EngineMetrics2 from a legacy-compatible
// `EngineMetricsLegacy` instance.
func FromEngine(e *EngineMetricsLegacy) *EngineMetrics {
	if e == nil {
		return nil
	}
	return &EngineMetrics{
		PublishedGlobally:     e.PublishedGlobally,
		PutTotal:              e.PutTotal,
		PutErrorsTotal:        e.PutErrorsTotal,
		GetTotal:              e.GetTotal,
		QueryTotal:            e.QueryTotal,
		QueryErrorsTotal:      e.QueryErrorsTotal,
		DeleteTotal:           e.DeleteTotal,
		FlushTotal:            e.FlushTotal,
		CompactionTotal:       e.CompactionTotal,
		CompactionErrorsTotal: e.CompactionErrorsTotal,

		SSTablesCreatedTotal: e.SSTablesCreatedTotal,
		SSTablesDeletedTotal: e.SSTablesDeletedTotal,

		FlushDataPointsFlushedTotal: e.FlushDataPointsFlushedTotal,
		FlushBytesFlushedTotal:      e.FlushBytesFlushedTotal,

		PutLatencyHist:              e.PutLatencyHist,
		GetLatencyHist:              e.GetLatencyHist,
		DeleteLatencyHist:           e.DeleteLatencyHist,
		QueryLatencyHist:            e.QueryLatencyHist,
		RangeScanLatencyHist:        e.RangeScanLatencyHist,
		AggregationQueryLatencyHist: e.AggregationQueryLatencyHist,

		FlushLatencyHist:      e.FlushLatencyHist,
		CompactionLatencyHist: e.CompactionLatencyHist,

		BloomFilterChecksTotal:         e.BloomFilterChecksTotal,
		BloomFilterFalsePositivesTotal: e.BloomFilterFalsePositivesTotal,

		WALBytesWrittenTotal:   e.WALBytesWrittenTotal,
		WALEntriesWrittenTotal: e.WALEntriesWrittenTotal,

		WALRecoveryDurationSeconds: e.WALRecoveryDurationSeconds,
		WALRecoveredEntriesTotal:   e.WALRecoveredEntriesTotal,

		CompactionDataReadBytesTotal:    e.CompactionDataReadBytesTotal,
		CompactionDataWrittenBytesTotal: e.CompactionDataWrittenBytesTotal,
		CompactionTablesMergedTotal:     e.CompactionTablesMergedTotal,

		CacheHits:   e.CacheHits,
		CacheMisses: e.CacheMisses,

		SeriesCreatedTotal: e.SeriesCreatedTotal,

		ActiveQueries:         e.ActiveQueries,
		CompactionsInProgress: e.CompactionsInProgress,

		ReplicationErrorsTotal:       e.ReplicationErrorsTotal,
		ReplicationPutTotal:          e.ReplicationPutTotal,
		ReplicationDeleteSeriesTotal: e.ReplicationDeleteSeriesTotal,
		ReplicationDeleteRangeTotal:  e.ReplicationDeleteRangeTotal,

		PreallocateEnabled: e.PreallocateEnabled,

		PreallocSuccesses:   e.PreallocSuccesses,
		PreallocFailures:    e.PreallocFailures,
		PreallocUnsupported: e.PreallocUnsupported,
	}
}
