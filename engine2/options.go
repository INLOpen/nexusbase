package engine2

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexuscore/utils/clock"
	"go.opentelemetry.io/otel/trace"
)

// StorageEngineOptions mirrors the legacy engine.StorageEngineOptions but
// lives in engine2 so engine2 is not dependent on the legacy package.
type StorageEngineOptions struct {
	DataDir                        string
	MemtableThreshold              int64
	MemtableFlushIntervalMs        int
	BlockCacheCapacity             int
	L0CompactionTriggerSize        int64
	WriteBufferSize                int
	Metrics                        *EngineMetrics
	MaxL0Files                     int
	CompactionIntervalSeconds      int
	TargetSSTableSize              int64
	LevelsTargetSizeMultiplier     int
	MaxLevels                      int
	MaxConcurrentLNCompactions     int
	BloomFilterFalsePositiveRate   float64
	SSTableDefaultBlockSize        int
	InitialSequenceNumber          uint64
	CheckpointIntervalSeconds      int
	TracerProvider                 trace.TracerProvider
	TestingOnlyFailFlushCount      *atomic.Int32
	ErrorOnSSTableLoadFailure      bool
	SSTableCompressor              core.Compressor
	TestingOnlyInjectWALCloseError error
	WALSyncMode                    core.WALSyncMode
	WALBatchSize                   int
	WALFlushIntervalMs             int
	WALPurgeKeepSegments           int
	WALMaxSegmentSize              int64
	RetentionPeriod                string
	MetadataSyncIntervalSeconds    int
	EnableTagBloomFilter           bool
	IndexMemtableThreshold         int64
	IndexFlushIntervalMs           int
	IndexCompactionIntervalSeconds int
	IndexMaxL0Files                int
	IndexMaxLevels                 int
	IndexBaseTargetSize            int64
	CompactionTombstoneWeight      float64
	CompactionOverlapWeight        float64
	Logger                         *slog.Logger
	Clock                          clock.Clock

	EnableSSTablePreallocate bool

	// SSTablePreallocMultiplier controls the number of bytes to preallocate
	// per estimated key when SSTable preallocation is enabled. A sensible
	// default is 128 (bytes per estimated key) when zero.
	SSTablePreallocMultiplier int

	// SSTableRestartPointInterval sets the default restart-point interval
	// for SSTable writers created by the engine adapter. If zero, the
	// writer's internal default is used.
	SSTableRestartPointInterval int

	SelfMonitoringEnabled      bool
	SelfMonitoringPrefix       string
	SelfMonitoringIntervalMs   int
	RelativeQueryRoundingRules []RoundingRule
	CompactionFallbackStrategy levels.CompactionFallbackStrategy

	IntraL0CompactionTriggerFiles     int
	IntraL0CompactionMaxFileSizeBytes int64
	ReplicationMode                   string
	LeaderAddress                     string
	// Optional HookManager to be attached to the adapter created by
	// compatibility helpers. When present, the adapter will be created with
	// this hook manager so callers can receive hook events.
	HookManager hooks.HookManager

	// TmpFileCleanupThresholdSeconds controls how old `.tmp` SSTable files
	// must be (in seconds) before they are cleaned up at startup or by the
	// periodic background cleaner. If zero, a default of 60 seconds is used.
	TmpFileCleanupThresholdSeconds int

	// TmpFileCleanupIntervalSeconds controls the periodic cleanup interval
	// (in seconds). If zero, no periodic cleanup goroutine is started.
	TmpFileCleanupIntervalSeconds int
}

// RoundingRule mirrors the helper type used by engine tests for relative
// query rounding rules.
type RoundingRule struct {
	QueryDurationThreshold time.Duration
	RoundingDuration       time.Duration
}
