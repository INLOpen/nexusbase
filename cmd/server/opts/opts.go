package opts

import (
	"log/slog"
	"strings"
	"time"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/levels"
)

// BuildEngineOptions maps the loaded configuration into engine2.StorageEngineOptions.
// Exported for reuse across packages and tests.
func BuildEngineOptions(cfg *config.Config, hookManager hooks.HookManager, logger *slog.Logger, sstCompressor core.Compressor) engine2.StorageEngineOptions {
	// Map compaction fallback strategy
	compactionFallback := levels.PickOldest
	switch strings.ToLower(cfg.Engine.Compaction.FallbackStrategy) {
	case "picklargest", "largest":
		compactionFallback = levels.PickLargest
	case "picksmallest", "smallest":
		compactionFallback = levels.PickSmallest
	case "pickmostkeys", "mostkeys":
		compactionFallback = levels.PickMostKeys
	case "picksmallestavgkeysize", "smallestavgkeysize":
		compactionFallback = levels.PickSmallestAvgKeySize
	case "pickoldestbytimestamp", "oldestbytimestamp":
		compactionFallback = levels.PickOldestByTimestamp
	case "pickfewestkeys", "fewestkeys":
		compactionFallback = levels.PickFewestKeys
	case "pickrandom", "random":
		compactionFallback = levels.PickRandom
	case "picklargestavgkeysize", "largestavgkeysize":
		compactionFallback = levels.PickLargestAvgKeySize
	case "picknewest", "newest":
		compactionFallback = levels.PickNewest
	case "pickhighesttombstonedensity", "highesttombstonedensity":
		compactionFallback = levels.PickHighestTombstoneDensity
	default:
		compactionFallback = levels.PickOldest
	}

	compactionIntervalSeconds := int(config.ParseDuration(cfg.Engine.Compaction.CheckInterval, 120*time.Second, logger).Seconds())

	opts := engine2.StorageEngineOptions{
		DataDir:                      cfg.Engine.DataDir,
		HookManager:                  hookManager,
		Logger:                       logger,
		EnableSSTablePreallocate:     cfg.Engine.SSTable.Preallocate,
		SSTablePreallocMultiplier:    cfg.Engine.SSTable.PreallocMultiplierBytesPerKey,
		SSTableDefaultBlockSize:      int(cfg.Engine.SSTable.BlockSizeBytes),
		BloomFilterFalsePositiveRate: cfg.Engine.SSTable.BloomFilterFPRate,
		SSTableCompressor:            sstCompressor,
		SSTableRestartPointInterval:  cfg.Engine.SSTable.RestartPointInterval,

		// Index-related configuration mapped from config.Engine.Index
		IndexMemtableThreshold:         cfg.Engine.Index.MemtableThreshold,
		IndexFlushIntervalMs:           int(config.ParseDuration(cfg.Engine.Index.FlushInterval, 60*time.Second, logger).Milliseconds()),
		IndexCompactionIntervalSeconds: int(config.ParseDuration(cfg.Engine.Index.CompactionCheckInterval, 20*time.Second, logger).Seconds()),
		IndexMaxL0Files:                cfg.Engine.Index.L0TriggerFileCount,
		IndexBaseTargetSize:            cfg.Engine.Index.BaseTargetSizeBytes,

		// Global compaction and tuning options mapped from config.Engine.Compaction
		L0CompactionTriggerSize:           cfg.Engine.Compaction.L0TriggerSizeBytes,
		TargetSSTableSize:                 cfg.Engine.Compaction.TargetSSTableSizeBytes,
		LevelsTargetSizeMultiplier:        cfg.Engine.Compaction.LevelsSizeMultiplier,
		MaxLevels:                         cfg.Engine.Compaction.MaxLevels,
		MaxL0Files:                        cfg.Engine.Compaction.L0TriggerFileCount,
		CompactionTombstoneWeight:         cfg.Engine.Compaction.TombstoneWeight,
		CompactionOverlapWeight:           cfg.Engine.Compaction.OverlapPenaltyWeight,
		IntraL0CompactionTriggerFiles:     cfg.Engine.Compaction.IntraL0TriggerFileCount,
		IntraL0CompactionMaxFileSizeBytes: cfg.Engine.Compaction.IntraL0MaxFileSizeBytes,
		CompactionFallbackStrategy:        compactionFallback,
		CompactionIntervalSeconds:         compactionIntervalSeconds,
	}

	// If self-monitoring is enabled, publish EngineMetrics globally using the configured prefix.
	if cfg.SelfMonitoring.Enabled {
		prefix := cfg.SelfMonitoring.MetricPrefix
		// Ensure prefix ends with an underscore for consistency when empty or custom
		if prefix == "" {
			prefix = "engine_"
		}
		opts.Metrics = engine2.NewEngineMetrics(true, prefix)
	}

	return opts
}
