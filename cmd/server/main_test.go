package main

import (
	"log/slog"
	"testing"

	optsPackage "github.com/INLOpen/nexusbase/cmd/server/opts"
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/levels"
)

func TestBuildEngineOptions_CompactionAndIndexPropagation(t *testing.T) {
	cfg := &config.Config{
		Engine: config.EngineConfig{
			DataDir: "testdata",
			Compaction: config.CompactionConfig{
				L0TriggerSizeBytes:      12345,
				TargetSSTableSizeBytes:  67890,
				LevelsSizeMultiplier:    7,
				MaxLevels:               9,
				L0TriggerFileCount:      5,
				TombstoneWeight:         2.5,
				OverlapPenaltyWeight:    1.25,
				IntraL0TriggerFileCount: 3,
				IntraL0MaxFileSizeBytes: 4096,
				CheckInterval:           "45s",
				FallbackStrategy:        "PickNewest",
			},
			Index: config.IndexConfig{
				MemtableThreshold:       777,
				FlushInterval:           "30s",
				CompactionCheckInterval: "15s",
				L0TriggerFileCount:      6,
				BaseTargetSizeBytes:     2048,
			},
			SSTable: config.SSTableConfig{
				BlockSizeBytes:                8192,
				PreallocMultiplierBytesPerKey: 128,
				RestartPointInterval:          16,
			},
		},
	}

	hm := hooks.NewHookManager(slog.Default())
	opts := optsPackage.BuildEngineOptions(cfg, hm, slog.Default(), compressors.NewSnappyCompressor())

	if opts.L0CompactionTriggerSize != cfg.Engine.Compaction.L0TriggerSizeBytes {
		t.Fatalf("L0CompactionTriggerSize not propagated: got %d want %d", opts.L0CompactionTriggerSize, cfg.Engine.Compaction.L0TriggerSizeBytes)
	}
	if opts.TargetSSTableSize != cfg.Engine.Compaction.TargetSSTableSizeBytes {
		t.Fatalf("TargetSSTableSize not propagated: got %d want %d", opts.TargetSSTableSize, cfg.Engine.Compaction.TargetSSTableSizeBytes)
	}
	if opts.LevelsTargetSizeMultiplier != cfg.Engine.Compaction.LevelsSizeMultiplier {
		t.Fatalf("LevelsTargetSizeMultiplier not propagated: got %d want %d", opts.LevelsTargetSizeMultiplier, cfg.Engine.Compaction.LevelsSizeMultiplier)
	}
	if opts.MaxLevels != cfg.Engine.Compaction.MaxLevels {
		t.Fatalf("MaxLevels not propagated: got %d want %d", opts.MaxLevels, cfg.Engine.Compaction.MaxLevels)
	}
	if opts.MaxL0Files != cfg.Engine.Compaction.L0TriggerFileCount {
		t.Fatalf("MaxL0Files not propagated: got %d want %d", opts.MaxL0Files, cfg.Engine.Compaction.L0TriggerFileCount)
	}
	if opts.CompactionTombstoneWeight != cfg.Engine.Compaction.TombstoneWeight {
		t.Fatalf("CompactionTombstoneWeight not propagated: got %f want %f", opts.CompactionTombstoneWeight, cfg.Engine.Compaction.TombstoneWeight)
	}
	if opts.CompactionOverlapWeight != cfg.Engine.Compaction.OverlapPenaltyWeight {
		t.Fatalf("CompactionOverlapWeight not propagated: got %f want %f", opts.CompactionOverlapWeight, cfg.Engine.Compaction.OverlapPenaltyWeight)
	}
	if opts.IntraL0CompactionTriggerFiles != cfg.Engine.Compaction.IntraL0TriggerFileCount {
		t.Fatalf("IntraL0CompactionTriggerFiles not propagated: got %d want %d", opts.IntraL0CompactionTriggerFiles, cfg.Engine.Compaction.IntraL0TriggerFileCount)
	}
	if opts.IntraL0CompactionMaxFileSizeBytes != cfg.Engine.Compaction.IntraL0MaxFileSizeBytes {
		t.Fatalf("IntraL0CompactionMaxFileSizeBytes not propagated: got %d want %d", opts.IntraL0CompactionMaxFileSizeBytes, cfg.Engine.Compaction.IntraL0MaxFileSizeBytes)
	}
	if opts.CompactionFallbackStrategy != levels.PickNewest {
		t.Fatalf("CompactionFallbackStrategy not propagated: got %v want %v", opts.CompactionFallbackStrategy, levels.PickNewest)
	}
	if opts.CompactionIntervalSeconds != 45 {
		t.Fatalf("CompactionIntervalSeconds not parsed: got %d want %d", opts.CompactionIntervalSeconds, 45)
	}

	// Index fields
	if opts.IndexMemtableThreshold != cfg.Engine.Index.MemtableThreshold {
		t.Fatalf("IndexMemtableThreshold not propagated: got %d want %d", opts.IndexMemtableThreshold, cfg.Engine.Index.MemtableThreshold)
	}
	if opts.IndexFlushIntervalMs != 30000 {
		t.Fatalf("IndexFlushIntervalMs not parsed: got %d want %d", opts.IndexFlushIntervalMs, 30000)
	}
	if opts.IndexCompactionIntervalSeconds != 15 {
		t.Fatalf("IndexCompactionIntervalSeconds not parsed: got %d want %d", opts.IndexCompactionIntervalSeconds, 15)
	}
	if opts.IndexMaxL0Files != cfg.Engine.Index.L0TriggerFileCount {
		t.Fatalf("IndexMaxL0Files not propagated: got %d want %d", opts.IndexMaxL0Files, cfg.Engine.Index.L0TriggerFileCount)
	}
	if opts.IndexBaseTargetSize != cfg.Engine.Index.BaseTargetSizeBytes {
		t.Fatalf("IndexBaseTargetSize not propagated: got %d want %d", opts.IndexBaseTargetSize, cfg.Engine.Index.BaseTargetSizeBytes)
	}
}
