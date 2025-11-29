package engine2

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/cache"
	"github.com/INLOpen/nexusbase/core"
	internalpkg "github.com/INLOpen/nexusbase/engine2/internal"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// CompactionManagerInterface defines the surface used by callers.
type CompactionManagerInterface interface {
	Start(wg *sync.WaitGroup)
	SetMetricsCounters(
		compactionCount *expvar.Int, compactionLatencyHist *expvar.Map,
		dataReadBytes *expvar.Int,
		dataWrittenBytes *expvar.Int,
		tablesMerged *expvar.Int,
	)
	Stop()
	Trigger()
}

// CompactionOptions holds configuration for the compaction process.
type CompactionOptions struct {
	MaxL0Files                        int
	L0CompactionTriggerSize           int64
	TargetSSTableSize                 int64
	LevelsTargetSizeMultiplier        int
	CompactionIntervalSeconds         int
	MaxConcurrentLNCompactions        int
	IntraL0CompactionTriggerFiles     int
	IntraL0CompactionMaxFileSizeBytes int64
	SSTableCompressor                 core.Compressor
	RetentionPeriod                   string
}

// CompactionManagerParams groups parameters for NewCompactionManager.
type CompactionManagerParams struct {
	// Engine must provide both the external API (hooks, snapshots, metrics)
	// and the internal API (GetClock, GetNextSSTableID, etc.). Use an
	// anonymous interface combining both surfaces so callers may pass our
	// concrete adapter which implements the full aggregated interface.
	Engine interface {
		internalpkg.StorageEngineInternal
		StorageEngineExternal
	}
	LevelsManager        levels.Manager
	DataDir              string
	Opts                 CompactionOptions
	Logger               *slog.Logger
	Tracer               trace.Tracer
	IsSeriesDeleted      iterator.SeriesDeletedChecker
	IsRangeDeleted       iterator.RangeDeletedChecker
	ExtractSeriesKeyFunc iterator.SeriesKeyExtractorFunc
	BlockCache           cache.Interface
	Metrics              *EngineMetrics
	FileRemover          core.FileRemover
	SSTableWriterFactory core.SSTableWriterFactory
	ShutdownChan         chan struct{}

	// EnableSSTablePreallocate instructs created sstable writers to attempt
	// platform/file-system preallocation when creating files.
	EnableSSTablePreallocate bool
	// SSTableRestartPointInterval sets the restart point interval for writers
	// created by the compaction manager. If zero, writers use their defaults.
	SSTableRestartPointInterval int
	// SSTablePreallocMultiplier controls bytes-per-estimated-key used when
	// preallocating SSTable files. If zero, the writer default is used.
	SSTablePreallocMultiplier int
}

// CompactionManager is responsible for managing and executing compaction tasks.
type CompactionManager struct {
	Engine interface {
		internalpkg.StorageEngineInternal
		StorageEngineExternal
	}
	levelsManager levels.Manager
	dataDir       string
	opts          CompactionOptions

	compactionChan        chan struct{}
	shutdownChan          chan struct{}
	compactionWg          sync.WaitGroup
	l0CompactionActive    atomic.Bool
	lnCompactionSemaphore chan struct{}

	logger *slog.Logger

	compactionCount                   *expvar.Int
	compactionLatencyHist             *expvar.Map
	metricsCompactionDataReadBytes    *expvar.Int
	metricsCompactionDataWrittenBytes *expvar.Int
	metricsCompactionTablesMerged     *expvar.Int
	blockCache                        cache.Interface

	fileRemover          core.FileRemover
	sstableWriterFactory core.SSTableWriterFactory
	tracer               trace.Tracer

	isSeriesDeletedChecker      iterator.SeriesDeletedChecker
	isRangeDeletedChecker       iterator.RangeDeletedChecker
	extractSeriesKeyFuncForIter iterator.SeriesKeyExtractorFunc
	sstableCompressor           core.Compressor
	enableSSTablePreallocate    bool
	sstableRestartPointInterval int
	sstablePreallocMultiplier   int

	metrics *EngineMetrics
}

var _ CompactionManagerInterface = (*CompactionManager)(nil)

type compactionTask struct {
	sourceLevel    int
	targetLevel    int
	inputTables    []*sstable.SSTable
	isL0Compaction bool
	parentSpanCtx  context.Context
}

// runCompactionTask executes a compaction task and handles applying results/cleanup.
func (cm *CompactionManager) runCompactionTask(task *compactionTask) ([]*sstable.SSTable, error) {
	ctx, span := cm.tracer.Start(task.parentSpanCtx, "CompactionManager.runCompactionTask")
	defer span.End()
	span.SetAttributes(
		attribute.Int("compaction.source_level", task.sourceLevel),
		attribute.Int("compaction.target_level", task.targetLevel),
		attribute.Int("compaction.input_tables_count", len(task.inputTables)),
	)

	newTables, err := cm.mergeMultipleSSTables(ctx, task.inputTables, task.targetLevel)
	if err != nil {
		if errors.Is(err, sstable.ErrCorrupted) {
			cm.logger.Warn("Unrecoverable corruption during compaction. Quarantining source tables.", "source_level", task.sourceLevel, "error", err)
			cm.quarantineSSTables(ctx, task.inputTables)
			return nil, nil
		}
		return nil, err
	}

	for _, oldTable := range task.inputTables {
		level, _ := cm.levelsManager.GetLevelForTable(oldTable.ID())
		preSSTableDeletePayload := hooks.SSTablePayload{
			ID:    oldTable.ID(),
			Level: level,
			Path:  oldTable.FilePath(),
			Size:  oldTable.Size(),
		}
		if hookErr := cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreSSTableDeleteEvent(preSSTableDeletePayload)); hookErr != nil {
			cm.logger.Error("PreSSTableDelete hook failed, aborting compaction task.", "table_id", oldTable.ID(), "error", hookErr, "trace_id", span.SpanContext().TraceID().String())
			return nil, fmt.Errorf("PreSSTableDelete hook failed for table %d: %w", oldTable.ID(), hookErr)
		}
	}

	if err := cm.levelsManager.ApplyCompactionResults(task.sourceLevel, task.targetLevel, newTables, task.inputTables); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_apply_compaction_results")
		return nil, fmt.Errorf("failed to apply compaction results: %w", err)
	}

	if err := cm.removeAndCleanupSSTables(ctx, task.inputTables); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_cleanup_old_tables")
	}
	span.SetAttributes(attribute.Int("compaction.output_tables_count", len(newTables)))
	return newTables, nil
}

func (cm *CompactionManager) compactIntraL0(ctx context.Context) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.compactIntraL0")
	defer span.End()

	inputTables := cm.levelsManager.PickIntraL0CompactionCandidates(cm.opts.IntraL0CompactionTriggerFiles, cm.opts.IntraL0CompactionMaxFileSizeBytes)
	if len(inputTables) == 0 {
		cm.logger.Info("Intra-L0 compaction was triggered, but no candidate files were picked. Skipping.")
		return nil
	}
	cm.logger.Info("Compacting files within L0.", "input_tables_count", len(inputTables), "input_table_ids", levels.GetTableIDs(inputTables))
	span.SetAttributes(attribute.Int("input.l0_tables_count", len(inputTables)))

	task := &compactionTask{
		sourceLevel:    0,
		targetLevel:    0,
		inputTables:    inputTables,
		isL0Compaction: true,
		parentSpanCtx:  ctx,
	}

	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		cm.logger.Error("Intra-L0 compaction failed during merge", "error", err)
		return err
	}
	cm.logger.Info("Intra-L0 compaction completed.", "removed_old_tables_count", len(inputTables), "created_new_tables_count", len(newTables))
	return nil
}

func (cm *CompactionManager) compactL0ToL1(ctx context.Context) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.compactL0ToL1")
	defer span.End()

	l0Tables := cm.levelsManager.GetTablesForLevel(0)
	if len(l0Tables) == 0 {
		cm.logger.Info("compactL0ToL1 called, but L0 is empty.")
		return nil
	}

	minL0Key, maxL0Key := getOverallKeyRange(l0Tables)
	l1OverlapTables := cm.levelsManager.GetOverlappingTables(1, minL0Key, maxL0Key)
	cm.logger.Info("Compacting L0->L1.", "l0_tables_count", len(l0Tables), "min_l0_key", string(minL0Key), "max_l0_key", string(maxL0Key), "l1_overlap_tables_count", len(l1OverlapTables))
	span.SetAttributes(attribute.Int("input.l0_tables_count", len(l0Tables)), attribute.Int("input.l1_overlap_tables_count", len(l1OverlapTables)))

	inputTables := append(l0Tables, l1OverlapTables...)
	if len(inputTables) == 0 {
		cm.logger.Info("No input tables for L0->L1 compaction.")
		return nil
	}

	task := &compactionTask{sourceLevel: 0, targetLevel: 1, inputTables: inputTables, isL0Compaction: true, parentSpanCtx: ctx}

	preCompactionPayload := hooks.PreCompactionPayload{SourceLevel: 0, TargetLevel: 1}
	cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreCompactionEvent(preCompactionPayload))

	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		cm.logger.Error("L0->L1 compaction failed during merge", "error", err)
		return err
	}

	newTableInfos := make([]hooks.CompactedTableInfo, len(newTables))
	for i, t := range newTables {
		newTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	oldTableInfos := make([]hooks.CompactedTableInfo, len(inputTables))
	for i, t := range inputTables {
		oldTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	postCompactionPayload := hooks.PostCompactionPayload{SourceLevel: 0, TargetLevel: 1, NewTables: newTableInfos, OldTables: oldTableInfos}
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostCompactionEvent(postCompactionPayload))
	cm.logger.Info("L0->L1 compaction completed.", "removed_old_tables_count", len(inputTables))
	return nil
}

func (cm *CompactionManager) compactLevelNToLevelNPlus1(ctx context.Context, levelN int) error {
	_, span := cm.tracer.Start(ctx, fmt.Sprintf("CompactionManager.compactL%dToL%d", levelN, levelN+1))
	defer span.End()
	span.SetAttributes(attribute.Int("compaction.source_level", levelN))

	if levelN <= 0 {
		cm.logger.Error("compactLevelNToLevelNPlus1 called with invalid levelN.", "level", levelN)
		span.SetStatus(codes.Error, "invalid_source_level")
		return fmt.Errorf("invalid levelN: %d for LN->LN+1 compaction", levelN)
	}
	cm.logger.Info("Starting LN->LN+1 compaction.", "source_level", levelN, "target_level", levelN+1)

	tableToCompactN := cm.levelsManager.PickCompactionCandidateForLevelN(levelN)
	if tableToCompactN == nil {
		cm.logger.Info("No tables in source level to compact.", "level", levelN)
		span.SetAttributes(attribute.Bool("compaction.performed", false), attribute.String("compaction.skipped_reason", "no_candidate_table"))
		return nil
	}

	minKey, maxKey := tableToCompactN.MinKey(), tableToCompactN.MaxKey()
	tablesToCompactNPlus1 := cm.levelsManager.GetOverlappingTables(levelN+1, minKey, maxKey)
	cm.logger.Info("Compacting LN->LN+1.", "source_level", levelN, "source_table_id", tableToCompactN.ID(), "source_min_key", string(minKey), "source_max_key", string(maxKey), "overlap_target_level_tables_count", len(tablesToCompactNPlus1), "target_level", levelN+1)
	span.SetAttributes(attribute.Int64("input.source_table_id", int64(tableToCompactN.ID())), attribute.Int("input.overlap_target_tables_count", len(tablesToCompactNPlus1)))

	inputTables := []*sstable.SSTable{tableToCompactN}
	inputTables = append(inputTables, tablesToCompactNPlus1...)

	task := &compactionTask{sourceLevel: levelN, targetLevel: levelN + 1, inputTables: inputTables, isL0Compaction: false, parentSpanCtx: ctx}

	preCompactionPayload := hooks.PreCompactionPayload{SourceLevel: levelN, TargetLevel: levelN + 1}
	cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreCompactionEvent(preCompactionPayload))

	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		cm.logger.Error("LN->LN+1 compaction failed.", "source_level", levelN, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("l%d_compaction_failed", levelN))
		return err
	}

	newTableInfos := make([]hooks.CompactedTableInfo, len(newTables))
	for i, t := range newTables {
		newTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	oldTableInfos := make([]hooks.CompactedTableInfo, len(inputTables))
	for i, t := range inputTables {
		oldTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	postCompactionPayload := hooks.PostCompactionPayload{SourceLevel: levelN, TargetLevel: levelN + 1, NewTables: newTableInfos, OldTables: oldTableInfos}
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostCompactionEvent(postCompactionPayload))
	cm.logger.Info("LN->LN+1 compaction completed.", "source_level", levelN, "target_level", levelN+1, "removed_old_tables_count", len(inputTables))
	return nil
}

// NewCompactionManager creates a new CompactionManager.
func NewCompactionManager(params CompactionManagerParams) (CompactionManagerInterface, error) {
	shutdownChan := params.ShutdownChan
	if shutdownChan == nil {
		shutdownChan = make(chan struct{})
	}
	cm := &CompactionManager{
		Engine:                      params.Engine,
		levelsManager:               params.LevelsManager,
		dataDir:                     params.DataDir,
		opts:                        params.Opts,
		shutdownChan:                shutdownChan,
		compactionChan:              make(chan struct{}, 1),
		logger:                      params.Logger.With("component", "CompactionManager"),
		tracer:                      params.Tracer,
		isSeriesDeletedChecker:      params.IsSeriesDeleted,
		isRangeDeletedChecker:       params.IsRangeDeleted,
		extractSeriesKeyFuncForIter: params.ExtractSeriesKeyFunc,
		sstableCompressor:           params.Opts.SSTableCompressor,
		blockCache:                  params.BlockCache,
		metrics:                     params.Metrics,
		enableSSTablePreallocate:    params.EnableSSTablePreallocate,
		sstableRestartPointInterval: params.SSTableRestartPointInterval,
		sstablePreallocMultiplier:   params.SSTablePreallocMultiplier,
	}

	if params.FileRemover != nil {
		cm.fileRemover = params.FileRemover
	} else {
		cm.fileRemover = &realFileRemover{}
	}

	if params.Opts.MaxConcurrentLNCompactions <= 0 {
		cm.logger.Info("MaxConcurrentLNCompactions not set or invalid, defaulting to number of CPUs.", "provided_value", params.Opts.MaxConcurrentLNCompactions, "default_value", runtime.NumCPU())
		params.Opts.MaxConcurrentLNCompactions = runtime.NumCPU()
	} else if params.Opts.MaxConcurrentLNCompactions > runtime.NumCPU() {
		cm.logger.Warn("MaxConcurrentLNCompactions is set higher than the number of available CPUs. This may not improve performance and could lead to increased resource consumption.", "provided_value", params.Opts.MaxConcurrentLNCompactions, "num_cpu", runtime.NumCPU())
		params.Opts.MaxConcurrentLNCompactions = runtime.NumCPU()
	}

	cm.lnCompactionSemaphore = make(chan struct{}, params.Opts.MaxConcurrentLNCompactions)

	if params.SSTableWriterFactory != nil {
		cm.sstableWriterFactory = params.SSTableWriterFactory
	} else {
		cm.sstableWriterFactory = func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		}
	}
	return cm, nil
}

func (cm *CompactionManager) SetMetricsCounters(
	compactionCount *expvar.Int,
	compactionLatencyHist *expvar.Map,
	dataReadBytes *expvar.Int,
	dataWrittenBytes *expvar.Int,
	tablesMerged *expvar.Int,
) {
	cm.compactionCount = compactionCount
	cm.compactionLatencyHist = compactionLatencyHist
	cm.metricsCompactionDataReadBytes = dataReadBytes
	cm.metricsCompactionDataWrittenBytes = dataWrittenBytes
	cm.metricsCompactionTablesMerged = tablesMerged
}

func (cm *CompactionManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := time.Duration(cm.opts.CompactionIntervalSeconds) * time.Second
		if cm.opts.CompactionIntervalSeconds <= 0 {
			cm.logger.Warn("Invalid CompactionIntervalSeconds, defaulting to 60 seconds.", "interval_seconds", cm.opts.CompactionIntervalSeconds, "default_seconds", 60)
			interval = 60 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cm.performCompactionCycle()
			case <-cm.compactionChan:
				cm.performCompactionCycle()
			case <-cm.shutdownChan:
				cm.logger.Info("Shutting down compaction loop.")
				return
			}
		}
	}()
	cm.logger.Info("Started background compaction loop.")
}

func (cm *CompactionManager) Stop() {
	if cm.shutdownChan != nil {
		select {
		case <-cm.shutdownChan:
		default:
			close(cm.shutdownChan)
		}
		cm.compactionWg.Wait()
	}
	cm.logger.Info("Compaction loop stopped.")
}

func (cm *CompactionManager) Trigger() {
	select {
	case cm.compactionChan <- struct{}{}:
		cm.logger.Info("Manual compaction check triggered.")
	default:
		cm.logger.Info("Compaction check already pending, skipping manual trigger.")
	}
}

func (cm *CompactionManager) runL0CompactionTask(parentCtx context.Context, compactionType string, compactionFunc func(context.Context) error) {
	cm.compactionWg.Add(1)
	go func() {
		if cm.metrics != nil && cm.metrics.CompactionsInProgress != nil {
			cm.metrics.CompactionsInProgress.Add(1)
			defer cm.metrics.CompactionsInProgress.Add(-1)
		}
		defer cm.compactionWg.Done()
		defer cm.l0CompactionActive.Store(false)

		ctx, span := cm.tracer.Start(parentCtx, fmt.Sprintf("CompactionManager.%sWorker", strings.ReplaceAll(compactionType, "->", "To")))
		defer span.End()
		span.SetAttributes(attribute.String("compaction.type", compactionType))

		clock := cm.Engine.GetClock()
		startTime := clock.Now()

		if err := compactionFunc(ctx); err == nil {
			duration := clock.Now().Sub(startTime).Seconds()
			if cm.compactionLatencyHist != nil {
				observeLatency(cm.compactionLatencyHist, duration)
			}
			span.SetAttributes(attribute.Float64("compaction.duration_seconds", duration))
			cm.logger.Info(fmt.Sprintf("%s compaction finished successfully.", compactionType), "duration_seconds", duration)
			if cm.compactionCount != nil {
				cm.compactionCount.Add(1)
			}
			span.SetAttributes(attribute.Bool("compaction.performed", true))
		} else {
			cm.logger.Error(fmt.Sprintf("%s compaction failed.", compactionType), "error", err)
			span.SetStatus(codes.Error, fmt.Sprintf("%s compaction failed: %v", compactionType, err))
			if cm.metrics != nil && cm.metrics.CompactionErrorsTotal != nil {
				cm.metrics.CompactionErrorsTotal.Add(1)
			}
		}
	}()
}

func (cm *CompactionManager) performCompactionCycle() {
	ctx, span := cm.tracer.Start(context.Background(), "CompactionManager.performCompactionCycle")
	defer span.End()
	cm.logger.Debug("Checking for compaction needs...", "trace_id", span.SpanContext().TraceID().String())
	var compactionInitiatedThisCycle bool
	if cm.levelsManager.NeedsIntraL0Compaction(cm.opts.IntraL0CompactionTriggerFiles, cm.opts.IntraL0CompactionMaxFileSizeBytes) {
		if cm.l0CompactionActive.CompareAndSwap(false, true) {
			compactionInitiatedThisCycle = true
			cm.logger.Info("Intra-L0 compaction needed, starting task.", "trigger_files", cm.opts.IntraL0CompactionTriggerFiles)
			cm.runL0CompactionTask(ctx, "Intra-L0", cm.compactIntraL0)
		} else {
			cm.logger.Info("Skipping Intra-L0 compaction as another L0 compaction is already active.")
		}
	}

	if !compactionInitiatedThisCycle && cm.levelsManager.NeedsL0Compaction(cm.opts.MaxL0Files, cm.opts.L0CompactionTriggerSize) {
		if cm.l0CompactionActive.CompareAndSwap(false, true) {
			compactionInitiatedThisCycle = true
			cm.logger.Info("L0 compaction needed, starting L0->L1 compaction task.", "max_l0_files", cm.opts.MaxL0Files)
			cm.runL0CompactionTask(ctx, "L0->L1", cm.compactL0ToL1)
		} else {
			cm.logger.Info("Skipping L0 compaction as one is already active.")
			span.SetAttributes(attribute.String("compaction.skipped_reason", "l0_already_active"))
		}
	}

	for levelN := 1; levelN < cm.levelsManager.MaxLevels()-1; levelN++ {
		if cm.levelsManager.NeedsLevelNCompaction(levelN, cm.opts.LevelsTargetSizeMultiplier) {
			select {
			case cm.lnCompactionSemaphore <- struct{}{}:
				compactionInitiatedThisCycle = true
				cm.compactionWg.Add(1)
				cm.logger.Info("LN compaction needed, starting task.", "source_level", levelN)

				go func(lvl int, parentCtx context.Context) {
					if cm.metrics != nil && cm.metrics.CompactionsInProgress != nil {
						cm.metrics.CompactionsInProgress.Add(1)
						defer cm.metrics.CompactionsInProgress.Add(-1)
					}
					defer func() {
						<-cm.lnCompactionSemaphore
						cm.compactionWg.Done()
					}()

					lnCtx, lnSpan := cm.tracer.Start(parentCtx, fmt.Sprintf("CompactionManager.LNCompactionWorker.L%d", lvl))
					defer lnSpan.End()
					lnSpan.SetAttributes(attribute.String("compaction.type", fmt.Sprintf("L%d->L%d", lvl, lvl+1)))

					clock := cm.Engine.GetClock()
					startTime := clock.Now()

					if err := cm.compactLevelNToLevelNPlus1(lnCtx, lvl); err == nil {
						duration := clock.Now().Sub(startTime).Seconds()
						if cm.compactionLatencyHist != nil {
							observeLatency(cm.compactionLatencyHist, duration)
						}
						lnSpan.SetAttributes(attribute.Float64("compaction.duration_seconds", duration))
						cm.logger.Info("LN->LN+1 compaction finished successfully.", "source_level", lvl, "target_level", lvl+1, "duration_seconds", duration)
						if cm.compactionCount != nil {
							cm.compactionCount.Add(1)
						}
						lnSpan.SetAttributes(attribute.Bool("compaction.performed", true))
					} else {
						cm.logger.Error("LN->LN+1 compaction failed.", "source_level", lvl, "target_level", lvl+1, "error", err)
						lnSpan.SetStatus(codes.Error, fmt.Sprintf("L%d->L%d compaction failed: %v", lvl, lvl+1, err))
						if cm.metrics != nil && cm.metrics.CompactionErrorsTotal != nil {
							cm.metrics.CompactionErrorsTotal.Add(1)
						}
					}
				}(levelN, ctx)
			default:
				cm.logger.Debug("Skipping LN compaction due to concurrency limit.", "level", levelN, "max_concurrent", cap(cm.lnCompactionSemaphore))
				span.SetAttributes(attribute.String("compaction.skipped_reason", "concurrency_limit"), attribute.Int("compaction.skipped_level", levelN))
			}
		}
	}

	span.SetAttributes(attribute.Bool("compaction.any_initiated", compactionInitiatedThisCycle))
	if !compactionInitiatedThisCycle {
		cm.logger.Debug("No compaction needed or initiated in this cycle.")
	}
}

func (cm *CompactionManager) removeAndCleanupSSTables(ctx context.Context, tables []*sstable.SSTable) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.removeAndCleanupSSTables")
	defer span.End()
	span.SetAttributes(attribute.Int("tables_to_remove_count", len(tables)))

	var allErrors []error
	for _, oldTable := range tables {
		cm.logger.Info("Deleting old SSTable file after compaction.", "path", oldTable.FilePath(), "id", oldTable.ID())
		if errClose := oldTable.Close(); errClose != nil {
			// Ignore benign already-closed sentinel; otherwise record the error.
			if errClose != sstable.ErrClosed {
				cm.logger.Error("Error closing old SSTable before deletion.", "path", oldTable.FilePath(), "id", oldTable.ID(), "error", errClose)
				allErrors = append(allErrors, fmt.Errorf("failed to close old SSTable %s: %w", oldTable.FilePath(), errClose))
			}
		}

		if err := cm.fileRemover.Remove(oldTable.FilePath()); err != nil {
			cm.logger.Error("Error deleting old SSTable file.", "path", oldTable.FilePath(), "error", err)
			allErrors = append(allErrors, fmt.Errorf("failed to delete old SSTable file %s: %w", oldTable.FilePath(), err))
		} else {
			if cm.metrics != nil && cm.metrics.SSTablesDeletedTotal != nil {
				cm.metrics.SSTablesDeletedTotal.Add(1)
			}
		}
	}
	err := errors.Join(allErrors...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "one_or_more_files_failed_to_cleanup")
	}
	return err
}

func (cm *CompactionManager) quarantineSSTables(ctx context.Context, tables []*sstable.SSTable) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.quarantineSSTables")
	defer span.End()

	if len(tables) == 0 {
		return
	}

	if cm.metrics != nil && cm.metrics.CompactionErrorsTotal != nil {
		cm.metrics.CompactionErrorsTotal.Add(1)
	}

	dlqDir := filepath.Join(filepath.Dir(cm.dataDir), "dlq")
	if err := os.MkdirAll(dlqDir, 0755); err != nil {
		cm.logger.Error("Failed to create quarantine directory. Corrupted files will remain in sst dir.", "path", dlqDir, "error", err)
		return
	}

	tableIDs := levels.GetTableIDs(tables)
	cm.logger.Warn("Quarantining SSTables.", "count", len(tables), "ids", tableIDs)

	if err := cm.levelsManager.RemoveTables(0, tableIDs); err != nil {
		cm.logger.Error("Failed to remove quarantined tables from L0 state.", "error", err)
	}
	if err := cm.levelsManager.RemoveTables(1, tableIDs); err != nil {
		cm.logger.Error("Failed to remove quarantined tables from L1 state.", "error", err)
	}

	for _, table := range tables {
		if err := table.Close(); err != nil {
			if err != sstable.ErrClosed {
				cm.logger.Warn("Error closing quarantined SSTable", "path", table.FilePath(), "error", err)
			}
		}
		destPath := filepath.Join(dlqDir, filepath.Base(table.FilePath()))
		cm.logger.Info("Moving corrupted SSTable to quarantine.", "from", table.FilePath(), "to", destPath)
		if err := sys.Rename(table.FilePath(), destPath); err != nil {
			cm.logger.Error("Failed to move SSTable to quarantine directory.", "path", table.FilePath(), "error", err)
		}
	}
}

func (cm *CompactionManager) startNewSSTableWriter(fileID *uint64) (core.SSTableWriterInterface, error) {
	*fileID = cm.Engine.GetNextSSTableID()
	estimatedKeysForNewTable := uint64(cm.opts.TargetSSTableSize / 100)
	if estimatedKeysForNewTable == 0 {
		estimatedKeysForNewTable = 100
	}

	newWriter, writerErr := cm.sstableWriterFactory(core.SSTableWriterOptions{
		DataDir:                      cm.dataDir,
		ID:                           *fileID,
		EstimatedKeys:                estimatedKeysForNewTable,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       cm.tracer,
		Compressor:                   cm.sstableCompressor,
		Logger:                       cm.logger.With("sstable_writer_id", *fileID),
		Preallocate:                  cm.enableSSTablePreallocate,
		PreallocMultiplier:           cm.sstablePreallocMultiplier,
		RestartPointInterval:         cm.sstableRestartPointInterval,
	})
	if writerErr != nil {
		return nil, fmt.Errorf("failed to create new sstable writer: %w", writerErr)
	}
	return newWriter, nil
}

func (cm *CompactionManager) writeCompactedEntry(
	currentWriter *core.SSTableWriterInterface,
	newSSTables *[]*sstable.SSTable,
	currentFileID *uint64,
	key, value []byte,
	entryType core.EntryType,
	pointID uint64,
) error {
	if *currentWriter != nil && (*currentWriter).CurrentSize() > 0 && cm.opts.TargetSSTableSize > 0 && (*currentWriter).CurrentSize() >= cm.opts.TargetSSTableSize {
		if err := (*currentWriter).Finish(); err != nil {
			(*currentWriter).Abort()
			*currentWriter = nil
			return fmt.Errorf("failed to finish sstable: %w", err)
		}

		finalPath := (*currentWriter).FilePath()
		sstLoadOpts := sstable.LoadSSTableOptions{FilePath: finalPath, ID: *currentFileID, BlockCache: cm.blockCache, Tracer: cm.tracer, Logger: cm.logger}
		loadedTable, loadErr := sstable.LoadSSTable(sstLoadOpts)
		if loadErr != nil {
			return fmt.Errorf("failed to load newly created sstable %s: %w", finalPath, loadErr)
		}
		if cm.metrics != nil && cm.metrics.SSTablesCreatedTotal != nil {
			cm.metrics.SSTablesCreatedTotal.Add(1)
		}

		*newSSTables = append(*newSSTables, loadedTable)

		var writerErr error
		*currentWriter, writerErr = cm.startNewSSTableWriter(currentFileID)
		if writerErr != nil {
			return fmt.Errorf("failed to create new sstable writer during rollover: %w", writerErr)
		}
	}

	if err := (*currentWriter).Add(key, value, entryType, pointID); err != nil {
		(*currentWriter).Abort()
		*currentWriter = nil
		return fmt.Errorf("failed to add entry to sstable writer (key: %s): %w", string(key), err)
	}
	return nil
}

func (cm *CompactionManager) createMergingIterator(ctx context.Context, tables []*sstable.SSTable) (core.IteratorInterface[*core.IteratorNode], error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.createMergingIterator")
	defer span.End()

	if len(tables) == 0 {
		return iterator.NewEmptyIterator(), nil
	}

	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, table := range tables {
		iter, err := table.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			for _, openedIter := range iters {
				openedIter.Close()
			}
			return nil, fmt.Errorf("failed to create iterator for table %d: %w", table.ID(), err)
		}
		iters = append(iters, iter)
		if cm.metricsCompactionDataReadBytes != nil {
			cm.metricsCompactionDataReadBytes.Add(table.Size())
		}
	}

	mergeParams := iterator.MergingIteratorParams{
		Iters:                iters,
		IsSeriesDeleted:      cm.isSeriesDeletedChecker,
		IsRangeDeleted:       cm.isRangeDeletedChecker,
		ExtractSeriesKeyFunc: cm.extractSeriesKeyFuncForIter,
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	return iterator.NewMergingIteratorWithTombstones(mergeParams)
}

func (cm *CompactionManager) processMergedEntries(
	ctx context.Context,
	mergedIter core.IteratorInterface[*core.IteratorNode],
	retentionCutoffTime int64,
	currentWriter *core.SSTableWriterInterface,
	newSSTables *[]*sstable.SSTable,
	currentFileID *uint64,
) (int64, error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.processMergedEntries")
	defer span.End()

	var totalBytesWritten int64 = 0
	for mergedIter.Next() {
		cur, errCur := mergedIter.At()
		if errCur != nil {
			return 0, errCur
		}
		key, value, entryType, pointID := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		if retentionCutoffTime > 0 && entryType == core.EntryTypePutEvent {
			ts, err := core.DecodeTimestamp(key[len(key)-8:])
			if err != nil {
				cm.logger.Warn("Failed to decode timestamp during retention check, skipping entry.", "key_hex", fmt.Sprintf("%x", key), "error", err)
				continue
			}
			if ts < retentionCutoffTime {
				cm.logger.Debug("Skipping entry due to retention policy", "timestamp", ts, "cutoff", retentionCutoffTime)
				continue
			}
		}

		err := cm.writeCompactedEntry(currentWriter, newSSTables, currentFileID, key, value, entryType, pointID)
		if err != nil {
			return totalBytesWritten, err
		}
		totalBytesWritten += int64(len(key) + len(value))
	}

	if err := mergedIter.Error(); err != nil {
		return totalBytesWritten, fmt.Errorf("merging iterator error: %w", err)
	}

	return totalBytesWritten, nil
}

func (cm *CompactionManager) finalizeCompactionWriter(ctx context.Context, currentWriter core.SSTableWriterInterface, currentFileID uint64, level int) (*sstable.SSTable, error) {
	if currentWriter == nil {
		return nil, nil
	}

	_, span := cm.tracer.Start(ctx, "CompactionManager.finalizeCompactionWriter")
	defer span.End()

	if currentWriter.CurrentSize() == 0 {
		if err := currentWriter.Abort(); err != nil {
			cm.logger.Error("Failed to abort empty final writer", "error", err)
		}
		return nil, nil
	}

	if err := currentWriter.Finish(); err != nil {
		currentWriter.Abort()
		return nil, fmt.Errorf("failed to finish final sstable: %w", err)
	}

	finalPath := currentWriter.FilePath()
	loadedTable, loadErr := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: finalPath, ID: currentFileID, BlockCache: cm.blockCache, Tracer: cm.tracer, Logger: cm.logger})
	if loadErr != nil {
		return nil, fmt.Errorf("failed to load final newly created sstable %s: %w", finalPath, loadErr)
	}

	if cm.metrics != nil && cm.metrics.SSTablesCreatedTotal != nil {
		cm.metrics.SSTablesCreatedTotal.Add(1)
	}

	postSSTableCreatePayload := hooks.SSTablePayload{ID: loadedTable.ID(), Level: level, Path: loadedTable.FilePath(), Size: loadedTable.Size()}
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostSSTableCreateEvent(postSSTableCreatePayload))

	return loadedTable, nil
}

func (cm *CompactionManager) calculateRetentionCutoffTime(span trace.Span) int64 {
	if cm.opts.RetentionPeriod == "" || cm.Engine == nil {
		return 0
	}
	clock := cm.Engine.GetClock()
	duration, err := time.ParseDuration(cm.opts.RetentionPeriod)
	if err != nil {
		cm.logger.Error("Invalid retention_period format, disabling retention for this cycle.", "retention_period", cm.opts.RetentionPeriod, "error", err)
		return 0
	}

	cutoffTime := clock.Now().Add(-duration).UnixNano()
	if span != nil {
		span.SetAttributes(attribute.Int64("compaction.retention_cutoff_ns", cutoffTime))
	}
	cm.logger.Debug("Retention policy applied", "cutoff_time", time.Unix(0, cutoffTime).Format(time.RFC3339Nano))
	return cutoffTime
}

func (cm *CompactionManager) mergeMultipleSSTables(ctx context.Context, tables []*sstable.SSTable, targetLevelNum int) ([]*sstable.SSTable, error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.mergeMultipleSSTables")
	defer span.End()
	span.SetAttributes(attribute.Int("input.tables_count", len(tables)), attribute.Int("compaction.target_level", targetLevelNum))

	if len(tables) == 0 {
		return nil, nil
	}

	if cm.metricsCompactionTablesMerged != nil {
		cm.metricsCompactionTablesMerged.Add(int64(len(tables)))
	}

	mergedIter, err := cm.createMergingIterator(ctx, tables)
	if err != nil {
		return nil, err
	}
	defer mergedIter.Close()

	var newSSTables []*sstable.SSTable
	var currentWriter core.SSTableWriterInterface
	var currentFileID uint64
	retentionCutoffTime := cm.calculateRetentionCutoffTime(span)

	currentWriter, err = cm.startNewSSTableWriter(&currentFileID)
	if err != nil {
		return nil, err
	}

	totalBytesWritten, err := cm.processMergedEntries(ctx, mergedIter, retentionCutoffTime, &currentWriter, &newSSTables, &currentFileID)
	if err != nil {
		if currentWriter != nil {
			currentWriter.Abort()
		}
		return nil, err
	}

	finalTable, err := cm.finalizeCompactionWriter(ctx, currentWriter, currentFileID, targetLevelNum)
	if err != nil {
		var cleanupErrors []error
		for _, tbl := range newSSTables {
			if closeErr := tbl.Close(); closeErr != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to close intermediate sstable %d on error path: %w", tbl.ID(), closeErr))
			}
			if removeErr := sys.Remove(tbl.FilePath()); removeErr != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to remove intermediate sstable %s on error path: %w", tbl.FilePath(), removeErr))
			}
		}
		return nil, errors.Join(append(cleanupErrors, err)...)
	}
	if finalTable != nil {
		newSSTables = append(newSSTables, finalTable)
	}

	if cm.metricsCompactionDataWrittenBytes != nil {
		cm.metricsCompactionDataWrittenBytes.Add(totalBytesWritten)
	}

	span.SetAttributes(attribute.Int("output.merged_sstable_count", len(newSSTables)))
	return newSSTables, nil
}

// ExportMergeMultipleSSTables provides a migration-friendly exported wrapper that
// lets engine2 tests call the internal merge logic without importing legacy engine.
func ExportMergeMultipleSSTables(cm *CompactionManager, ctx context.Context, tables []*sstable.SSTable, targetLevel int) ([]*sstable.SSTable, error) {
	return cm.mergeMultipleSSTables(ctx, tables, targetLevel)
}
