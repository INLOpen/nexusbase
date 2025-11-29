package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

// IndexMemtable is the in-memory store for the tag index.
// It maps tag IDs to roaring bitmaps of series IDs.
type IndexMemtable struct {
	mu sync.RWMutex
	// map[tagKeyID] -> map[tagValueID] -> *roaring64.Bitmap
	index map[uint64]tagValueMap
	// size tracks the number of bitmaps in the memtable.
	size int64
}

// NewIndexMemtable creates a new, empty index memtable.
func NewIndexMemtable() *IndexMemtable {
	return &IndexMemtable{
		index: make(map[uint64]tagValueMap),
	}
}

// tagValueMap represents the nested map for a tag key mapping to value bitmaps.
type tagValueMap map[uint64]*roaring64.Bitmap

// Add adds a seriesID to the bitmap for a given tag key/value pair.
// It is safe for concurrent use.
func (im *IndexMemtable) Add(tagKeyID, tagValueID uint64, seriesID uint64) {
	im.mu.Lock()
	defer im.mu.Unlock()

	vmap, ok := im.index[tagKeyID]
	if !ok {
		vmap = make(tagValueMap)
		im.index[tagKeyID] = vmap
	}
	if _, ok := vmap[tagValueID]; !ok {
		vmap[tagValueID] = roaring64.New()
		im.size++ // Increment size only when a new bitmap is created.
	}
	vmap[tagValueID].Add(seriesID)
}

// Remove removes a seriesID from all bitmaps in the memtable.
func (im *IndexMemtable) Remove(seriesID uint64) {
	im.mu.Lock()
	defer im.mu.Unlock()

	for _, valueMap := range im.index {
		for _, bitmap := range valueMap {
			bitmap.Remove(seriesID)
		}
	}
}

// Size returns the number of bitmaps in the memtable.
func (im *IndexMemtable) Size() int64 {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.size
}

// GetBitmap returns a clone of the bitmap for a given tag.
// It returns a new empty bitmap if the tag is not found.
func (im *IndexMemtable) GetBitmap(tagKeyID, tagValueID uint64) *roaring64.Bitmap {
	im.mu.RLock()
	defer im.mu.RUnlock()
	if valMap, ok := im.index[tagKeyID]; ok {
		if bm, ok := valMap[tagValueID]; ok {
			return bm.Clone() // Return a clone to prevent race conditions
		}
	}
	return roaring64.New()
}

//	provides the necessary shared components from the main
//
// storage engine to the TagIndexManager, promoting looser coupling.
type TagIndexDependencies struct {
	StringStore     StringStoreInterface
	SeriesIDStore   SeriesIDStoreInterface
	DeletedSeries   map[string]uint64
	DeletedSeriesMu *sync.RWMutex
	SSTNextID       core.SSTableNextIDFactory
}

// TagIndexManagerOptions holds configuration for the TagIndexManager.
type TagIndexManagerOptions struct {
	DataDir                    string
	MemtableThreshold          int64 // Threshold in number of bitmaps to trigger a flush.
	LevelsTargetSizeMultiplier int
	FlushIntervalMs            int // Interval for periodic index memtable flushes.
	CompactionIntervalSeconds  int // Interval to check for index compaction.
	CompactionFallbackStrategy levels.CompactionFallbackStrategy
	L0CompactionTriggerSize    int64
	MaxL0Files                 int // Number of L0 index files to trigger compaction.
	MaxLevels                  int
	BaseTargetSize             int64
	CompactionTombstoneWeight  float64     // New: Weight for tombstone score
	CompactionOverlapWeight    float64     // New: Weight for overlap penalty
	Clock                      clock.Clock // Clock interface for time measurement, allows mocking in tests.
	// EnableSSTablePreallocate instructs writers for index SSTables to attempt
	// preallocation when creating files. Best-effort; failures are non-fatal.
	EnableSSTablePreallocate bool
	// SSTableRestartPointInterval sets the restart-point frequency for index
	// sstable writers (number of entries per restart point). If zero, writer
	// defaults are used.
	SSTableRestartPointInterval int
	// SSTablePreallocMultiplier controls bytes-per-estimated-key used when
	// preallocating index SSTable files. If zero, the writer default is used.
	SSTablePreallocMultiplier int
}

// TagIndexManager manages the LSM-tree for the secondary tag index.
type TagIndexManager struct {
	opts                  TagIndexManagerOptions
	mu                    sync.RWMutex
	memtable              *IndexMemtable
	immutableMemtables    []*IndexMemtable
	sstDir                string
	sstableWriterFactory  core.SSTableWriterFactory
	levelsManager         levels.Manager // Manages Index SSTables
	manifestPath          string
	flushChan             chan struct{}
	compactionChan        chan struct{}
	l0CompactionActive    atomic.Bool
	lnCompactionSemaphore chan struct{} // Limits concurrent LN compactions
	blockReadSemaphore    chan struct{} // NEW: Limits concurrent block reads during index compaction
	shutdownChan          chan struct{}
	wg                    sync.WaitGroup
	logger                *slog.Logger
	deps                  *TagIndexDependencies
	tracer                trace.Tracer
	clock                 clock.Clock // Clock interface for time measurement, allows mocking in tests.
}

// Instrumentation for debugging creation/stop leaks of TagIndexManager.
var (
	tagIndexManagerActiveCount atomic.Int64
	tagIndexManagerCreatesMu   sync.Mutex
	tagIndexManagerCreates     []string
	tagIndexManagerStopsMu     sync.Mutex
	tagIndexManagerStops       []string
	// track instances for test-only best-effort shutdown
	tagIndexManagerInstancesMu sync.Mutex
	tagIndexManagerInstances   []*TagIndexManager
)

// GetActiveTagIndexManagerCount returns the number of currently-active
// TagIndexManager instances (instrumentation helper).
func GetActiveTagIndexManagerCount() int64 {
	return tagIndexManagerActiveCount.Load()
}

// DumpTagIndexManagerInstrumentation returns current active count and copies of
// the collected creation and stop stack traces. Useful for test diagnostics.
func DumpTagIndexManagerInstrumentation() (int64, []string, []string) {
	cnt := tagIndexManagerActiveCount.Load()
	tagIndexManagerCreatesMu.Lock()
	creates := make([]string, len(tagIndexManagerCreates))
	copy(creates, tagIndexManagerCreates)
	tagIndexManagerCreatesMu.Unlock()
	tagIndexManagerStopsMu.Lock()
	stops := make([]string, len(tagIndexManagerStops))
	copy(stops, tagIndexManagerStops)
	tagIndexManagerStopsMu.Unlock()
	return cnt, creates, stops
}

// NewTagIndexManager creates a new manager for the tag index.
func NewTagIndexManager(opts TagIndexManagerOptions, deps *TagIndexDependencies, logger *slog.Logger, tracer trace.Tracer) (*TagIndexManager, error) {
	var clk clock.Clock
	if opts.Clock == nil {
		clk = clock.SystemClockDefault
	} else {
		clk = opts.Clock
	}
	indexSstDir := filepath.Join(opts.DataDir, core.IndexSSTDirName)
	if err := os.MkdirAll(indexSstDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index sst directory %s: %w", indexSstDir, err)
	}
	if opts.MaxLevels <= 0 {
		opts.MaxLevels = 7 // Default value
	}

	if opts.BaseTargetSize <= 0 {
		opts.BaseTargetSize = 16 * 1024 * 1024 // 16MB default
	}

	// Set reasonable defaults for critical options if they are not provided.
	if opts.MemtableThreshold <= 0 {
		// Default to flushing after 4096 new series have been indexed.
		opts.MemtableThreshold = 4096
	}
	if opts.MaxL0Files <= 0 {
		// Default to triggering compaction when 4 L0 files accumulate.
		opts.MaxL0Files = 4
	}

	// Set a reasonable default if not provided
	if opts.LevelsTargetSizeMultiplier <= 0 {
		opts.LevelsTargetSizeMultiplier = 5
	}

	lm, err := levels.NewLevelsManager(
		opts.MaxLevels,
		opts.MaxL0Files,
		opts.BaseTargetSize,
		tracer,
		opts.CompactionFallbackStrategy,
		opts.CompactionTombstoneWeight,
		opts.CompactionOverlapWeight,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create levels manager for tag index: %w", err)
	}

	tim := &TagIndexManager{
		opts:     opts,
		memtable: NewIndexMemtable(),
		sstDir:   indexSstDir,
		// Provide a factory for index SSTables, using the provided SysFileOpener
		// It's crucial for the index SSTables to use the correct SysFileOpener.
		sstableWriterFactory:  defaultIndexSSTableWriterFactory(tracer, logger),
		levelsManager:         lm,
		flushChan:             make(chan struct{}, 1),
		compactionChan:        make(chan struct{}, 1),
		lnCompactionSemaphore: make(chan struct{}, 1), // Default to 1 concurrent LN compaction for the index
		blockReadSemaphore:    make(chan struct{}, runtime.NumCPU()),
		shutdownChan:          make(chan struct{}),
		logger:                logger.With("component", "TagIndexManager"),
		tracer:                tracer,
		deps:                  deps,
		manifestPath:          filepath.Join(indexSstDir, core.IndexManifestFileName),
		clock:                 clk,
	}

	// Instrumentation: record creation stack and increment active count.
	tagIndexManagerActiveCount.Add(1)
	// record instance for test-time diagnostics and best-effort shutdown
	tagIndexManagerInstancesMu.Lock()
	tagIndexManagerInstances = append(tagIndexManagerInstances, tim)
	tagIndexManagerInstancesMu.Unlock()
	if logger != nil {
		logger.Info("TagIndexManager created", "active_count", tagIndexManagerActiveCount.Load())
	}
	// capture a short stack trace to help identify the creator
	buf := make([]byte, 1<<12)
	n := runtime.Stack(buf, false)
	stackStr := string(buf[:n])
	tagIndexManagerCreatesMu.Lock()
	tagIndexManagerCreates = append(tagIndexManagerCreates, fmt.Sprintf("%s\n%s", time.Now().Format(time.RFC3339Nano), stackStr))
	tagIndexManagerCreatesMu.Unlock()

	// Load existing state from the index manifest
	if err := tim.LoadFromFile(opts.DataDir); err != nil {
		// Close the levels manager if loading fails to release any file handles
		lm.Close()
		// decrement active count on failure to construct
		tagIndexManagerActiveCount.Add(-1)
		return nil, fmt.Errorf("failed to load tag index state from disk: %w", err)
	}

	return tim, nil
}

// StopAllTagIndexManagersForTest invokes Stop() on all created TagIndexManager
// instances on a best-effort basis. This is intended for use by tests that
// simulate crashes and need to ensure background goroutines are not leaked
// across test boundaries. It is safe to call multiple times.
func StopAllTagIndexManagersForTest() {
	tagIndexManagerInstancesMu.Lock()
	instances := make([]*TagIndexManager, len(tagIndexManagerInstances))
	copy(instances, tagIndexManagerInstances)
	// clear the slice so subsequent calls don't repeatedly stop the same ones
	tagIndexManagerInstances = nil
	tagIndexManagerInstancesMu.Unlock()

	for _, inst := range instances {
		if inst == nil {
			continue
		}
		// Best-effort stop; recover from any panics during shutdown to avoid
		// flaking the test helper itself.
		func(tim *TagIndexManager) {
			defer func() {
				_ = recover()
			}()
			tim.Stop()
		}(inst)
	}
}

// Private
func (tim *TagIndexManager) GetShutdownChain() chan struct{} {
	return tim.shutdownChan
}

func (tim *TagIndexManager) GetWaitGroup() *sync.WaitGroup {
	return &tim.wg
}

func (tim *TagIndexManager) GetLevelsManager() levels.Manager {
	return tim.levelsManager
}

// Add updates the index with a new series and its tags.
func (tim *TagIndexManager) Add(seriesID uint64, tags map[string]string) error {
	tim.mu.RLock()
	mem := tim.memtable
	tim.mu.RUnlock()

	// This is the entry point from StorageEngine.PutDataPoint
	for tagKey, tagValue := range tags {
		// It's assumed that the stringStore has already been updated by the caller (PutDataPoint)
		tagKeyID, ok := tim.deps.StringStore.GetID(tagKey)
		if !ok {
			tim.logger.Warn("Could not find ID for tag key during index update", "tagKey", tagKey)
			continue
		}
		tagValueID, ok := tim.deps.StringStore.GetID(tagValue)
		if !ok {
			tim.logger.Warn("Could not find ID for tag value during index update", "tagValue", tagValue)
			continue
		}
		mem.Add(tagKeyID, tagValueID, seriesID)
	}

	tim.checkAndRotateMemtable(mem)
	return nil
}

// Start begins the background processes for the index manager.
func (tim *TagIndexManager) Start() {
	tim.logger.Info("Starting tag index manager background processes...")
	tim.wg.Add(2) // One for flush, one for compaction
	go tim.flushLoop()
	go tim.compactionLoop()
}

// Stop gracefully shuts down the index manager.
func (tim *TagIndexManager) Stop() {
	tim.logger.Info("Stopping tag index manager...")
	// This check prevents closing a channel twice, which would panic.
	select {
	case <-tim.shutdownChan:
		// Already closed.
	default:
		close(tim.shutdownChan)
	}
	tim.wg.Wait()
	// Flush final memtable
	tim.flushFinalMemtable()
	// Close the levels manager to release file handles of index SSTables.
	if err := tim.levelsManager.Close(); err != nil {
		tim.logger.Error("Failed to close index levels manager", "error", err)
	}
	// Instrumentation: decrement active count and record stop stack
	tagIndexManagerActiveCount.Add(-1)
	if tim.logger != nil {
		tim.logger.Info("TagIndexManager stopped", "active_count", tagIndexManagerActiveCount.Load())
	}
	stopBuf := make([]byte, 1<<12)
	sn := runtime.Stack(stopBuf, false)
	stopStack := string(stopBuf[:sn])
	tagIndexManagerStopsMu.Lock()
	tagIndexManagerStops = append(tagIndexManagerStops, fmt.Sprintf("%s\n%s", time.Now().Format(time.RFC3339Nano), stopStack))
	tagIndexManagerStopsMu.Unlock()
	tim.logger.Info("Tag index manager stopped.")
}

// Query finds series IDs matching the given tags.
// This is a placeholder and will need to query memtable and SSTables.
func (tim *TagIndexManager) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	if len(tags) == 0 {
		// Returning none is safer and more consistent with filtering.
		return roaring64.New(), nil
	}

	var resultBitmap *roaring64.Bitmap
	firstTag := true

	for tagKey, tagValue := range tags {
		tagKeyID, ok1 := tim.deps.StringStore.GetID(tagKey)
		tagValueID, ok2 := tim.deps.StringStore.GetID(tagValue)
		if !ok1 || !ok2 {
			// If any part of the tag doesn't exist in the store, no series can match.
			return roaring64.New(), nil
		}

		// Get the complete bitmap for this tag from all sources (memtables + sstables)
		currentTagBitmap, err := tim.getBitmapForTag(tagKeyID, tagValueID)
		if err != nil {
			return nil, fmt.Errorf("failed to get bitmap for tag %s=%s: %w", tagKey, tagValue, err)
		}

		if currentTagBitmap.IsEmpty() {
			// If any tag results in an empty set, the final intersection will be empty.
			return roaring64.New(), nil
		}

		if firstTag {
			resultBitmap = currentTagBitmap // No need to clone, it's a new bitmap from the helper.
			firstTag = false
		} else {
			resultBitmap.And(currentTagBitmap)
		}

		if resultBitmap.IsEmpty() {
			// Optimization: if intersection becomes empty, no need to continue.
			return resultBitmap, nil
		}
	}

	if resultBitmap == nil {
		// This happens if the tags map was not empty but the loop didn't run, which is impossible.
		// Return an empty bitmap as a safe default.
		return roaring64.New(), nil
	}

	return resultBitmap, nil
}

// getBitmapForTag finds the complete roaring bitmap for a single tag key-value pair
// by merging results from the memtables and all SSTable levels.
func (tim *TagIndexManager) getBitmapForTag(tagKeyID, tagValueID uint64) (*roaring64.Bitmap, error) {
	mergedBitmap := roaring64.New()
	indexKey := EncodeTagIndexKey(tagKeyID, tagValueID)

	// 1. Check memtables (mutable and immutable)
	tim.mu.RLock()
	mutableBitmap := tim.memtable.GetBitmap(tagKeyID, tagValueID)
	mergedBitmap.Or(mutableBitmap)

	for _, im := range tim.immutableMemtables {
		immutableBitmap := im.GetBitmap(tagKeyID, tagValueID)
		mergedBitmap.Or(immutableBitmap)
	}
	tim.mu.RUnlock()

	// 2. Check SSTables
	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()

	// Search from newest to oldest data (L0 -> L1 -> ...)
	for levelIdx := 0; levelIdx < len(levelStates); levelIdx++ {
		levelTables := levelStates[levelIdx].GetTables()
		for _, table := range levelTables {
			// A simple optimization: if the key is outside the table's range, skip it.
			if bytes.Compare(indexKey, table.MinKey()) < 0 || bytes.Compare(indexKey, table.MaxKey()) > 0 {
				continue
			}

			value, entryType, err := table.Get(indexKey)
			if err == nil && entryType == core.EntryTypePutEvent {
				// Found it. Deserialize and merge.
				tempBitmap := roaring64.New()
				if _, err := tempBitmap.ReadFrom(bytes.NewReader(value)); err == nil {
					mergedBitmap.Or(tempBitmap)
				} else {
					tim.logger.Warn("Failed to deserialize bitmap from index sstable", "key", indexKey, "table_id", table.ID(), "error", err)
				}
			} else if err != nil && err != sstable.ErrNotFound {
				return nil, fmt.Errorf("error getting key from index sstable %d: %w", table.ID(), err)
			}
		}
	}

	return mergedBitmap, nil
}

func (tim *TagIndexManager) compactionLoop() {
	defer tim.wg.Done()

	interval := time.Duration(tim.opts.CompactionIntervalSeconds) * time.Second
	if tim.opts.CompactionIntervalSeconds <= 0 {
		tim.logger.Warn("Invalid IndexCompactionIntervalSeconds, defaulting to 30 seconds.", "interval_seconds", tim.opts.CompactionIntervalSeconds)
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tim.performIndexCompactionCycle()
		case <-tim.compactionChan:
			tim.performIndexCompactionCycle()
		case <-tim.shutdownChan:
			tim.logger.Info("Index compaction loop shutting down.")
			return
		}
	}
}

func (tim *TagIndexManager) performIndexCompactionCycle() {
	if tim.levelsManager.NeedsL0Compaction(tim.opts.MaxL0Files, tim.opts.L0CompactionTriggerSize) {
		if tim.l0CompactionActive.CompareAndSwap(false, true) {
			tim.logger.Info("Starting index L0->L1 compaction.")

			if err := tim.compactIndexL0ToL1(); err != nil {
				tim.logger.Error("Index L0->L1 compaction failed", "error", err)
			} else {
				tim.logger.Info("Index L0->L1 compaction finished successfully.")
			}
			tim.l0CompactionActive.Store(false)
		} else {
			tim.logger.Info("Skipping index L0 compaction as one is already active.")
		}
	}

	for levelN := 1; levelN < tim.levelsManager.MaxLevels()-1; levelN++ {
		if tim.levelsManager.NeedsLevelNCompaction(levelN, tim.opts.LevelsTargetSizeMultiplier) {
			select {
			case tim.lnCompactionSemaphore <- struct{}{}:
				tim.wg.Add(1)
				tim.logger.Info("Starting index LN->LN+1 compaction.", "level", levelN)
				go func(lvl int) {
					defer func() {
						<-tim.lnCompactionSemaphore
						tim.wg.Done()
					}()
					if err := tim.compactIndexLevelNToLevelNPlus1(lvl); err != nil {
						tim.logger.Error("Index LN->LN+1 compaction failed", "level", lvl, "error", err)
					} else {
						tim.logger.Info("Index LN->LN+1 compaction finished successfully.", "level", lvl)
					}
				}(levelN)
			default:
				tim.logger.Info("Skipping index LN compaction due to concurrency limit.", "level", levelN)
			}
		}
	}
}

func (tim *TagIndexManager) compactIndexLevelNToLevelNPlus1(levelN int) error {
	// Pick a candidate table from levelN
	tableToCompact := tim.levelsManager.PickCompactionCandidateForLevelN(levelN)
	if tableToCompact == nil {
		return nil // Nothing to compact
	}

	// Find overlapping tables in levelN+1
	minKey, maxKey := tableToCompact.MinKey(), tableToCompact.MaxKey()
	overlappingTables := tim.levelsManager.GetOverlappingTables(levelN+1, minKey, maxKey)

	inputTables := append([]*sstable.SSTable{tableToCompact}, overlappingTables...)

	// Get deleted series snapshot
	tim.deps.DeletedSeriesMu.RLock()
	deletedSeriesSnapshot := make(map[string]uint64, len(tim.deps.DeletedSeries))
	for k, v := range tim.deps.DeletedSeries {
		deletedSeriesSnapshot[k] = v
	}
	tim.deps.DeletedSeriesMu.RUnlock()

	// Merge them
	newTables, err := tim.mergeIndexSSTables(inputTables, deletedSeriesSnapshot)
	if err != nil {
		return fmt.Errorf("failed to merge index sstables for L%d->L%d: %w", levelN, levelN+1, err)
	}

	// Apply results
	if err := tim.levelsManager.ApplyCompactionResults(levelN, levelN+1, newTables, inputTables); err != nil {
		// Cleanup new files if apply fails
		for _, sst := range newTables {
			sys.Remove(sst.FilePath())
		}
		return fmt.Errorf("failed to apply index compaction results for L%d->L%d: %w", levelN, levelN+1, err)
	}

	// Persist manifest
	tim.mu.Lock()
	if err := tim.persistIndexManifestLocked(); err != nil {
		tim.logger.Error("CRITICAL: Failed to persist index manifest after LN compaction.", "error", err)
	}
	tim.mu.Unlock()

	// Cleanup old files
	for _, oldTable := range inputTables {
		if err := oldTable.Close(); err != nil {
			// Ignore benign already-closed sentinel; otherwise log the error.
			if err != sstable.ErrClosed {
				tim.logger.Error("Failed to close old index sstable after LN compaction", "path", oldTable.FilePath(), "error", err)
			}
		}
		if err := sys.Remove(oldTable.FilePath()); err != nil {
			tim.logger.Error("Failed to remove old index sstable after LN compaction", "path", oldTable.FilePath(), "error", err)
		}
	}

	return nil
}

func (tim *TagIndexManager) compactIndexL0ToL1() error {
	l0Tables := tim.levelsManager.GetTablesForLevel(0)
	if len(l0Tables) == 0 {
		return nil
	}

	// Get a snapshot of deleted series to pass to the merge function
	// This ensures we have a consistent view of deletions for this compaction run.
	tim.deps.DeletedSeriesMu.RLock()
	deletedSeriesSnapshot := make(map[string]uint64, len(tim.deps.DeletedSeries))
	for k, v := range tim.deps.DeletedSeries {
		deletedSeriesSnapshot[k] = v
	}
	tim.deps.DeletedSeriesMu.RUnlock()

	// For index L0->L1 compaction, we merge all L0 tables.
	// A more advanced strategy would also include overlapping L1 tables.
	newSSTs, err := tim.mergeIndexSSTables(l0Tables, deletedSeriesSnapshot)
	if err != nil {
		return fmt.Errorf("failed to merge index L0 sstables: %w", err)
	}

	// Atomically update the levels manager
	if err := tim.levelsManager.ApplyCompactionResults(0, 1, newSSTs, l0Tables); err != nil {
		// If applying results fails, we need to clean up the newly created files.
		for _, sst := range newSSTs {
			sys.Remove(sst.FilePath())
		}
		return fmt.Errorf("failed to apply index compaction results: %w", err)
	}

	// Persist manifest after successfully applying compaction results.
	tim.mu.Lock()
	if err := tim.persistIndexManifestLocked(); err != nil {
		tim.logger.Error("CRITICAL: Failed to persist index manifest after compaction. Index state may be inconsistent on next startup.", "error", err)
	}
	tim.mu.Unlock()

	// Cleanup old L0 files.
	for _, oldTable := range l0Tables {
		if err := oldTable.Close(); err != nil {
			if err != sstable.ErrClosed {
				tim.logger.Error("Failed to close old index L0 sstable", "path", oldTable.FilePath(), "error", err)
			}
		}
		if err := sys.Remove(oldTable.FilePath()); err != nil {
			tim.logger.Error("Failed to remove old index L0 sstable", "path", oldTable.FilePath(), "error", err)
		}
	}

	return nil
}

func (tim *TagIndexManager) mergeIndexSSTables(tables []*sstable.SSTable, deletedSeries map[string]uint64) ([]*sstable.SSTable, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, table := range tables {
		iter, err := table.NewIterator(nil, nil, tim.blockReadSemaphore, types.Ascending)
		if err != nil {
			for _, openedIter := range iters {
				openedIter.Close()
			}
			return nil, fmt.Errorf("failed to create iterator for index table %d: %w", table.ID(), err)
		}
		iters = append(iters, iter)
	}

	heap := iterator.NewMinHeap(iters)
	if heap.Len() == 0 {
		return nil, nil // All iterators were empty
	}

	var newSSTs []*sstable.SSTable
	writer, fileID, err := tim.createNewIndexWriter()
	if err != nil {
		return nil, err
	}

	currentKey := make([]byte, len(heap.Key()))
	copy(currentKey, heap.Key())
	mergedBitmap := roaring64.New()

	for heap.Len() > 0 {
		key := heap.Key()
		value := heap.Value()

		if !bytes.Equal(key, currentKey) {
			if err := tim.writeCleanBitmap(writer, currentKey, mergedBitmap, deletedSeries); err != nil {
				writer.Abort()
				return nil, err
			}
			copy(currentKey, key)
			mergedBitmap.Clear()
		}

		tempBitmap := roaring64.New()
		if _, err := tempBitmap.ReadFrom(bytes.NewReader(value)); err != nil {
			tim.logger.Warn("Skipping corrupted bitmap in index sstable", "key", key, "error", err)
		} else {
			mergedBitmap.Or(tempBitmap)
		}

		heap.Next()
	}

	if err := tim.writeCleanBitmap(writer, currentKey, mergedBitmap, deletedSeries); err != nil {
		writer.Abort()
		return nil, err
	}
	if err := writer.Finish(); err != nil {
		writer.Abort()
		return nil, fmt.Errorf("failed to finish index sstable writer: %w", err)
	}

	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: fileID, Tracer: tim.tracer, Logger: tim.logger}
	newSST, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		sys.Remove(writer.FilePath())
		return nil, fmt.Errorf("failed to load newly created index sstable %s: %w", writer.FilePath(), err)
	}
	newSSTs = append(newSSTs, newSST)

	return newSSTs, nil
}

// writeCleanBitmap filters deleted series IDs from a merged bitmap and writes the result to the SSTable writer.
func (tim *TagIndexManager) writeCleanBitmap(writer core.SSTableWriterInterface, key []byte, bitmap *roaring64.Bitmap, deletedSeries map[string]uint64) error {
	if bitmap.IsEmpty() {
		return nil
	}

	cleanBitmap := roaring64.New()
	iterator := bitmap.Iterator()
	for iterator.HasNext() {
		seriesID := iterator.Next()
		// The seriesID from the bitmap is now uint64.
		seriesKeyStr, found := tim.deps.SeriesIDStore.GetKey(seriesID)
		if !found {
			tim.logger.Warn("SeriesID found in index but not in seriesIDStore during compaction", "seriesID", seriesID)
			continue
		}
		if _, isDeleted := deletedSeries[seriesKeyStr]; !isDeleted {
			cleanBitmap.Add(seriesID)
		}
	}

	// Only write if the bitmap is not empty after cleaning.
	if !cleanBitmap.IsEmpty() {
		serializedBitmap, err := cleanBitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize cleaned bitmap for key %v: %w", key, err)
		}
		if err := writer.Add(key, serializedBitmap, core.EntryTypePutEvent, 0); err != nil {
			return fmt.Errorf("failed to write cleaned bitmap to index sstable: %w", err)
		}
	}
	return nil
}

func (tim *TagIndexManager) flushLoop() {
	defer tim.wg.Done()

	var flushTicker *time.Ticker
	var tickerChan <-chan time.Time

	if tim.opts.FlushIntervalMs > 0 {
		interval := time.Duration(tim.opts.FlushIntervalMs) * time.Millisecond
		flushTicker = time.NewTicker(interval)
		tickerChan = flushTicker.C
		tim.logger.Info("Periodic index memtable flush enabled.", "interval", interval)
		defer flushTicker.Stop()
	}

	for {
		select {
		case <-tim.flushChan:
			tim.processImmutableIndexMemtables()
		case <-tickerChan:
			tim.triggerPeriodicFlush()
		case <-tim.shutdownChan:
			tim.logger.Info("Index flush loop shutting down.")
			return
		}
	}
}

func (tim *TagIndexManager) triggerPeriodicFlush() {
	tim.mu.Lock()
	if tim.memtable != nil && tim.memtable.Size() > 0 && len(tim.immutableMemtables) == 0 {
		tim.logger.Info("Triggering periodic index memtable flush.", "size_bitmaps", tim.memtable.Size())
		tim.immutableMemtables = append(tim.immutableMemtables, tim.memtable)
		tim.memtable = NewIndexMemtable()
		tim.mu.Unlock()

		select {
		case tim.flushChan <- struct{}{}:
		default:
		}
	} else {
		tim.mu.Unlock()
	}
}

func (tim *TagIndexManager) processImmutableIndexMemtables() {
	tim.mu.Lock()
	if len(tim.immutableMemtables) == 0 {
		tim.mu.Unlock()
		return
	}
	memToFlush := tim.immutableMemtables[0]
	tim.immutableMemtables = tim.immutableMemtables[1:]
	tim.mu.Unlock()

	if memToFlush == nil {
		return
	}

	newSST, err := tim.flushIndexMemtableToSSTable(memToFlush)
	if err != nil {
		// TODO: Implement retry logic and DLQ for index memtables, similar to the main engine.
		tim.logger.Error("Failed to flush index memtable", "error", err)
		return
	}

	if newSST != nil {
		tim.mu.Lock()
		tim.levelsManager.AddL0Table(newSST)
		tim.persistIndexManifestLocked() // Persist state after adding the new table
		tim.mu.Unlock()

		// After a successful flush, check if compaction is needed.
		tim.mu.RLock()
		needsCompaction := tim.levelsManager.NeedsL0Compaction(tim.opts.MaxL0Files, tim.opts.L0CompactionTriggerSize)
		tim.mu.RUnlock()
		if needsCompaction {
			tim.signalCompaction()
		}
	}
}

func (tim *TagIndexManager) flushIndexMemtableToSSTable(memToFlush *IndexMemtable) (*sstable.SSTable, error) {
	if memToFlush == nil || memToFlush.Size() == 0 {
		return nil, nil
	}

	writer, fileID, err := tim.createNewIndexWriter()
	memToFlush.mu.RLock()
	defer memToFlush.mu.RUnlock()

	// To ensure sorted order for SSTable, we need to iterate keys deterministically.
	sortedTagKeyIDs := make([]uint64, 0, len(memToFlush.index))
	for k := range memToFlush.index {
		sortedTagKeyIDs = append(sortedTagKeyIDs, k)
	}
	sort.Slice(sortedTagKeyIDs, func(i, j int) bool { return sortedTagKeyIDs[i] < sortedTagKeyIDs[j] })

	for _, tagKeyID := range sortedTagKeyIDs {
		tagValueMap := memToFlush.index[tagKeyID]
		sortedTagValueIDs := make([]uint64, 0, len(tagValueMap))
		for v := range tagValueMap {
			sortedTagValueIDs = append(sortedTagValueIDs, v)
		}
		sort.Slice(sortedTagValueIDs, func(i, j int) bool { return sortedTagValueIDs[i] < sortedTagValueIDs[j] })

		for _, tagValueID := range sortedTagValueIDs {
			bitmap := tagValueMap[tagValueID]
			key := EncodeTagIndexKey(tagKeyID, tagValueID)
			value, err := bitmap.ToBytes()
			if err != nil {
				writer.Abort()
				return nil, fmt.Errorf("failed to serialize roaring bitmap for tag %d-%d: %w", tagKeyID, tagValueID, err)
			}
			// Use EntryTypePut for index entries.
			if err := writer.Add(key, value, core.EntryTypePutEvent, 0); err != nil {
				writer.Abort()
				return nil, fmt.Errorf("failed to add index entry to SSTable writer: %w", err)
			}
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		return nil, fmt.Errorf("failed to finish index SSTable: %w", err)
	}

	loadOpts := sstable.LoadSSTableOptions{
		FilePath:   writer.FilePath(),
		ID:         fileID,
		BlockCache: nil, // Index SSTables might not need a block cache, or could have their own. For now, nil.
		Tracer:     tim.tracer,
		Logger:     tim.logger,
	}
	newSST, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		sys.Remove(writer.FilePath()) // Best effort cleanup
		return nil, fmt.Errorf("failed to load newly created index SSTable %s: %w", writer.FilePath(), err)
	}
	tim.logger.Info("Successfully flushed and loaded index SSTable.", "path", writer.FilePath(), "id", fileID)
	return newSST, nil
}

func (tim *TagIndexManager) signalCompaction() {
	select {
	case tim.compactionChan <- struct{}{}:
		// Signal sent
	default:
		// Compaction is already signaled or busy, do nothing.
	}
}

func (tim *TagIndexManager) flushFinalMemtable() {
	tim.mu.Lock()
	var allMemsToFlush []*IndexMemtable
	allMemsToFlush = append(allMemsToFlush, tim.immutableMemtables...)
	if tim.memtable != nil && tim.memtable.Size() > 0 {
		allMemsToFlush = append(allMemsToFlush, tim.memtable)
	}
	// Clear the memtables from the manager state now that we have them locally.
	tim.immutableMemtables = nil
	tim.memtable = NewIndexMemtable()
	tim.mu.Unlock()

	if len(allMemsToFlush) == 0 {
		return
	}

	var newSSTs []*sstable.SSTable
	for _, mem := range allMemsToFlush {
		newSST, err := tim.flushIndexMemtableToSSTable(mem)
		if err != nil {
			tim.logger.Error("Failed to flush index memtable during shutdown", "error", err)
			continue // Try to flush the next one
		}
		if newSST != nil {
			newSSTs = append(newSSTs, newSST)
		}
	}

	// After all flushes are done, acquire lock to update levels and manifest
	if len(newSSTs) > 0 {
		tim.mu.Lock()
		defer tim.mu.Unlock()
		for _, sst := range newSSTs {
			tim.levelsManager.AddL0Table(sst)
		}
		tim.persistIndexManifestLocked()
	}
}

func (tim *TagIndexManager) createNewIndexWriter() (core.SSTableWriterInterface, uint64, error) {
	fileID := tim.deps.SSTNextID()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tim.sstDir,
		ID:                           fileID,
		EstimatedKeys:                1000,     // A reasonable default estimate
		BloomFilterFalsePositiveRate: 0.01,     // Can be tuned for index
		BlockSize:                    4 * 1024, // Smaller blocks might be better for index
		Tracer:                       tim.tracer,
		Compressor:                   &compressors.NoCompressionCompressor{}, // Bitmaps are already compressed
		Logger:                       tim.logger,
	}
	// Propagate optional tuning from manager options
	writerOpts.Preallocate = tim.opts.EnableSSTablePreallocate
	writerOpts.RestartPointInterval = tim.opts.SSTableRestartPointInterval
	writerOpts.PreallocMultiplier = tim.opts.SSTablePreallocMultiplier
	writer, err := tim.sstableWriterFactory(writerOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create index SSTable writer: %w", err)
	}
	return writer, fileID, nil
}

// persistIndexManifest saves the current state of the index's levels manager to disk.
func (tim *TagIndexManager) persistIndexManifestLocked() error {
	manifest := core.SnapshotManifest{ // Reusing the main manifest struct for simplicity
		Levels: make([]core.SnapshotLevelManifest, 0, tim.levelsManager.MaxLevels()),
	}

	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()

	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		if len(tablesInLevel) == 0 {
			continue
		}
		levelManifest := core.SnapshotLevelManifest{
			LevelNumber: levelNum,
			Tables:      make([]core.SSTableMetadata, 0, len(tablesInLevel)),
		}
		for _, table := range tablesInLevel {
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
				ID:       table.ID(),
				FileName: filepath.Base(table.FilePath()), // Store only the base name
				MinKey:   table.MinKey(),
				MaxKey:   table.MaxKey(),
			})
		}
		manifest.Levels = append(manifest.Levels, levelManifest)
	}

	if err := PersistIndexManifest(tim.manifestPath, manifest); err != nil {
		return err
	}

	tim.logger.Debug("Index manifest persisted successfully.")
	return nil
}

// CreateSnapshot creates a snapshot of the tag index's current state into the given directory.
func (tim *TagIndexManager) CreateSnapshot(snapshotDir string) error {
	tim.logger.Info("Creating tag index snapshot.", "snapshot_dir", snapshotDir)

	// 1. Flush any in-memory data to ensure disk state is up-to-date.
	// This is a synchronous operation for the snapshot.
	tim.flushFinalMemtable()

	// 2. Get a consistent view of the levels.
	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()

	// 3. Create the manifest for the snapshot.
	manifest := core.SnapshotManifest{
		Levels: make([]core.SnapshotLevelManifest, 0, len(levelStates)),
	}

	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		if len(tablesInLevel) == 0 {
			continue
		}
		levelManifest := core.SnapshotLevelManifest{
			LevelNumber: levelNum,
			Tables:      make([]core.SSTableMetadata, 0, len(tablesInLevel)),
		}
		for _, table := range tablesInLevel {
			baseFileName := filepath.Base(table.FilePath())
			destPath := filepath.Join(snapshotDir, baseFileName)

			// Copy the SSTable file to the snapshot directory.
			if err := core.CopyFile(table.FilePath(), destPath); err != nil {
				return fmt.Errorf("failed to copy index SSTable %s to snapshot: %w", table.FilePath(), err)
			}
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
				ID:       table.ID(),
				FileName: baseFileName, // Store only the base name
				MinKey:   table.MinKey(),
				MaxKey:   table.MaxKey(),
			})
		}
		manifest.Levels = append(manifest.Levels, levelManifest)
	}

	// 4. Write the manifest file for the index snapshot.
	manifestPath := filepath.Join(snapshotDir, core.IndexManifestFileName)
	file, err := os.Create(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to create index snapshot manifest file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		return fmt.Errorf("failed to encode index snapshot manifest: %w", err)
	}

	tim.logger.Info("Tag index snapshot created successfully.")
	return nil
}

// RestoreFromSnapshot restores the tag index state from a snapshot directory.
// It expects the target directory (tim.sstDir) to be clean.
func (tim *TagIndexManager) RestoreFromSnapshot(snapshotDir string) error {
	tim.logger.Info("Restoring tag index from snapshot.", "snapshot_dir", snapshotDir)

	// 1. Read the manifest from the snapshot directory.
	manifestPath := filepath.Join(snapshotDir, core.IndexManifestFileName)
	file, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			tim.logger.Warn("No index manifest found in snapshot, skipping index restore.", "path", manifestPath)
			return nil // Not an error if the index didn't exist in the snapshot.
		}
		return fmt.Errorf("failed to open index snapshot manifest: %w", err)
	}
	defer file.Close()

	var manifest core.SnapshotManifest
	if err := json.NewDecoder(file).Decode(&manifest); err != nil {
		return fmt.Errorf("failed to decode index snapshot manifest: %w", err)
	}

	// 2. Copy all SSTable files listed in the manifest to the target directory.
	for _, level := range manifest.Levels {
		for _, tableMeta := range level.Tables {
			srcPath := filepath.Join(snapshotDir, tableMeta.FileName)
			destPath := filepath.Join(tim.sstDir, tableMeta.FileName)
			if err := core.CopyFile(srcPath, destPath); err != nil {
				return fmt.Errorf("failed to restore index SSTable %s: %w", srcPath, err)
			}
		}
	}

	// 3. Copy the manifest file itself.
	if err := core.CopyFile(manifestPath, tim.manifestPath); err != nil {
		return fmt.Errorf("failed to restore index manifest file: %w", err)
	}

	tim.logger.Info("Tag index restored successfully.")
	return nil
}

// LoadFromFile loads the index's state (SSTables) from its manifest file in the given data directory.
// This method is crucial for the engine's fallback recovery mechanism.
func (tim *TagIndexManager) LoadFromFile(dataDir string) error {
	indexSstDir := filepath.Join(dataDir, core.IndexSSTDirName)
	manifestPath := filepath.Join(indexSstDir, core.IndexManifestFileName)
	manifest, err := LoadIndexManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to load index manifest: %w", err)
	}
	if manifest == nil {
		tim.logger.Info("Index manifest not found, starting with a fresh index state.", "path", manifestPath)
		return nil
	}

	tim.logger.Info("Loading index SSTables from manifest.", "path", manifestPath)
	for _, levelManifest := range manifest.Levels {
		for _, tableMeta := range levelManifest.Tables {
			filePath := filepath.Join(indexSstDir, tableMeta.FileName)
			loadOpts := sstable.LoadSSTableOptions{
				FilePath: filePath,
				ID:       tableMeta.ID,
				Tracer:   tim.tracer,
				Logger:   tim.logger,
			}
			table, loadErr := sstable.LoadSSTable(loadOpts)
			if loadErr != nil {
				tim.logger.Error("Failed to load index SSTable from manifest, skipping.", "file", filePath, "id", tableMeta.ID, "error", loadErr)
				continue // Skip corrupted tables
			}
			if err := tim.levelsManager.AddTableToLevel(levelManifest.LevelNumber, table); err != nil {
				tim.logger.Error("Failed to add loaded index SSTable to level from manifest, skipping.", "id", table.ID(), "level", levelManifest.LevelNumber, "error", err)
				if cerr := table.Close(); cerr != nil && cerr != sstable.ErrClosed {
					tim.logger.Error("Failed to close index SSTable after add-to-level failure", "id", table.ID(), "error", cerr)
				}
			}
		}
	}
	tim.logger.Info("Index state loaded from manifest successfully.")
	return nil
}

// AddEncoded updates the index with a new series and its pre-encoded tags.
// This is an optimization for scenarios like WAL recovery where tag IDs are already known.
func (tim *TagIndexManager) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	tim.mu.RLock()
	mem := tim.memtable
	tim.mu.RUnlock()

	for _, pair := range encodedTags {
		mem.Add(pair.KeyID, pair.ValueID, seriesID)
	}
	tim.checkAndRotateMemtable(mem)
	return nil
}

// RemoveSeries removes a series from the in-memory indexes (mutable and immutable).
// This is called when a series tombstone is created to ensure immediate consistency for queries.
func (tim *TagIndexManager) RemoveSeries(seriesID uint64) {
	tim.mu.RLock()
	defer tim.mu.RUnlock()

	if tim.memtable != nil {
		tim.memtable.Remove(seriesID)
	}

	for _, im := range tim.immutableMemtables {
		if im != nil {
			im.Remove(seriesID)
		}
	}
}

// checkAndRotateMemtable checks if the current memtable is full and, if so,
// rotates it into the immutable list and signals the flush loop.
func (tim *TagIndexManager) checkAndRotateMemtable(mem *IndexMemtable) {
	if mem.Size() >= tim.opts.MemtableThreshold {
		tim.mu.Lock()
		defer tim.mu.Unlock()
		// Double-check under lock to prevent race conditions where another goroutine
		// might have already rotated it. We only rotate if the memtable we added to
		// is still the active one.
		if tim.memtable == mem && tim.memtable.Size() >= tim.opts.MemtableThreshold {
			tim.logger.Debug("Index memtable threshold reached, rotating and signaling flush.", "size", tim.memtable.Size(), "threshold", tim.opts.MemtableThreshold)
			tim.immutableMemtables = append(tim.immutableMemtables, tim.memtable)
			tim.memtable = NewIndexMemtable()
			select {
			case tim.flushChan <- struct{}{}:
			default:
			}
		}
	}
}

// defaultIndexSSTableWriterFactory creates a factory function for index SSTable writers.
func defaultIndexSSTableWriterFactory(tracer trace.Tracer, logger *slog.Logger) core.SSTableWriterFactory {
	return func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
		// Ensure SysFileOpener is set in the options for index SSTables too.
		opts.Tracer = tracer
		opts.Logger = logger.With("component", "TagIndexManager-SSTableWriter")
		if opts.BlockSize == 0 {
			opts.BlockSize = 4 * 1024 // Default to smaller blocks for index
		}
		if opts.Compressor == nil {
			opts.Compressor = &compressors.NoCompressionCompressor{} // Bitmaps are already efficient
		}
		if opts.EstimatedKeys == 0 {
			opts.EstimatedKeys = 1000 // Default for index bloom filter
		}
		return sstable.NewSSTableWriter(opts)
	}
}
