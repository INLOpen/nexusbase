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

// TagIndexManager2 is a standalone implementation of the tag index manager
// intended for use by `engine2`. It largely mirrors the behavior of the
// original TagIndexManager while keeping an independent type so engine2 can
// evolve it separately (for example to rely on the block-level index disk
// format described in `docs/index-disk-format.md`).
type TagIndexManager2 struct {
	opts                  TagIndexManagerOptions
	mu                    sync.RWMutex
	memtable              *IndexMemtable
	immutableMemtables    []*IndexMemtable
	sstDir                string
	sstableWriterFactory  core.SSTableWriterFactory
	levelsManager         levels.Manager
	manifestPath          string
	flushChan             chan struct{}
	compactionChan        chan struct{}
	l0CompactionActive    atomic.Bool
	lnCompactionSemaphore chan struct{}
	blockReadSemaphore    chan struct{}
	shutdownChan          chan struct{}
	wg                    sync.WaitGroup
	logger                *slog.Logger
	deps                  *TagIndexDependencies
	tracer                trace.Tracer
	clock                 clock.Clock
}

// Instrumentation for TagIndexManager2 instances (separate from the original manager)
var (
	tagIndexManager2ActiveCount atomic.Int64
	tagIndexManager2CreatesMu   sync.Mutex
	tagIndexManager2Creates     []string
	tagIndexManager2StopsMu     sync.Mutex
	tagIndexManager2Stops       []string
	tagIndexManager2InstancesMu sync.Mutex
	tagIndexManager2Instances   []*TagIndexManager2
)

// GetActiveTagIndexManager2Count returns the number of currently-active
// TagIndexManager2 instances (instrumentation helper).
func GetActiveTagIndexManager2Count() int64 {
	return tagIndexManager2ActiveCount.Load()
}

// DumpTagIndexManager2Instrumentation returns active count and captured traces.
func DumpTagIndexManager2Instrumentation() (int64, []string, []string) {
	cnt := tagIndexManager2ActiveCount.Load()
	tagIndexManager2CreatesMu.Lock()
	creates := make([]string, len(tagIndexManager2Creates))
	copy(creates, tagIndexManager2Creates)
	tagIndexManager2CreatesMu.Unlock()
	tagIndexManager2StopsMu.Lock()
	stops := make([]string, len(tagIndexManager2Stops))
	copy(stops, tagIndexManager2Stops)
	tagIndexManager2StopsMu.Unlock()
	return cnt, creates, stops
}

// NewTagIndexManager2 creates a new manager for the tag index.
func NewTagIndexManager2(opts TagIndexManagerOptions, deps *TagIndexDependencies, logger *slog.Logger, tracer trace.Tracer) (*TagIndexManager2, error) {
	var clk clock.Clock
	if opts.Clock == nil {
		clk = clock.SystemClockDefault
	} else {
		clk = opts.Clock
	}
	indexSstDir := filepath.Join(opts.DataDir, core.IndexSSTDirName)
	if err := os.MkdirAll(indexSstDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create index sst directory %s: %w", indexSstDir, err)
	}
	if opts.MaxLevels <= 0 {
		opts.MaxLevels = 7
	}
	if opts.BaseTargetSize <= 0 {
		opts.BaseTargetSize = 16 * 1024 * 1024
	}
	if opts.MemtableThreshold <= 0 {
		opts.MemtableThreshold = 4096
	}
	if opts.MaxL0Files <= 0 {
		opts.MaxL0Files = 4
	}
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

	tim := &TagIndexManager2{
		opts:                  opts,
		memtable:              NewIndexMemtable(),
		sstDir:                indexSstDir,
		sstableWriterFactory:  defaultIndexSSTableWriterFactory(tracer, logger),
		levelsManager:         lm,
		flushChan:             make(chan struct{}, 1),
		compactionChan:        make(chan struct{}, 1),
		lnCompactionSemaphore: make(chan struct{}, 1),
		blockReadSemaphore:    make(chan struct{}, runtime.NumCPU()),
		shutdownChan:          make(chan struct{}),
		logger:                logger.With("component", "TagIndexManager2"),
		tracer:                tracer,
		deps:                  deps,
		manifestPath:          filepath.Join(indexSstDir, core.IndexManifestFileName),
		clock:                 clk,
	}

	tagIndexManager2ActiveCount.Add(1)
	tagIndexManager2InstancesMu.Lock()
	tagIndexManager2Instances = append(tagIndexManager2Instances, tim)
	tagIndexManager2InstancesMu.Unlock()
	if logger != nil {
		logger.Info("TagIndexManager2 created", "active_count", tagIndexManager2ActiveCount.Load())
	}
	buf := make([]byte, 1<<12)
	n := runtime.Stack(buf, false)
	stackStr := string(buf[:n])
	tagIndexManager2CreatesMu.Lock()
	tagIndexManager2Creates = append(tagIndexManager2Creates, fmt.Sprintf("%s\n%s", time.Now().Format(time.RFC3339Nano), stackStr))
	tagIndexManager2CreatesMu.Unlock()

	if err := tim.LoadFromFile(opts.DataDir); err != nil {
		lm.Close()
		tagIndexManager2ActiveCount.Add(-1)
		return nil, fmt.Errorf("failed to load tag index state from disk: %w", err)
	}

	return tim, nil
}

// StopAllTagIndexManagers2ForTest stops all created TagIndexManager2 instances.
func StopAllTagIndexManagers2ForTest() {
	tagIndexManager2InstancesMu.Lock()
	instances := make([]*TagIndexManager2, len(tagIndexManager2Instances))
	copy(instances, tagIndexManager2Instances)
	tagIndexManager2Instances = nil
	tagIndexManager2InstancesMu.Unlock()

	for _, inst := range instances {
		if inst == nil {
			continue
		}
		func(tim *TagIndexManager2) {
			defer func() { _ = recover() }()
			tim.Stop()
		}(inst)
	}
}

func (tim *TagIndexManager2) GetShutdownChain() chan struct{}  { return tim.shutdownChan }
func (tim *TagIndexManager2) GetWaitGroup() *sync.WaitGroup    { return &tim.wg }
func (tim *TagIndexManager2) GetLevelsManager() levels.Manager { return tim.levelsManager }

// Add updates the index with a new series and its tags.
func (tim *TagIndexManager2) Add(seriesID uint64, tags map[string]string) error {
	tim.mu.RLock()
	mem := tim.memtable
	tim.mu.RUnlock()

	for tagKey, tagValue := range tags {
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

func (tim *TagIndexManager2) Start() {
	tim.logger.Info("Starting tag index manager2 background processes...")
	tim.wg.Add(2)
	go tim.flushLoop()
	go tim.compactionLoop()
}

func (tim *TagIndexManager2) Stop() {
	tim.logger.Info("Stopping tag index manager2...")
	select {
	case <-tim.shutdownChan:
	default:
		close(tim.shutdownChan)
	}
	tim.wg.Wait()
	tim.flushFinalMemtable()
	if err := tim.levelsManager.Close(); err != nil {
		tim.logger.Error("Failed to close index levels manager", "error", err)
	}
	tagIndexManager2ActiveCount.Add(-1)
	if tim.logger != nil {
		tim.logger.Info("TagIndexManager2 stopped", "active_count", tagIndexManager2ActiveCount.Load())
	}
	stopBuf := make([]byte, 1<<12)
	sn := runtime.Stack(stopBuf, false)
	stopStack := string(stopBuf[:sn])
	tagIndexManager2StopsMu.Lock()
	tagIndexManager2Stops = append(tagIndexManager2Stops, fmt.Sprintf("%s\n%s", time.Now().Format(time.RFC3339Nano), stopStack))
	tagIndexManager2StopsMu.Unlock()
	tim.logger.Info("Tag index manager2 stopped.")
}

func (tim *TagIndexManager2) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	if len(tags) == 0 {
		return roaring64.New(), nil
	}
	var resultBitmap *roaring64.Bitmap
	firstTag := true
	for tagKey, tagValue := range tags {
		tagKeyID, ok1 := tim.deps.StringStore.GetID(tagKey)
		tagValueID, ok2 := tim.deps.StringStore.GetID(tagValue)
		if !ok1 || !ok2 {
			return roaring64.New(), nil
		}
		currentTagBitmap, err := tim.getBitmapForTag(tagKeyID, tagValueID)
		if err != nil {
			return nil, fmt.Errorf("failed to get bitmap for tag %s=%s: %w", tagKey, tagValue, err)
		}
		if currentTagBitmap.IsEmpty() {
			return roaring64.New(), nil
		}
		if firstTag {
			resultBitmap = currentTagBitmap
			firstTag = false
		} else {
			resultBitmap.And(currentTagBitmap)
		}
		if resultBitmap.IsEmpty() {
			return resultBitmap, nil
		}
	}
	if resultBitmap == nil {
		return roaring64.New(), nil
	}
	return resultBitmap, nil
}

func (tim *TagIndexManager2) getBitmapForTag(tagKeyID, tagValueID uint64) (*roaring64.Bitmap, error) {
	mergedBitmap := roaring64.New()
	indexKey := EncodeTagIndexKey(tagKeyID, tagValueID)
	tim.mu.RLock()
	mutableBitmap := tim.memtable.GetBitmap(tagKeyID, tagValueID)
	mergedBitmap.Or(mutableBitmap)
	for _, im := range tim.immutableMemtables {
		immutableBitmap := im.GetBitmap(tagKeyID, tagValueID)
		mergedBitmap.Or(immutableBitmap)
	}
	tim.mu.RUnlock()
	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()
	for levelIdx := 0; levelIdx < len(levelStates); levelIdx++ {
		levelTables := levelStates[levelIdx].GetTables()
		for _, table := range levelTables {
			if bytes.Compare(indexKey, table.MinKey()) < 0 || bytes.Compare(indexKey, table.MaxKey()) > 0 {
				continue
			}
			value, entryType, err := table.Get(indexKey)
			if err == nil && entryType == core.EntryTypePutEvent {
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

func (tim *TagIndexManager2) compactionLoop() {
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

func (tim *TagIndexManager2) performIndexCompactionCycle() {
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
					defer func() { <-tim.lnCompactionSemaphore; tim.wg.Done() }()
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

func (tim *TagIndexManager2) compactIndexLevelNToLevelNPlus1(levelN int) error {
	tableToCompact := tim.levelsManager.PickCompactionCandidateForLevelN(levelN)
	if tableToCompact == nil {
		return nil
	}
	minKey, maxKey := tableToCompact.MinKey(), tableToCompact.MaxKey()
	overlappingTables := tim.levelsManager.GetOverlappingTables(levelN+1, minKey, maxKey)
	inputTables := append([]*sstable.SSTable{tableToCompact}, overlappingTables...)
	tim.deps.DeletedSeriesMu.RLock()
	deletedSeriesSnapshot := make(map[string]uint64, len(tim.deps.DeletedSeries))
	for k, v := range tim.deps.DeletedSeries {
		deletedSeriesSnapshot[k] = v
	}
	tim.deps.DeletedSeriesMu.RUnlock()
	newTables, err := tim.mergeIndexSSTables(inputTables, deletedSeriesSnapshot)
	if err != nil {
		return fmt.Errorf("failed to merge index sstables for L%d->L%d: %w", levelN, levelN+1, err)
	}
	if err := tim.levelsManager.ApplyCompactionResults(levelN, levelN+1, newTables, inputTables); err != nil {
		for _, sst := range newTables {
			sys.Remove(sst.FilePath())
		}
		return fmt.Errorf("failed to apply index compaction results for L%d->L%d: %w", levelN, levelN+1, err)
	}
	tim.mu.Lock()
	if err := tim.persistIndexManifestLocked(); err != nil {
		tim.logger.Error("CRITICAL: Failed to persist index manifest after LN compaction.", "error", err)
	}
	tim.mu.Unlock()
	for _, oldTable := range inputTables {
		if err := oldTable.Close(); err != nil {
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

func (tim *TagIndexManager2) compactIndexL0ToL1() error {
	l0Tables := tim.levelsManager.GetTablesForLevel(0)
	if len(l0Tables) == 0 {
		return nil
	}
	tim.deps.DeletedSeriesMu.RLock()
	deletedSeriesSnapshot := make(map[string]uint64, len(tim.deps.DeletedSeries))
	for k, v := range tim.deps.DeletedSeries {
		deletedSeriesSnapshot[k] = v
	}
	tim.deps.DeletedSeriesMu.RUnlock()
	newSSTs, err := tim.mergeIndexSSTables(l0Tables, deletedSeriesSnapshot)
	if err != nil {
		return fmt.Errorf("failed to merge index L0 sstables: %w", err)
	}
	if err := tim.levelsManager.ApplyCompactionResults(0, 1, newSSTs, l0Tables); err != nil {
		for _, sst := range newSSTs {
			sys.Remove(sst.FilePath())
		}
		return fmt.Errorf("failed to apply index compaction results: %w", err)
	}
	tim.mu.Lock()
	if err := tim.persistIndexManifestLocked(); err != nil {
		tim.logger.Error("CRITICAL: Failed to persist index manifest after compaction. Index state may be inconsistent on next startup.", "error", err)
	}
	tim.mu.Unlock()
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

func (tim *TagIndexManager2) mergeIndexSSTables(tables []*sstable.SSTable, deletedSeries map[string]uint64) ([]*sstable.SSTable, error) {
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
		return nil, nil
	}
	var newSSTs []*sstable.SSTable
	// Create a block-format writer for the merged output
	fileID := tim.deps.SSTNextID()
	outPath := filepath.Join(tim.sstDir, fmt.Sprintf("%d.sst", fileID))
	bwOpts := sstable.BlockWriterOptions{TargetBlockSize: int(tim.opts.BaseTargetSize), Compression: sstable.CompressionNone}
	blockWriter, err := sstable.NewBlockWriter(outPath, bwOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create block index writer: %w", err)
	}
	// wrap blockWriter with an adapter implementing core.SSTableWriterInterface
	writer := &blockWriterAdapter{bw: blockWriter}
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
		_ = writer.Abort()
		return nil, err
	}
	if err := writer.Finish(); err != nil {
		_ = writer.Abort()
		return nil, fmt.Errorf("failed to finish index sstable writer: %w", err)
	}
	loadOpts := sstable.LoadSSTableOptions{FilePath: outPath, ID: fileID, Tracer: tim.tracer, Logger: tim.logger}
	newSST, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		sys.Remove(outPath)
		return nil, fmt.Errorf("failed to load newly created index sstable %s: %w", outPath, err)
	}
	newSSTs = append(newSSTs, newSST)
	return newSSTs, nil
}

// blockWriterAdapter adapts sstable.BlockWriter to core.SSTableWriterInterface
type blockWriterAdapter struct {
	bw *sstable.BlockWriter
}

func (a *blockWriterAdapter) Add(key, value []byte, entryType core.EntryType, pointID uint64) error {
	return a.bw.Add(key, value)
}

func (a *blockWriterAdapter) Finish() error {
	return a.bw.Finish()
}

func (a *blockWriterAdapter) Abort() error {
	return a.bw.Abort()
}

func (a *blockWriterAdapter) FilePath() string {
	return a.bw.FilePath()
}

func (a *blockWriterAdapter) CurrentSize() int64 {
	return a.bw.CurrentSize()
}

func (tim *TagIndexManager2) writeCleanBitmap(writer core.SSTableWriterInterface, key []byte, bitmap *roaring64.Bitmap, deletedSeries map[string]uint64) error {
	if bitmap.IsEmpty() {
		return nil
	}
	cleanBitmap := roaring64.New()
	iterator := bitmap.Iterator()
	for iterator.HasNext() {
		seriesID := iterator.Next()
		seriesKeyStr, found := tim.deps.SeriesIDStore.GetKey(seriesID)
		if !found {
			tim.logger.Warn("SeriesID found in index but not in seriesIDStore during compaction", "seriesID", seriesID)
			continue
		}
		if _, isDeleted := deletedSeries[seriesKeyStr]; !isDeleted {
			cleanBitmap.Add(seriesID)
		}
	}
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

func (tim *TagIndexManager2) flushLoop() {
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

func (tim *TagIndexManager2) triggerPeriodicFlush() {
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

func (tim *TagIndexManager2) processImmutableIndexMemtables() {
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
		tim.logger.Error("Failed to flush index memtable", "error", err)
		return
	}
	if newSST != nil {
		tim.mu.Lock()
		tim.levelsManager.AddL0Table(newSST)
		tim.persistIndexManifestLocked()
		tim.mu.Unlock()
		tim.mu.RLock()
		needsCompaction := tim.levelsManager.NeedsL0Compaction(tim.opts.MaxL0Files, tim.opts.L0CompactionTriggerSize)
		tim.mu.RUnlock()
		if needsCompaction {
			tim.signalCompaction()
		}
	}
}

func (tim *TagIndexManager2) flushIndexMemtableToSSTable(memToFlush *IndexMemtable) (*sstable.SSTable, error) {
	if memToFlush == nil || memToFlush.Size() == 0 {
		return nil, nil
	}
	// Create a block-format writer for flushed memtables
	fileID := tim.deps.SSTNextID()
	outPath := filepath.Join(tim.sstDir, fmt.Sprintf("%d.sst", fileID))
	bwOpts := sstable.BlockWriterOptions{TargetBlockSize: int(tim.opts.BaseTargetSize), Compression: sstable.CompressionNone}
	blockWriter, err := sstable.NewBlockWriter(outPath, bwOpts)
	memToFlush.mu.RLock()
	defer memToFlush.mu.RUnlock()
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
				_ = blockWriter.Abort()
				return nil, fmt.Errorf("failed to serialize roaring bitmap for tag %d-%d: %w", tagKeyID, tagValueID, err)
			}
			if err := blockWriter.Add(key, value); err != nil {
				_ = blockWriter.Abort()
				return nil, fmt.Errorf("failed to add index entry to BlockWriter: %w", err)
			}
		}
	}
	if err := blockWriter.Finish(); err != nil {
		_ = blockWriter.Abort()
		return nil, fmt.Errorf("failed to finish index SSTable: %w", err)
	}
	loadOpts := sstable.LoadSSTableOptions{FilePath: outPath, ID: fileID, BlockCache: nil, Tracer: tim.tracer, Logger: tim.logger}
	newSST, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		sys.Remove(outPath)
		return nil, fmt.Errorf("failed to load newly created index SSTable %s: %w", outPath, err)
	}
	tim.logger.Info("Successfully flushed and loaded index SSTable.", "path", outPath, "id", fileID)
	return newSST, nil
}

func (tim *TagIndexManager2) signalCompaction() {
	select {
	case tim.compactionChan <- struct{}{}:
	default:
	}
}

func (tim *TagIndexManager2) flushFinalMemtable() {
	tim.mu.Lock()
	var allMemsToFlush []*IndexMemtable
	allMemsToFlush = append(allMemsToFlush, tim.immutableMemtables...)
	if tim.memtable != nil && tim.memtable.Size() > 0 {
		allMemsToFlush = append(allMemsToFlush, tim.memtable)
	}
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
			continue
		}
		if newSST != nil {
			newSSTs = append(newSSTs, newSST)
		}
	}
	if len(newSSTs) > 0 {
		tim.mu.Lock()
		defer tim.mu.Unlock()
		for _, sst := range newSSTs {
			tim.levelsManager.AddL0Table(sst)
		}
		tim.persistIndexManifestLocked()
	}
}

func (tim *TagIndexManager2) createNewIndexWriter() (core.SSTableWriterInterface, uint64, error) {
	fileID := tim.deps.SSTNextID()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      tim.sstDir,
		ID:                           fileID,
		EstimatedKeys:                1000,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    4 * 1024,
		Tracer:                       tim.tracer,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       tim.logger,
	}
	writerOpts.Preallocate = tim.opts.EnableSSTablePreallocate
	writerOpts.RestartPointInterval = tim.opts.SSTableRestartPointInterval
	writerOpts.PreallocMultiplier = tim.opts.SSTablePreallocMultiplier
	writer, err := tim.sstableWriterFactory(writerOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create index SSTable writer: %w", err)
	}
	return writer, fileID, nil
}

func (tim *TagIndexManager2) persistIndexManifestLocked() error {
	manifest := core.SnapshotManifest{Levels: make([]core.SnapshotLevelManifest, 0, tim.levelsManager.MaxLevels())}
	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		if len(tablesInLevel) == 0 {
			continue
		}
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelNum, Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{ID: table.ID(), FileName: filepath.Base(table.FilePath()), MinKey: table.MinKey(), MaxKey: table.MaxKey()})
		}
		manifest.Levels = append(manifest.Levels, levelManifest)
	}
	if err := PersistIndexManifest(tim.manifestPath, manifest); err != nil {
		return err
	}
	tim.logger.Debug("Index manifest persisted successfully.")
	return nil
}

func (tim *TagIndexManager2) CreateSnapshot(snapshotDir string) error {
	tim.logger.Info("Creating tag index snapshot.", "snapshot_dir", snapshotDir)
	tim.flushFinalMemtable()
	levelStates, unlockFunc := tim.levelsManager.GetSSTablesForRead()
	defer unlockFunc()
	manifest := core.SnapshotManifest{Levels: make([]core.SnapshotLevelManifest, 0, len(levelStates))}
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		if len(tablesInLevel) == 0 {
			continue
		}
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelNum, Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			baseFileName := filepath.Base(table.FilePath())
			destPath := filepath.Join(snapshotDir, baseFileName)
			if err := core.CopyFile(table.FilePath(), destPath); err != nil {
				return fmt.Errorf("failed to copy index SSTable %s to snapshot: %w", table.FilePath(), err)
			}
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{ID: table.ID(), FileName: baseFileName, MinKey: table.MinKey(), MaxKey: table.MaxKey()})
		}
		manifest.Levels = append(manifest.Levels, levelManifest)
	}
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

func (tim *TagIndexManager2) RestoreFromSnapshot(snapshotDir string) error {
	tim.logger.Info("Restoring tag index from snapshot.", "snapshot_dir", snapshotDir)
	manifestPath := filepath.Join(snapshotDir, core.IndexManifestFileName)
	file, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			tim.logger.Warn("No index manifest found in snapshot, skipping index restore.", "path", manifestPath)
			return nil
		}
		return fmt.Errorf("failed to open index snapshot manifest: %w", err)
	}
	defer file.Close()
	var manifest core.SnapshotManifest
	if err := json.NewDecoder(file).Decode(&manifest); err != nil {
		return fmt.Errorf("failed to decode index snapshot manifest: %w", err)
	}
	for _, level := range manifest.Levels {
		for _, tableMeta := range level.Tables {
			srcPath := filepath.Join(snapshotDir, tableMeta.FileName)
			destPath := filepath.Join(tim.sstDir, tableMeta.FileName)
			if err := core.CopyFile(srcPath, destPath); err != nil {
				return fmt.Errorf("failed to restore index SSTable %s: %w", srcPath, err)
			}
		}
	}
	if err := core.CopyFile(manifestPath, tim.manifestPath); err != nil {
		return fmt.Errorf("failed to restore index manifest file: %w", err)
	}
	tim.logger.Info("Tag index restored successfully.")
	return nil
}

func (tim *TagIndexManager2) LoadFromFile(dataDir string) error {
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
			loadOpts := sstable.LoadSSTableOptions{FilePath: filePath, ID: tableMeta.ID, Tracer: tim.tracer, Logger: tim.logger}
			table, loadErr := sstable.LoadSSTable(loadOpts)
			if loadErr != nil {
				tim.logger.Error("Failed to load index SSTable from manifest, skipping.", "file", filePath, "id", tableMeta.ID, "error", loadErr)
				continue
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

func (tim *TagIndexManager2) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	tim.mu.RLock()
	mem := tim.memtable
	tim.mu.RUnlock()
	for _, pair := range encodedTags {
		mem.Add(pair.KeyID, pair.ValueID, seriesID)
	}
	tim.checkAndRotateMemtable(mem)
	return nil
}

func (tim *TagIndexManager2) RemoveSeries(seriesID uint64) {
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

func (tim *TagIndexManager2) checkAndRotateMemtable(mem *IndexMemtable) {
	if mem.Size() >= tim.opts.MemtableThreshold {
		tim.mu.Lock()
		defer tim.mu.Unlock()
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
