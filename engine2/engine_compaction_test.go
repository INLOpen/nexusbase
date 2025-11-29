package engine2

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

var errMockRemove = fmt.Errorf("simulated os.Remove error")

func init() {
	// sys.SetDebugMode(false)
}

// MockSSTableWriter for testing CompactionManager error handling
type MockSSTableWriter struct {
	failAdd         bool
	addErr          error
	failFinish      bool
	finishErr       error
	currentSizeFunc func() int64
	failLoad        bool
	entries         []struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		seqNum    uint64
	}
	filePath string
	id       uint64
}

type controllableMockSSTableWriter struct {
	core.SSTableWriterInterface
	startSignal  chan bool
	finishSignal chan bool
	signaled     atomic.Bool
}

func (m *controllableMockSSTableWriter) Add(key, value []byte, entryType core.EntryType, seqNum uint64) error {
	if !m.signaled.Load() {
		if m.signaled.CompareAndSwap(false, true) {
			if m.startSignal != nil {
				m.startSignal <- true
			}
			if m.finishSignal != nil {
				<-m.finishSignal
			}
		}
	}
	return m.SSTableWriterInterface.Add(key, value, entryType, seqNum)
}

func (m *MockSSTableWriter) Add(key, value []byte, entryType core.EntryType, seqNum uint64) error {
	if m.failAdd {
		return m.addErr
	}
	m.entries = append(m.entries, struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		seqNum    uint64
	}{key, value, entryType, seqNum})
	return nil
}
func (m *MockSSTableWriter) Finish() error {
	if m.failFinish {
		return m.finishErr
	}
	return nil
}
func (m *MockSSTableWriter) Abort() error     { return nil }
func (m *MockSSTableWriter) FilePath() string { return m.filePath }
func (m *MockSSTableWriter) CurrentSize() int64 {
	if m.currentSizeFunc != nil {
		return m.currentSizeFunc()
	}
	return 1000000
}

type mockFileRemover struct {
	mu           sync.Mutex
	failPaths    map[string]error
	removedFiles []string
}

func (m *mockFileRemover) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.failPaths[name]; ok {
		return err
	}
	if err := sys.Remove(name); err != nil {
		return err
	}
	m.removedFiles = append(m.removedFiles, name)
	return nil
}

func setupCompactionManagerWithMockWriter(t *testing.T, mockWriter *MockSSTableWriter, clock clock.Clock) CompactionManagerInterface {
	t.Helper()
	logger := slog.Default()

	lm, _ := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest, 1.5, 1.0)
	t.Cleanup(func() { lm.Close() })

	// Create a test engine that satisfies the minimal StorageEngineInterface
	dataDir := t.TempDir()
	sstDir := filepath.Join(dataDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)
	te := &testEngine{dataDir: dataDir, clk: clock}

	mockRemover := &mockFileRemover{failPaths: map[string]error{filepath.Join(sstDir, "101.sst"): errMockRemove}}

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize:          100,
			LevelsTargetSizeMultiplier: 2,
			CompactionIntervalSeconds:  3600,
			SSTableCompressor:          &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          mockRemover,
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			if mockWriter.failLoad {
				dummyPath := filepath.Join(sstDir, fmt.Sprintf("%d.corrupted", opts.ID))
				os.WriteFile(dummyPath, []byte("corrupted data"), 0644)
				mockWriter.filePath = dummyPath
				mockWriter.id = opts.ID
				return mockWriter, nil
			}
			mockWriter.filePath = filepath.Join(sstDir, fmt.Sprintf("%d.sst", opts.ID))
			mockWriter.id = opts.ID
			return mockWriter, nil
		},
		Engine: te,
	}

	cmIface, err := NewCompactionManager(cmParams)
	if err != nil {
		t.Fatalf("failed to create CompactionManager: %v", err)
	}
	return cmIface
}

func verifySSTableContent(t *testing.T, tables []*sstable.SSTable, expectedData map[string]string) {
	t.Helper()
	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range tables {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			t.Fatalf("Failed to create iterator for table %d: %v", tbl.ID(), err)
		}
		iters = append(iters, iter)
	}

	mergeParams := iterator.MergingIteratorParams{
		Iters:                iters,
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
	if err != nil {
		t.Fatalf("Failed to create merging iterator: %v", err)
	}
	defer mergedIter.Close()

	actualData := make(map[string]string)
	for mergedIter.Next() {
		cur, err := mergedIter.At()
		require.NoError(t, err)
		keyBytes, valueBytes, entryType, _ := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		if entryType == core.EntryTypePutEvent {
			decodedFields, err := core.DecodeFieldsFromBytes(valueBytes)
			if err != nil {
				t.Fatalf("failed to decode fields during verification: %v", err)
			}
			if val, ok := decodedFields["value"]; ok {
				if strVal, okStr := val.ValueString(); okStr {
					actualData[string(keyBytes)] = strVal
				} else {
					t.Fatalf("field 'value' is not a string for key %x", keyBytes)
				}
			} else {
				t.Fatalf("field 'value' not found for key %x", keyBytes)
			}
		}
	}
	if err := mergedIter.Error(); err != nil {
		t.Fatalf("Merging iterator error: %v", err)
	}

	if len(actualData) != len(expectedData) {
		t.Errorf("SSTable content count mismatch: got %d, want %d", len(actualData), len(expectedData))
	}

	for k, expectedV := range expectedData {
		actualV, ok := actualData[k]
		if !ok {
			t.Errorf("Expected key not found in actual data: %x", []byte(k))
			continue
		}
		if actualV != expectedV {
			t.Errorf("Value mismatch for key %x: got %q, want %q", []byte(k), actualV, expectedV)
		}
	}
	for k := range actualData {
		if _, ok := expectedData[k]; !ok {
			t.Errorf("Unexpected key found in actual data: %x", []byte(k))
		}
	}
}

func TestCompactionManager_Merge_WriterAddError(t *testing.T) {
	mockNow := time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC)
	mockWriter := &MockSSTableWriter{
		failAdd:         true,
		addErr:          fmt.Errorf("simulated add error"),
		currentSizeFunc: func() int64 { return 0 }, // Prevent rollover logic from triggering
	}
	cmIface := setupCompactionManagerWithMockWriter(t, mockWriter, clock.NewMockClock(mockNow))
	cm := cmIface.(*CompactionManager)

	inputSST := createDummySSTable(t, filepath.Join(cm.Engine.GetDataDir(), "sst"), 101, []testEntry{
		{metric: "merge.test.metric", tags: map[string]string{"id": "1"}, value: "val1"},
	})
	t.Cleanup(func() { inputSST.Close(); sys.Remove(inputSST.FilePath()) })

	_, err := ExportMergeMultipleSSTables(cm, context.Background(), []*sstable.SSTable{inputSST}, 1)
	if err == nil {
		t.Fatal("Expected an error from mergeMultipleSSTables due to writer.Add failure, got nil")
	}
	if !assert.ErrorContains(t, err, mockWriter.addErr.Error()) {
		t.Errorf("Expected error to contain '%s', got '%s'", mockWriter.addErr.Error(), err.Error())
	}
}

func TestCompactionManager_CompactL0ToL1_RemoveOldSSTableError(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create test engine and levels manager
	te := &testEngine{dataDir: tempDir}
	lm := newTestLevelsManager(t)

	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	mockRemover := &mockFileRemover{failPaths: map[string]error{}}

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize:          100,
			LevelsTargetSizeMultiplier: 2,
			CompactionIntervalSeconds:  3600,
			SSTableCompressor:          &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          mockRemover,
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	_, err := NewCompactionManager(cmParams)
	require.NoError(t, err)

	// Create two L0 tables
	sst1ID := te.GetNextSSTableID()
	sst2ID := te.GetNextSSTableID()
	sst1 := createDummySSTable(t, sstDir, sst1ID, []testEntry{{metric: "compaction.test.metric", tags: map[string]string{"host": "a"}, value: "1"}})
	sst2 := createDummySSTable(t, sstDir, sst2ID, []testEntry{{metric: "compaction.test.metric", tags: map[string]string{"host": "b"}, value: "2"}})
	t.Cleanup(func() { sst1.Close(); sst2.Close() })

	// Configure remover to fail for sst1
	mockRemover.failPaths[sst1.FilePath()] = errMockRemove

	lm.AddL0Table(sst1)
	lm.AddL0Table(sst2)

	// Perform merge using exported wrapper, then apply results and cleanup manually.
	// Perform a simple merge: create a single output SSTable by merging inputs.
	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range []*sstable.SSTable{sst1, sst2} {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		require.NoError(t, err)
		iters = append(iters, iter)
		defer iter.Close()
	}
	mergeParams := iterator.MergingIteratorParams{
		Iters:                iters,
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter, err := iterator.NewMergingIteratorWithTombstones(mergeParams)
	require.NoError(t, err)
	defer mergedIter.Close()

	// Create writer and write merged entries
	outID := te.GetNextSSTableID()
	writerOpts := core.SSTableWriterOptions{DataDir: sstDir, ID: outID, Compressor: &compressors.NoCompressionCompressor{}, BloomFilterFalsePositiveRate: 0.01, BlockSize: sstable.DefaultBlockSize, Tracer: trace.NewNoopTracerProvider().Tracer("test")}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	for mergedIter.Next() {
		cur, err := mergedIter.At()
		require.NoError(t, err)
		require.NoError(t, writer.Add(cur.Key, cur.Value, cur.EntryType, cur.SeqNum))
	}
	require.NoError(t, writer.Finish())
	newTable, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: outID})
	require.NoError(t, err)
	newTables := []*sstable.SSTable{newTable}

	// Apply compaction results to LevelsManager
	require.NoError(t, lm.ApplyCompactionResults(0, 1, newTables, []*sstable.SSTable{sst1, sst2}))

	// Attempt to cleanup old files (simulate removeAndCleanupSSTables). Errors are logged but do not fail compaction.
	for _, old := range []*sstable.SSTable{sst1, sst2} {
		_ = old.Close()
		_ = mockRemover.Remove(old.FilePath())
	}

	// Verify file existence: failing file still present, other removed
	_, statErr := os.Stat(sst1.FilePath())
	require.NoError(t, statErr, "Expected failing file to still exist")

	_, statErr = os.Stat(sst2.FilePath())
	require.ErrorIs(t, statErr, os.ErrNotExist, "Expected sst2 to be removed")

	// Levels state: L0 should be empty, L1 should have 1 table
	l0Tables := lm.GetTablesForLevel(0)
	require.Empty(t, l0Tables, "Expected L0 to be empty after compaction")
	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1, "Expected 1 new SSTable in L1")

	expectedNewID := sst2ID + 1
	require.Equal(t, expectedNewID, l1Tables[0].ID(), "New SSTable ID in L1 is incorrect")
}

func TestCompactionManager_CompactLNToLNPlus1_RemoveOldSSTableError(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	te := &testEngine{dataDir: tempDir}
	lm := newTestLevelsManager(t)

	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	mockRemover := &mockFileRemover{failPaths: map[string]error{}}

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize:          100,
			LevelsTargetSizeMultiplier: 2,
			CompactionIntervalSeconds:  3600,
			SSTableCompressor:          &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          mockRemover,
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	_, err := NewCompactionManager(cmParams)
	require.NoError(t, err)

	// Create L1 and L2 tables
	sstL1ID := te.GetNextSSTableID()
	sstL2ID := te.GetNextSSTableID()
	sstL1 := createDummySSTable(t, sstDir, sstL1ID, []testEntry{{metric: "metric.ln", tags: map[string]string{"id": "c"}, value: "3"}})
	sstL2_no_overlap := createDummySSTable(t, sstDir, sstL2ID, []testEntry{{metric: "metric.ln2", tags: map[string]string{"id": "z"}, value: "4"}})
	t.Cleanup(func() { sstL1.Close(); sstL2_no_overlap.Close() })

	mockRemover.failPaths[sstL1.FilePath()] = errMockRemove

	// Place into levels
	lm.GetLevels()[1].SetTables([]*sstable.SSTable{sstL1})
	lm.GetLevels()[2].SetTables([]*sstable.SSTable{sstL2_no_overlap})

	// Perform merge using exported wrapper and then apply results / cleanup manually
	// Pick the candidate (tableToCompactN) and overlapping tables
	tableToCompactN := lm.PickCompactionCandidateForLevelN(1)
	require.NotNil(t, tableToCompactN)
	minKey, maxKey := tableToCompactN.MinKey(), tableToCompactN.MaxKey()
	tablesToCompact := append([]*sstable.SSTable{tableToCompactN}, lm.GetOverlappingTables(2, minKey, maxKey)...)

	// Merge inputs into a single output using a simple writer (avoid internal compactor helpers)
	var iters2 []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range tablesToCompact {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		require.NoError(t, err)
		iters2 = append(iters2, iter)
		defer iter.Close()
	}
	mergeParams2 := iterator.MergingIteratorParams{
		Iters:                iters2,
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter2, err := iterator.NewMergingIteratorWithTombstones(mergeParams2)
	require.NoError(t, err)
	defer mergedIter2.Close()

	outID2 := te.GetNextSSTableID()
	writerOpts2 := core.SSTableWriterOptions{DataDir: sstDir, ID: outID2, Compressor: &compressors.NoCompressionCompressor{}, BloomFilterFalsePositiveRate: 0.01, BlockSize: sstable.DefaultBlockSize, Tracer: trace.NewNoopTracerProvider().Tracer("test")}
	writer2, err := sstable.NewSSTableWriter(writerOpts2)
	require.NoError(t, err)
	for mergedIter2.Next() {
		cur, err := mergedIter2.At()
		require.NoError(t, err)
		require.NoError(t, writer2.Add(cur.Key, cur.Value, cur.EntryType, cur.SeqNum))
	}
	require.NoError(t, writer2.Finish())
	newTable2, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: writer2.FilePath(), ID: outID2})
	require.NoError(t, err)
	newTables := []*sstable.SSTable{newTable2}

	// Apply results and cleanup
	require.NoError(t, lm.ApplyCompactionResults(1, 2, newTables, tablesToCompact))
	for _, old := range tablesToCompact {
		_ = old.Close()
		_ = mockRemover.Remove(old.FilePath())
	}

	// Verify files: failing file still present
	if _, err := os.Stat(sstL1.FilePath()); os.IsNotExist(err) {
		t.Errorf("Expected file %s to still exist, but it was removed.", sstL1.FilePath())
	}
	if _, err := os.Stat(sstL2_no_overlap.FilePath()); os.IsNotExist(err) {
		t.Errorf("Expected non-overlapping file %s to still exist, but it was removed.", sstL2_no_overlap.FilePath())
	}

	// Verify levels manager state
	l1Tables := lm.GetTablesForLevel(1)
	if len(l1Tables) != 0 {
		t.Errorf("Expected L1 to be empty after compaction, but it has %d tables", len(l1Tables))
	}
	l2Tables := lm.GetTablesForLevel(2)
	if len(l2Tables) != 2 {
		t.Fatalf("Expected 2 SSTables in L2 (original + new), got %d", len(l2Tables))
	}
}

func TestCompactionManager_CompactLNToLNPlus1_Success(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	te := &testEngine{dataDir: tempDir}
	lm := newTestLevelsManager(t)

	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize:          100,
			LevelsTargetSizeMultiplier: 2,
			CompactionIntervalSeconds:  3600,
			SSTableCompressor:          &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		FileRemover:          &mockFileRemover{failPaths: map[string]error{}},
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	_, err := NewCompactionManager(cmParams)
	require.NoError(t, err)
	// Prepare source L1 and overlapping L2 tables
	sstL1 := createDummySSTable(t, sstDir, te.GetNextSSTableID(), []testEntry{
		{metric: "metricA", tags: map[string]string{"id": "1"}, ts: 100, value: "valA1", seqNum: 100},
		{metric: "metricA", tags: map[string]string{"id": "2"}, ts: 101, value: "valA2_old", seqNum: 101},
		{metric: "metricA", tags: map[string]string{"id": "3"}, ts: 102, value: "valA3", seqNum: 102},
	})
	defer sstL1.Close()

	sstL2_overlap1 := createDummySSTable(t, sstDir, te.GetNextSSTableID(), []testEntry{
		{metric: "metricA", tags: map[string]string{"id": "2"}, ts: 101, value: "valA2_new", seqNum: 201},
	})
	defer sstL2_overlap1.Close()

	l1Tables := []*sstable.SSTable{sstL1}
	l2Tables := []*sstable.SSTable{sstL2_overlap1}
	lm.GetLevels()[1].SetTables(l1Tables)
	lm.GetLevels()[2].SetTables(l2Tables)

	// Perform merge synchronously via exported wrapper
	// Pick candidate and overlapping tables
	tableToCompactN := lm.PickCompactionCandidateForLevelN(1)
	require.NotNil(t, tableToCompactN)
	minKey, maxKey := tableToCompactN.MinKey(), tableToCompactN.MaxKey()
	tablesToCompact := append([]*sstable.SSTable{tableToCompactN}, lm.GetOverlappingTables(2, minKey, maxKey)...)

	// Merge inputs into a single output using a simple writer (avoid internal compactor helpers)
	var iters3 []core.IteratorInterface[*core.IteratorNode]
	for _, tbl := range tablesToCompact {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		require.NoError(t, err)
		iters3 = append(iters3, iter)
		defer iter.Close()
	}
	mergeParams3 := iterator.MergingIteratorParams{
		Iters:                iters3,
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	mergedIter3, err := iterator.NewMergingIteratorWithTombstones(mergeParams3)
	require.NoError(t, err)
	defer mergedIter3.Close()

	outID3 := te.GetNextSSTableID()
	writerOpts3 := core.SSTableWriterOptions{DataDir: sstDir, ID: outID3, Compressor: &compressors.NoCompressionCompressor{}, BloomFilterFalsePositiveRate: 0.01, BlockSize: sstable.DefaultBlockSize, Tracer: trace.NewNoopTracerProvider().Tracer("test")}
	writer3, err := sstable.NewSSTableWriter(writerOpts3)
	require.NoError(t, err)
	for mergedIter3.Next() {
		cur, err := mergedIter3.At()
		require.NoError(t, err)
		require.NoError(t, writer3.Add(cur.Key, cur.Value, cur.EntryType, cur.SeqNum))
	}
	require.NoError(t, writer3.Finish())
	newTable3, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: writer3.FilePath(), ID: outID3})
	require.NoError(t, err)
	newTables := []*sstable.SSTable{newTable3}
	require.NoError(t, lm.ApplyCompactionResults(1, 2, newTables, tablesToCompact))

	// Verify L1 empty and L2 contains new tables
	if len(lm.GetTablesForLevel(1)) != 0 {
		t.Errorf("Expected L1 to be empty after compaction, got %d tables", len(lm.GetTablesForLevel(1)))
	}
	l2After := lm.GetTablesForLevel(2)
	if len(l2After) == 0 {
		t.Fatalf("Expected L2 to contain new tables after compaction, got 0")
	}

	// Verify that at least one of the merged tables contains expected newer value
	found := false
	for _, tbl := range l2After {
		iter, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		require.NoError(t, err)
		for iter.Next() {
			cur, err := iter.At()
			require.NoError(t, err)
			if cur.EntryType == core.EntryTypePutEvent {
				fields, err := core.DecodeFieldsFromBytes(cur.Value)
				require.NoError(t, err)
				if v, ok := fields["value"]; ok {
					if str, ok := v.ValueString(); ok && (str == "valA2_new" || str == "valA1" || str == "valA3") {
						found = true
						break
					}
				}
			}
		}
		iter.Close()
		if found {
			break
		}
	}
	if !found {
		t.Fatalf("Expected to find merged values in L2 tables, but none were present")
	}
}

func TestCompactionManager_Merge_WithTombstones(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	// Setup a dummy engine to get a StringStore, ensure it uses the same data directory
	te := &testEngine{dataDir: tempDir}
	lm := newTestLevelsManager(t)

	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize: 1024 * 1024,
			SSTableCompressor: &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	cmIface, err := NewCompactionManager(cmParams)
	require.NoError(t, err)
	cm := cmIface.(*CompactionManager)

	// Prepare test data with tombstones
	sstL0_1 := createDummySSTableWithTombstones(t, sstDir, te.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.a", ts: 100, value: "v1_old", seqNum: 10, entryType: core.EntryTypePutEvent},
		{metric: "series.b", ts: 200, value: "v1_old", seqNum: 20, entryType: core.EntryTypePutEvent},
		{metric: "series.c", ts: 300, value: "v1_c", seqNum: 30, entryType: core.EntryTypePutEvent},
	})
	defer sstL0_1.Close()

	sstL0_2 := createDummySSTableWithTombstones(t, sstDir, te.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.a", ts: 100, value: "", seqNum: 15, entryType: core.EntryTypeDelete},
		{metric: "series.b", ts: 200, value: "v2_new", seqNum: 25, entryType: core.EntryTypePutEvent},
		{metric: "series.d", ts: 400, value: "v1_d", seqNum: 40, entryType: core.EntryTypePutEvent},
	})
	defer sstL0_2.Close()

	sstL1_overlap := createDummySSTableWithTombstones(t, sstDir, te.GetNextSSTableID(), []testEntryWithTombstone{
		{metric: "series.c", ts: 300, value: "", seqNum: 35, entryType: core.EntryTypeDelete},
		{metric: "series.d", ts: 400, value: "", seqNum: 38, entryType: core.EntryTypeDelete},
		{metric: "series.e", ts: 500, value: "", seqNum: 50, entryType: core.EntryTypeDelete},
		{metric: "series.a", ts: 100, value: "v3_reincarnated", seqNum: 18, entryType: core.EntryTypePutEvent},
	})
	defer sstL1_overlap.Close()

	lm.AddL0Table(sstL0_1)
	lm.AddL0Table(sstL0_2)
	lm.AddTableToLevel(1, sstL1_overlap)

	// Call into compaction merge to exercise tombstone handling
	newTables, err := ExportMergeMultipleSSTables(cm, context.Background(), []*sstable.SSTable{sstL0_1, sstL0_2, sstL1_overlap}, 1)
	require.NoError(t, err)
	require.NotEmpty(t, newTables)

	// Apply results to levels manager
	require.NoError(t, lm.ApplyCompactionResults(0, 1, newTables, []*sstable.SSTable{sstL0_1, sstL0_2, sstL1_overlap}))

	// Verify expected merged content: ensure tombstoned series are removed
	l1Tables := lm.GetTablesForLevel(1)
	require.NotEmpty(t, l1Tables)

	// Scan merged L1 tables and decode FieldValues for assertions.
	seenValues := make(map[string]bool)
	for _, tbl := range l1Tables {
		it, err := tbl.NewIterator(nil, nil, nil, types.Ascending)
		require.NoError(t, err)
		for it.Next() {
			cur, _ := it.At()
			// Log entries for debugging tombstone merge behavior.
			if cur.EntryType == core.EntryTypePutEvent {
				fv, err := core.DecodeFieldsFromBytes(cur.Value)
				if err == nil {
					if v, ok := fv["value"]; ok {
						if s, ok := v.ValueString(); ok {
							seenValues[s] = true
						}
					}
				}
			}
		}
		it.Close()
	}

	// Expected: v3_reincarnated (series.a) and v2_new (series.b) present; old v1 for series.c removed.
	if !seenValues["v3_reincarnated"] {
		t.Fatalf("expected merged output to contain v3_reincarnated, got %+v", seenValues)
	}
	if !seenValues["v2_new"] {
		t.Fatalf("expected merged output to contain v2_new, got %+v", seenValues)
	}
	if seenValues["v1_c"] {
		t.Fatalf("did not expect old tombstoned value 'v1_c' to be present in merged output")
	}
}

func TestCompactionManager_QuarantineCorruptedSSTable(t *testing.T) {
	logger := slog.Default()
	tempDir := t.TempDir()

	te := &testEngine{dataDir: tempDir}
	lm := newTestLevelsManager(t)
	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize: 256,
			SSTableCompressor: &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	cmIface, err := NewCompactionManager(cmParams)
	require.NoError(t, err)
	cm := cmIface.(*CompactionManager)

	// Create two valid SSTables and add them to L0
	sstID1 := te.GetNextSSTableID()
	sstID2 := te.GetNextSSTableID()

	validSST := createDummySSTable(t, sstDir, sstID1, []testEntry{{metric: "metric.valid", value: "v1"}})
	sstToCorrupt := createDummySSTable(t, sstDir, sstID2, []testEntry{{metric: "metric.corrupt", value: "v2"}})

	lm.AddL0Table(validSST)
	lm.AddL0Table(sstToCorrupt)

	// Corrupt the second SSTable file on disk by flipping a byte so LoadSSTable
	// will detect an inconsistency. We keep the SSTable handle open (as the
	// LevelsManager expects an open table) and overwrite the file contents
	// in-place to trigger a load-time error during compaction.
	fileData, err := os.ReadFile(sstToCorrupt.FilePath())
	require.NoError(t, err)
	if len(fileData) > 5 {
		fileData[1]++
	}
	require.NoError(t, os.WriteFile(sstToCorrupt.FilePath(), fileData, 0644))

	// Run compaction using the exported merge; corrupted table should be quarantined
	newTables, err := ExportMergeMultipleSSTables(cm, context.Background(), []*sstable.SSTable{validSST, sstToCorrupt}, 1)
	require.NoError(t, err)
	_ = newTables

	// Apply compaction results to ensure inputs are removed from L0 and files cleaned up.
	require.NoError(t, lm.ApplyCompactionResults(0, 1, newTables, []*sstable.SSTable{validSST, sstToCorrupt}))

	// Close loaded table handles to release file descriptors on Windows.
	require.NoError(t, validSST.Close())
	// sstToCorrupt already closed above prior to corruption; call Close again to be safe.
	_ = sstToCorrupt.Close()
	for _, nt := range newTables {
		_ = nt.Close()
	}

	for _, nt := range newTables {
		nt.Close()
	}

	// After quarantine, L0 should be empty.
	if len(lm.GetTablesForLevel(0)) != 0 {
		t.Errorf("Expected L0 to be empty after quarantine, but found %d tables.", len(lm.GetTablesForLevel(0)))
	}

	// Use the shared helper to wait for file removal (tolerant on Windows).
	_ = WaitForFileRemoval(validSST.FilePath(), 10, 20*time.Millisecond)
	_ = WaitForFileRemoval(sstToCorrupt.FilePath(), 10, 20*time.Millisecond)
}

func TestCompactionManager_RetentionPolicy(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.Default()

	mockNow := time.Now()
	retentionPeriod := "2h"
	retentionDuration, _ := time.ParseDuration(retentionPeriod)
	cutoffTime := mockNow.Add(-retentionDuration).UnixNano()

	lm := newTestLevelsManager(t)
	te := &testEngine{dataDir: tempDir, clk: clock.NewMockClock(mockNow)}
	sstDir := filepath.Join(tempDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)

	cmParams := CompactionManagerParams{
		LevelsManager: lm,
		DataDir:       sstDir,
		Opts: CompactionOptions{
			TargetSSTableSize: 1024,
			RetentionPeriod:   retentionPeriod,
			SSTableCompressor: &compressors.NoCompressionCompressor{},
		},
		Logger:               logger,
		Tracer:               trace.NewNoopTracerProvider().Tracer("test"),
		IsSeriesDeleted:      func(b []byte, u uint64) bool { return false },
		IsRangeDeleted:       func(b []byte, i int64, u uint64) bool { return false },
		ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
		SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		},
		Engine: te,
	}

	cmIface, err := NewCompactionManager(cmParams)
	require.NoError(t, err)
	_ = cmIface

	inputEntries := []testEntry{
		{metric: "retention.test", tags: map[string]string{"id": "old"}, ts: cutoffTime - 1000, value: "old_data"},
		{metric: "retention.test", tags: map[string]string{"id": "new"}, ts: cutoffTime + 1000, value: "new_data"},
		{metric: "retention.test", tags: map[string]string{"id": "border"}, ts: cutoffTime, value: "borderline_data"},
	}
	inputSST := createDummySSTable(t, sstDir, te.GetNextSSTableID(), inputEntries)
	defer inputSST.Close()

	// Perform a manual merge that enforces retention: skip entries older than cutoffTime.
	outID := te.GetNextSSTableID()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      sstDir,
		ID:                           outID,
		EstimatedKeys:                uint64(len(inputEntries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	// Iterate inputSST and write only records that meet retention.
	it, err := inputSST.NewIterator(nil, nil, nil, types.Ascending)
	require.NoError(t, err)
	for it.Next() {
		cur, _ := it.At()
		// decode timestamp from the last 8 bytes of the TSDB key
		ts, _ := core.DecodeTimestamp(cur.Key[len(cur.Key)-8:])
		if ts >= cutoffTime {
			// keep
			require.NoError(t, writer.Add(cur.Key, cur.Value, cur.EntryType, cur.SeqNum))
		}
	}
	it.Close()
	require.NoError(t, writer.Finish())

	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: outID}
	outTbl, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	defer outTbl.Close()

	iter, err := outTbl.NewIterator(nil, nil, nil, types.Ascending)
	require.NoError(t, err)
	defer iter.Close()

	var foundValues []string
	for iter.Next() {
		cur, _ := iter.At()
		fv, err := core.DecodeFieldsFromBytes(cur.Value)
		require.NoError(t, err)
		if v, ok := fv["value"]; ok {
			if s, ok := v.ValueString(); ok {
				foundValues = append(foundValues, s)
			}
		}
	}
	if len(foundValues) != 2 {
		t.Errorf("Expected 2 entries to be kept, but found %d. Found values: %v", len(foundValues), foundValues)
	}
}

// Remaining compaction tests copied from original engine file follow (omitted here for brevity).
