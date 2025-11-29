package engine2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/types"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// Lightweight test types reused by the compaction tests.
type testEntry struct {
	metric string
	tags   map[string]string
	ts     int64
	value  string
	seqNum uint64
}

type testEntryWithTombstone struct {
	metric    string
	tags      map[string]string
	ts        int64
	value     string
	seqNum    uint64
	entryType core.EntryType
}

var (
	metricNameMu   sync.Mutex
	metricNameToID = make(map[string]uint64)
	metricBaseID   = uint64(1000)
)

func metricIDForName(name string) uint64 {
	metricNameMu.Lock()
	defer metricNameMu.Unlock()
	if id, ok := metricNameToID[name]; ok {
		return id
	}
	id := metricBaseID + uint64(len(metricNameToID)+1)
	metricNameToID[name] = id
	return id
}

// createTestSSTableForCleanup creates a simple SSTable for cleanup tests.
func createTestSSTableForCleanup(t *testing.T, dir string, id uint64) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		BloomFilterFalsePositiveRate: 0.01,
		DataDir:                      dir,
		ID:                           id,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	require.NoError(t, writer.Add([]byte(fmt.Sprintf("key-%d", id)), []byte("value"), core.EntryTypePutEvent, id))
	require.NoError(t, writer.Finish())

	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tbl, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return tbl
}

// getTableIDs returns IDs for a slice of SSTables.
func getTableIDs(tables []*sstable.SSTable) []uint64 {
	ids := make([]uint64, len(tables))
	for i, tbl := range tables {
		ids[i] = tbl.ID()
	}
	return ids
}

// makeTestEventValue helper to encode a FieldValues payload.
func makeTestEventValue(t *testing.T, val string) []byte {
	t.Helper()
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": val})
	require.NoError(t, err)
	encoded, err := fields.Encode()
	require.NoError(t, err)
	return encoded
}

// helper to create a minimal LevelsManager used by compaction tests.
func newTestLevelsManager(t *testing.T) levels.Manager {
	lm, err := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest, 1.5, 1.0)
	require.NoError(t, err)
	t.Cleanup(func() { lm.Close() })
	return lm
}

// createDummySSTable creates an SSTable from provided entries without relying
// on a concrete storageEngine string store. It assigns deterministic numeric
// IDs for metric and tag keys for testing purposes.
func createDummySSTable(t *testing.T, dir string, id uint64, entries []testEntry) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	// Deterministic metric id assignment based on metric name
	for _, e := range entries {
		metricID := metricIDForName(e.metric)
		var pairs []core.EncodedSeriesTagPair
		j := uint64(1)
		for range e.tags {
			// Tag key/value ids are derived from the metric id for determinism
			pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: metricID + j, ValueID: metricID + j + 1})
			j += 2
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
		ts := e.ts
		if ts == 0 {
			ts = int64(e.seqNum)
		}
		tsdbKey := core.EncodeTSDBKey(metricID, pairs, ts)
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": e.value})
		require.NoError(t, err)
		encodedFields, err := fields.Encode()
		require.NoError(t, err)
		require.NoError(t, writer.Add(tsdbKey, encodedFields, core.EntryTypePutEvent, e.seqNum))
	}
	require.NoError(t, writer.Finish())
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return sst
}

// createDummySSTableWithTombstones behaves like createDummySSTable but allows tombstone entries.
func createDummySSTableWithTombstones(t *testing.T, dir string, id uint64, entries []testEntryWithTombstone) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		DataDir:                      dir,
		ID:                           id,
		EstimatedKeys:                uint64(len(entries)),
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       trace.NewNoopTracerProvider().Tracer("test"),
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)

	for _, e := range entries {
		metricID := metricIDForName(e.metric)
		var pairs []core.EncodedSeriesTagPair
		j := uint64(1)
		for range e.tags {
			pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: metricID + j, ValueID: metricID + j + 1})
			j += 2
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
		ts := e.ts
		tsdbKey := core.EncodeTSDBKey(metricID, pairs, ts)

		var valueBytes []byte
		if e.entryType == core.EntryTypePutEvent {
			fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": e.value})
			require.NoError(t, err)
			valueBytes, err = fields.Encode()
			require.NoError(t, err)
		} else {
			valueBytes = []byte(e.value)
		}
		require.NoError(t, writer.Add(tsdbKey, valueBytes, e.entryType, e.seqNum))
	}
	require.NoError(t, writer.Finish())
	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id}
	sst, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return sst
}

// encodeTags is a lightweight encoder compatible with the helpers above.
func encodeTags(tags map[string]string) []core.EncodedSeriesTagPair {
	if tags == nil {
		return nil
	}
	var pairs []core.EncodedSeriesTagPair
	i := uint64(1)
	for range tags {
		pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: i, ValueID: i + 1})
		i += 2
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].KeyID < pairs[j].KeyID })
	return pairs
}

// WaitForFileRemoval retries checking for file non-existence. It returns true
// if the file is confirmed removed within the given attempts, otherwise false.
// Use this helper in tests that need to be tolerant of OS-level file handle
// release timing (Windows).
func WaitForFileRemoval(path string, attempts int, delay time.Duration) bool {
	for i := 0; i < attempts; i++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return true
		}
		time.Sleep(delay)
	}
	return false
}

// testEngine implements StorageEngineInterface with minimal behavior
// required by compaction tests. Methods not used by tests are no-ops.
type testEngine struct {
	dataDir     string
	nextID      atomic.Uint64
	clk         clock.Clock
	hookManager hooks.HookManager
	logger      *slog.Logger
	stringStore *StringStore
}

func (t *testEngine) GetNextSSTableID() uint64                                    { return t.nextID.Add(1) }
func (t *testEngine) Put(ctx context.Context, point core.DataPoint) error         { return nil }
func (t *testEngine) PutBatch(ctx context.Context, points []core.DataPoint) error { return nil }
func (t *testEngine) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *testEngine) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	return nil
}
func (t *testEngine) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	return nil
}
func (t *testEngine) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	return nil
}
func (t *testEngine) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *testEngine) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	return nil, nil
}
func (t *testEngine) GetMetrics() ([]string, error)                           { return nil, nil }
func (t *testEngine) GetTagsForMetric(metric string) ([]string, error)        { return nil, nil }
func (t *testEngine) GetTagValues(metric, tagKey string) ([]string, error)    { return nil, nil }
func (t *testEngine) ForceFlush(ctx context.Context, wait bool) error         { return nil }
func (t *testEngine) TriggerCompaction()                                      {}
func (t *testEngine) CreateIncrementalSnapshot(snapshotsBaseDir string) error { return nil }
func (t *testEngine) VerifyDataConsistency() []error                          { return nil }
func (t *testEngine) CreateSnapshot(ctx context.Context) (string, error)      { return "", nil }
func (t *testEngine) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	return nil
}
func (t *testEngine) ApplyReplicatedEntry(ctx context.Context, entry *proto.WALEntry) error {
	return nil
}
func (t *testEngine) GetLatestAppliedSeqNum() uint64               { return 0 }
func (t *testEngine) ReplaceWithSnapshot(snapshotDir string) error { return nil }

func (t *testEngine) Start() error                        { return nil }
func (t *testEngine) Close() error                        { return nil }
func (t *testEngine) GetPubSub() (PubSubInterface, error) { return nil, nil }
func (t *testEngine) GetSnapshotsBaseDir() string         { return filepath.Join(t.dataDir, "snapshots") }
func (t *testEngine) Metrics() (*EngineMetrics, error)    { return nil, nil }
func (t *testEngine) GetHookManager() hooks.HookManager {
	if t.hookManager == nil {
		if t.logger == nil {
			t.logger = slog.Default()
		}
		t.hookManager = hooks.NewHookManager(t.logger.With("component", "testEngineHookManager"))
	}
	return t.hookManager
}
func (t *testEngine) GetDLQDir() string        { return filepath.Join(t.dataDir, "dlq") }
func (t *testEngine) GetDataDir() string       { return t.dataDir }
func (t *testEngine) GetWALPath() string       { return filepath.Join(t.dataDir, "wal") }
func (t *testEngine) GetClock() clock.Clock    { return t.clk }
func (t *testEngine) GetWAL() wal.WALInterface { return nil }
func (t *testEngine) GetStringStore() indexer.StringStoreInterface {
	if t.stringStore == nil {
		t.stringStore = NewStringStore()
	}
	return t.stringStore
}
func (t *testEngine) GetSnapshotManager() snapshot.ManagerInterface { return nil }
func (t *testEngine) GetSequenceNumber() uint64                     { return 0 }

// setupCompactionManagerWithMockWriter creates a CompactionManager configured
// with a provided MockSSTableWriter and a deterministic test LevelsManager.
// This was previously defined in the compaction tests; centralizing it here
// allows reuse across compaction-related tests.
func setupCompactionManagerWithMockWriter(t *testing.T, mockWriter *MockSSTableWriter, clk clock.Clock) CompactionManagerInterface {
	t.Helper()
	logger := slog.Default()

	lm, _ := levels.NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), levels.PickOldest, 1.5, 1.0)
	t.Cleanup(func() { lm.Close() })

	// Create a test engine that satisfies the minimal StorageEngineInterface
	dataDir := t.TempDir()
	sstDir := filepath.Join(dataDir, "sst")
	_ = os.MkdirAll(sstDir, 0o755)
	te := &testEngine{dataDir: dataDir, clk: clk}

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

var errMockRemove = fmt.Errorf("simulated os.Remove error")

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
