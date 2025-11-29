package engine2

import (
	"context"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// fakeTagIndexManager is a minimal implementation used for tests.
type fakeTagIndexManager struct {
	lastAddedID  uint64
	lastAddedEnc []core.EncodedSeriesTagPair
	lastRemoved  uint64
}

func (f *fakeTagIndexManager) Add(seriesID uint64, tags map[string]string) error { return nil }
func (f *fakeTagIndexManager) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	f.lastAddedID = seriesID
	f.lastAddedEnc = make([]core.EncodedSeriesTagPair, len(encodedTags))
	copy(f.lastAddedEnc, encodedTags)
	return nil
}
func (f *fakeTagIndexManager) RemoveSeries(seriesID uint64) { f.lastRemoved = seriesID }
func (f *fakeTagIndexManager) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	return nil, nil
}
func (f *fakeTagIndexManager) Start()                                       {}
func (f *fakeTagIndexManager) Stop()                                        {}
func (f *fakeTagIndexManager) CreateSnapshot(snapshotDir string) error      { return nil }
func (f *fakeTagIndexManager) RestoreFromSnapshot(snapshotDir string) error { return nil }
func (f *fakeTagIndexManager) LoadFromFile(dataDir string) error            { return nil }

func TestApplyReplicatedEntry_PutAndDeleteSeries(t *testing.T) {
	dataDir := t.TempDir()
	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: dataDir, HookManager: hooks.NewHookManager(nil)})
	a := ai.(*Engine2Adapter)
	// inject fake tag index
	fake := &fakeTagIndexManager{}
	a.tagIndexManager = fake

	require.NoError(t, a.Start())

	// Build a PUT WALEntry
	fields, ferr := structpb.NewStruct(map[string]interface{}{"value": 1.23})
	require.NoError(t, ferr)
	entry := &pb.WALEntry{
		SequenceNumber: 1,
		EntryType:      pb.WALEntry_PUT_EVENT,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "a", "region": "us"},
		Timestamp:      123,
		Fields:         fields,
	}

	require.NoError(t, a.ApplyReplicatedEntry(context.Background(), entry))

	// compute series key as adapter would
	// Batch-create IDs for metric and tags to match runtime batching
	list := []string{"cpu", "host", "a", "region", "us"}
	idMap := make(map[string]uint64, len(list))
	if ss, ok := a.stringStore.(*indexer.StringStore); ok {
		ids, err := ss.AddStringsBatch(list)
		require.NoError(t, err)
		for i, s := range list {
			idMap[s] = ids[i]
		}
	} else {
		// fallback: create individually
		for _, s := range list {
			id, err := a.stringStore.GetOrCreateID(s)
			require.NoError(t, err)
			idMap[s] = id
		}
	}
	metricID := idMap["cpu"]
	var pairs []core.EncodedSeriesTagPair
	pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: idMap["host"], ValueID: idMap["a"]})
	pairs = append(pairs, core.EncodedSeriesTagPair{KeyID: idMap["region"], ValueID: idMap["us"]})
	// ensure canonical ordering
	if pairs[0].KeyID > pairs[1].KeyID {
		pairs[0], pairs[1] = pairs[1], pairs[0]
	}
	seriesKey := core.EncodeSeriesKey(metricID, pairs)
	sid, ok := a.seriesIDStore.GetID(string(seriesKey))
	require.True(t, ok, "series id should exist after applying put")
	require.Equal(t, sid, fake.lastAddedID, "tag index should have been updated for series id")

	// Now apply delete series
	delEntry := &pb.WALEntry{
		SequenceNumber: 2,
		EntryType:      pb.WALEntry_DELETE_SERIES,
		Metric:         "cpu",
		Tags:           map[string]string{"host": "a", "region": "us"},
	}
	require.NoError(t, a.ApplyReplicatedEntry(context.Background(), delEntry))

	// deletedSeries map should contain the series key
	a.deletedSeriesMu.RLock()
	_, found := a.deletedSeries[string(seriesKey)]
	a.deletedSeriesMu.RUnlock()
	require.True(t, found, "deleted series should be recorded")
	require.Equal(t, sid, fake.lastRemoved, "tag index RemoveSeries should be called with same series id")
}
