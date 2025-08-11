package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper for memtable tests to create a valid encoded FieldValues byte slice from a simple string value.
func makeTestEventValue(tb testing.TB, val string) []byte {
	tb.Helper()
	// An empty string value in the test case represents a nil value for tombstones.
	if val == "" {
		return nil
	}
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": val})
	require.NoError(tb, err)
	encoded, err := fields.Encode()
	require.NoError(tb, err)
	return encoded
}

// Helper to decode and extract the simple "value" field for verification.
func getTestEventValue(tb testing.TB, data []byte) string {
	tb.Helper()
	if data == nil {
		return ""
	}
	fields, err := core.DecodeFieldsFromBytes(data)
	require.NoError(tb, err)
	if val, ok := fields["value"]; ok {
		if strVal, okStr := val.ValueString(); okStr {
			return strVal
		}
	}
	// This can happen if the value was nil (e.g. for a tombstone)
	return ""
}

func TestMemtable_Get_Scenarios(t *testing.T) {
	type operation struct {
		key       []byte
		value     string
		entryType core.EntryType
		pointID   uint64
	}

	type testCase struct {
		name          string
		operations    []operation
		getKey        []byte
		expectedValue string
		expectedType  core.EntryType
		expectedFound bool
	}

	testCases := []testCase{
		{
			name: "Simple Get - Found",
			operations: []operation{
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // pointID = 1
			},
			getKey:        []byte("key1"),
			expectedValue: "val1",
			expectedType:  core.EntryTypePutEvent,
			expectedFound: true,
		},
		{
			name: "Simple Get - Not Found",
			operations: []operation{
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // pointID = 1
			},
			getKey:        []byte("non-existent"),
			expectedValue: "",
			expectedType:  0,
			expectedFound: false,
		},
		{
			name: "Get after Update",
			operations: []operation{
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // pointID = 1
				{[]byte("key1"), "val2", core.EntryTypePutEvent, 2}, // pointID = 2
			},
			getKey:        []byte("key1"),
			expectedValue: "val2",
			expectedType:  core.EntryTypePutEvent,
			expectedFound: true,
		},
		{
			name: "Get after Delete (Tombstone)",
			operations: []operation{
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // pointID = 1
				{[]byte("key1"), "", core.EntryTypeDelete, 2},       // pointID = 2
			},
			getKey:        []byte("key1"),
			expectedValue: "",
			expectedType:  core.EntryTypeDelete, // Expect to find the tombstone entry
			expectedFound: true,                 // Get() should return found=true for tombstones
		},
		{
			name: "Get after older update",
			operations: []operation{
				{[]byte("key1"), "val2", core.EntryTypePutEvent, 2}, // pointID = 2
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // This should be ignored (pointID = 1)
			},
			getKey:        []byte("key1"),
			expectedValue: "val2",
			expectedType:  core.EntryTypePutEvent,
			expectedFound: true,
		},
		{
			name: "Get after Re-Put (Reincarnation)",
			operations: []operation{
				{[]byte("key1"), "val1", core.EntryTypePutEvent, 1}, // pointID = 1
				{[]byte("key1"), "", core.EntryTypeDelete, 2},       // pointID = 2
				{[]byte("key1"), "val3", core.EntryTypePutEvent, 3}, // pointID = 3
			},
			getKey:        []byte("key1"),
			expectedValue: "val3",
			expectedType:  core.EntryTypePutEvent,
			expectedFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mt := NewMemtable(1024, &utils.SystemClock{})
			for _, op := range tc.operations {
				// Encode the test value string into the expected byte format for FieldValues
				valueBytes := makeTestEventValue(t, op.value)
				if err := mt.Put(op.key, valueBytes, op.entryType, op.pointID); err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}

			value, entryType, found := mt.Get(tc.getKey)
			actualValue := getTestEventValue(t, value)

			if found != tc.expectedFound {
				t.Errorf("Found mismatch: got %v, want %v", found, tc.expectedFound)
			}
			if entryType != tc.expectedType {
				t.Errorf("EntryType mismatch: got %v, want %v", entryType, tc.expectedType)
			}
			assert.Equal(t, tc.expectedValue, actualValue, "Value mismatch")
		})
	}
}
func TestMemtable_Size(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})

	key1 := []byte("testKey1")
	value1 := makeTestEventValue(t, "testValue1")
	key2 := []byte("testKey2")
	value2 := makeTestEventValue(t, "testValue2Longer")

	// Initial size should be 0
	if mt.Size() != 0 {
		t.Errorf("Initial size should be 0, got %d", mt.Size())
	}

	// Helper to calculate expected entry size based on memtable.go logic
	calculateExpectedEntrySize := func(key, value []byte) int {
		return len(key) + len(value) + binary.MaxVarintLen64 /*PointID*/ + 1 /*EntryType*/
	}

	// Put some entries
	mt.Put(key1, value1, core.EntryTypePutEvent, 1)
	currentSize := mt.Size()
	expectedSize1 := calculateExpectedEntrySize(key1, value1)
	if currentSize != int64(expectedSize1) {
		t.Errorf("Size after Put(key1) should be %d, got %d", int64(expectedSize1), currentSize)
	}

	mt.Put(key2, value2, core.EntryTypePutEvent, 2)
	currentSize = mt.Size() // Update currentSize for the second check
	expectedSize2 := calculateExpectedEntrySize(key2, value2)
	totalExpectedSize := expectedSize1 + expectedSize2
	if currentSize != int64(totalExpectedSize) {
		t.Errorf("Size after Put(key2) should be %d, got %d", int64(totalExpectedSize), currentSize)
	}
}

func TestMemtable_Iterator(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})

	entries := []struct {
		key   []byte
		value string
	}{
		{[]byte("key1"), "value1"},
		{[]byte("key2"), "value2"},
		{[]byte("key3"), "value3"},
	}

	// Put some entries
	for i, entry := range entries {
		valueBytes := makeTestEventValue(t, entry.value)
		mt.Put(entry.key, valueBytes, core.EntryTypePutEvent, uint64(i+1))
	}

	// Create an iterator
	iter := mt.NewIterator(nil, nil, core.Ascending)
	defer iter.Close()

	// Iterate and check the entries
	var iteratedEntries []struct {
		key   []byte
		value string
	}

	for iter.Next() {
		key, value, _, _ := iter.At()
		iteratedEntries = append(iteratedEntries, struct {
			key   []byte
			value string
		}{
			key:   key,
			value: getTestEventValue(t, value),
		})
	}

	// Compare the iterated entries with the original entries
	if len(iteratedEntries) != len(entries) {
		t.Errorf("Number of iterated entries mismatch: got %d, want %d", len(iteratedEntries), len(entries))
	}

	for i := range entries {
		if !bytes.Equal(iteratedEntries[i].key, entries[i].key) {
			t.Errorf("Key mismatch at index %d: got %s, want %s", i, iteratedEntries[i].key, entries[i].key)
		}
		if iteratedEntries[i].value != entries[i].value {
			t.Errorf("Value mismatch at index %d: got %s, want %s", i, iteratedEntries[i].value, entries[i].value)
		}
	}
}

func TestMemtable_Iterator_Ranges(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})
	keys := []string{"a", "b", "c", "d", "e", "f"}
	for i, k := range keys {
		valueBytes := makeTestEventValue(t, "v"+k)
		mt.Put([]byte(k), valueBytes, core.EntryTypePutEvent, uint64(i+1))
	}

	testCases := []struct {
		name         string
		startKey     []byte
		endKey       []byte
		order        core.SortOrder
		expectedKeys []string
	}{
		// --- Ascending ---
		{
			name:         "Asc Full range (nil, nil)",
			startKey:     nil,
			endKey:       nil,
			order:        core.Ascending,
			expectedKeys: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:         "Asc From startKey 'c'",
			startKey:     []byte("c"),
			endKey:       nil,
			order:        core.Ascending,
			expectedKeys: []string{"c", "d", "e", "f"},
		},
		{
			name:         "Asc Up to endKey 'd'",
			startKey:     nil,
			endKey:       []byte("d"),
			order:        core.Ascending,
			expectedKeys: []string{"a", "b", "c"}, // endKey is exclusive
		},
		{
			name:         "Asc Between 'b' and 'e'",
			startKey:     []byte("b"),
			endKey:       []byte("e"),
			order:        core.Ascending,
			expectedKeys: []string{"b", "c", "d"},
		},
		{
			name:         "Asc Empty range",
			startKey:     []byte("c"),
			endKey:       []byte("c"),
			order:        core.Ascending,
			expectedKeys: []string{},
		},
		{
			name:         "Asc Start key does not exist",
			startKey:     []byte("b1"),
			endKey:       nil,
			order:        core.Ascending,
			expectedKeys: []string{"c", "d", "e", "f"},
		},
		{
			name:         "Asc Range with no items",
			startKey:     []byte("x"),
			endKey:       []byte("z"),
			order:        core.Ascending,
			expectedKeys: []string{},
		},
		// --- Descending ---
		{
			name:         "Desc Full range (nil, nil)",
			startKey:     nil,
			endKey:       nil,
			order:        core.Descending,
			expectedKeys: []string{"f", "e", "d", "c", "b", "a"},
		},
		{
			name:         "Desc From startKey 'c'",
			startKey:     []byte("c"),
			endKey:       nil,
			order:        core.Descending,
			expectedKeys: []string{"f", "e", "d", "c"},
		},
		{
			name:         "Desc Up to endKey 'd'",
			startKey:     nil,
			endKey:       []byte("d"),
			order:        core.Descending,
			expectedKeys: []string{"c", "b", "a"}, // endKey is exclusive
		},
		{
			name:         "Desc Between 'b' and 'e'",
			startKey:     []byte("b"),
			endKey:       []byte("e"),
			order:        core.Descending,
			expectedKeys: []string{"d", "c", "b"},
		},
		{
			name:         "Desc Empty range",
			startKey:     []byte("c"),
			endKey:       []byte("c"),
			order:        core.Descending,
			expectedKeys: []string{},
		},
		{
			name:         "Desc End key does not exist",
			startKey:     nil,
			endKey:       []byte("d1"),
			order:        core.Descending,
			expectedKeys: []string{"d", "c", "b", "a"},
		},
		{
			name:         "Desc Range with no items",
			startKey:     []byte("x"),
			endKey:       []byte("z"),
			order:        core.Descending,
			expectedKeys: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter := mt.NewIterator(tc.startKey, tc.endKey, tc.order)
			defer iter.Close()

			actualKeys := make([]string, 0)
			for iter.Next() {
				key, _, _, _ := iter.At()
				actualKeys = append(actualKeys, string(key))
			}

			assert.Equal(t, tc.expectedKeys, actualKeys)
		})
	}
}

func TestMemtable_IsFull(t *testing.T) {
	mt := NewMemtable(100, &utils.SystemClock{}) // Small threshold for testing

	// Put entries until it's almost full
	i := 0
	for mt.Size() < 60 { // Fill it up to a known state below the threshold. 80 was too high and overshot the 100 mark.
		key := []byte(fmt.Sprintf("key%d", i))
		value := makeTestEventValue(t, "some_value")
		mt.Put(key, value, core.EntryTypePutEvent, uint64(i+1))
		i++
	}

	if mt.IsFull() {
		t.Fatalf("Memtable should not be full yet, but it is. Size: %d", mt.Size())
	}

	// Add one more entry to push it over the threshold
	key := []byte("final_key_that_makes_it_full")
	value := makeTestEventValue(t, "final_value")
	mt.Put(key, value, core.EntryTypePutEvent, uint64(i+1))

	if !mt.IsFull() {
		t.Errorf("Memtable should be full after the final put, but it's not. Size: %d", mt.Size())
	}
}

// MockSSTableWriter for testing Memtable.FlushToSSTable error handling
type MockSSTableWriter struct {
	failAdd bool
	addErr  error
	entries []struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		pointID   uint64
	}
}

func (m *MockSSTableWriter) Add(key, value []byte, entryType core.EntryType, pointID uint64) error {
	if m.failAdd {
		return m.addErr
	}
	m.entries = append(m.entries, struct {
		key       []byte
		value     []byte
		entryType core.EntryType
		pointID   uint64
	}{key, value, entryType, pointID})
	return nil
}
func (m *MockSSTableWriter) Finish() error      { return nil }
func (m *MockSSTableWriter) Abort() error       { return nil }
func (m *MockSSTableWriter) FilePath() string   { return "mock_path" }
func (m *MockSSTableWriter) CurrentSize() int64 { return 0 } // Not relevant for this test

func TestMemtable_FlushToSSTable_WriterError(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})
	mt.Put([]byte("key1"), makeTestEventValue(t, "val1"), core.EntryTypePutEvent, 1)
	mt.Put([]byte("key2"), makeTestEventValue(t, "val2"), core.EntryTypePutEvent, 2)

	mockErr := fmt.Errorf("simulated writer add error")
	mockWriter := &MockSSTableWriter{failAdd: true, addErr: mockErr}

	err := mt.FlushToSSTable(mockWriter)
	if err == nil {
		t.Fatal("Expected an error from FlushToSSTable, got nil")
	}
	if !strings.Contains(err.Error(), mockErr.Error()) {
		t.Errorf("Expected error to contain '%s', got '%s'", mockErr.Error(), err.Error())
	}
}

func TestMemtable_Close_Pool(t *testing.T) {
	t.Skip("Skipping test: The underlying skiplist library's Clear() method discards its node pool, preventing node reuse across memtable instances. This test's expectation of 0 allocations is therefore no longer valid with the current library version.")

	// This test verifies that the Close method correctly returns objects
	// (MemtableKey, MemtableEntry) to the sync.Pool, allowing for reuse
	// and preventing allocations on subsequent operations.

	// Pre-generate test data to avoid allocations from fmt.Sprintf inside the measured part of the test.
	const numEntries = 10
	testData := make([]struct{ key, value []byte }, numEntries)
	for i := 0; i < numEntries; i++ {
		testData[i].key = []byte(fmt.Sprintf("key-%d", i))
		testData[i].value = []byte(fmt.Sprintf("value-%d", i))
	}

	// 1. Create and populate a memtable. This initial run will have allocations
	// as it gets new objects from the pool.
	mt1 := NewMemtable(1024, &utils.SystemClock{})
	for i, data := range testData {
		valueBytes := makeTestEventValue(t, string(data.value))
		if err := mt1.Put(data.key, valueBytes, core.EntryTypePutEvent, uint64(i+1)); err != nil {
			t.Fatalf("Initial Put failed: %v", err)
		}
	}

	// 2. Close the memtable. This is the method under test.
	// It should return all keys and entries to their respective pools.
	mt1.Close()

	// 3. Now, measure the allocations for populating a new memtable.
	// Because the pools for MemtableKey and MemtableEntry should now be populated, we expect zero allocations for them.
	allocs := testing.AllocsPerRun(1, func() {
		mt2 := NewMemtable(1024, &utils.SystemClock{})
		for i, data := range testData {
			valueBytes := makeTestEventValue(t, string(data.value))
			// These Put calls should get objects from the pool, resulting in no new allocations.
			_ = mt2.Put(data.key, valueBytes, core.EntryTypePutEvent, uint64(i+1))
		}
	})

	if allocs > 0 {
		t.Errorf("Expected 0 allocations when reusing objects from the pool after Close, but got %f", allocs)
	}
}

func TestMemtable_Iterator_ComplexPutDelete(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})

	// Sequence of operations
	// SeqNum is crucial here for determining the latest state of a key
	operations := []struct {
		key       []byte
		value     string
		entryType core.EntryType
		pointID   uint64
	}{
		{[]byte("apple"), "red_v1", core.EntryTypePutEvent, 1},                   // pointID = 1
		{[]byte("banana"), "yellow_v1", core.EntryTypePutEvent, 2},               // pointID = 2
		{[]byte("apple"), "red_v2", core.EntryTypePutEvent, 3},                   // Update apple
		{[]byte("cherry"), "sweet_v1", core.EntryTypePutEvent, 4},                // pointID = 4
		{[]byte("banana"), "", core.EntryTypeDelete, 5},                          // Delete banana
		{[]byte("date"), "brown_v1", core.EntryTypePutEvent, 6},                  // pointID = 6
		{[]byte("apple"), "", core.EntryTypeDelete, 7},                           // Delete apple
		{[]byte("cherry"), "sweet_v2_updated", core.EntryTypePutEvent, 8},        // Still yields the put entry
		{[]byte("elderberry"), "purple_v1", core.EntryTypePutEvent, 9},           // pointID = 9
		{[]byte("banana"), "yellow_v2_reincarnated", core.EntryTypePutEvent, 10}, // Put banana again
	}

	for _, op := range operations {
		valueBytes := makeTestEventValue(t, op.value)
		if err := mt.Put(op.key, valueBytes, op.entryType, op.pointID); err != nil {
			t.Fatalf("Put failed for key %s: %v", string(op.key), err)
		}
	}

	t.Run("Ascending", func(t *testing.T) {
		// Expected state after all operations, considering tombstones and latest SeqNum
		// The MemtableIterator now returns the latest version of each distinct key,
		// including delete entries (tombstones). Filtering is done by higher-level iterators.
		expectedEntries := []struct {
			key       []byte
			value     string
			entryType core.EntryType
			pointID   uint64
		}{
			{[]byte("apple"), "", core.EntryTypeDelete, 7}, // Latest is a delete
			{[]byte("banana"), "yellow_v2_reincarnated", core.EntryTypePutEvent, 10},
			{[]byte("cherry"), "sweet_v2_updated", core.EntryTypePutEvent, 8},
			{[]byte("date"), "brown_v1", core.EntryTypePutEvent, 6},
			{[]byte("elderberry"), "purple_v1", core.EntryTypePutEvent, 9},
		}

		iter := mt.NewIterator(nil, nil, core.Ascending)
		defer iter.Close()

		var actualEntries []MemtableEntry
		for iter.Next() {
			key, value, entryType, pointID := iter.At()
			actualEntries = append(actualEntries, MemtableEntry{
				Key:       append([]byte(nil), key...), // Make copies
				Value:     value,
				EntryType: entryType,
				PointID:   pointID,
			})
		}

		require.Equal(t, len(expectedEntries), len(actualEntries), "Number of iterated entries mismatch")

		for i, expected := range expectedEntries {
			actualValueStr := getTestEventValue(t, actualEntries[i].Value)
			assert.Equal(t, expected.key, actualEntries[i].Key, "Key mismatch at index %d", i)
			assert.Equal(t, expected.value, actualValueStr, "Value mismatch at index %d", i)
			assert.Equal(t, expected.entryType, actualEntries[i].EntryType, "EntryType mismatch at index %d", i)
			assert.Equal(t, expected.pointID, actualEntries[i].PointID, "PointID mismatch at index %d", i)
		}
	})

	t.Run("Descending", func(t *testing.T) {
		expectedEntries := []struct {
			key       []byte
			value     string
			entryType core.EntryType
			pointID   uint64
		}{
			{[]byte("elderberry"), "purple_v1", core.EntryTypePutEvent, 9},
			{[]byte("date"), "brown_v1", core.EntryTypePutEvent, 6},
			{[]byte("cherry"), "sweet_v2_updated", core.EntryTypePutEvent, 8},
			{[]byte("banana"), "yellow_v2_reincarnated", core.EntryTypePutEvent, 10},
			{[]byte("apple"), "", core.EntryTypeDelete, 7},
		}

		iter := mt.NewIterator(nil, nil, core.Descending)
		defer iter.Close()

		var actualEntries []MemtableEntry
		for iter.Next() {
			key, value, entryType, pointID := iter.At()
			actualEntries = append(actualEntries, MemtableEntry{
				Key:       append([]byte(nil), key...), // Make copies
				Value:     value,
				EntryType: entryType,
				PointID:   pointID,
			})
		}

		require.Equal(t, len(expectedEntries), len(actualEntries), "Number of iterated entries mismatch")

		for i, expected := range expectedEntries {
			actualValueStr := getTestEventValue(t, actualEntries[i].Value)
			assert.Equal(t, expected.key, actualEntries[i].Key, "Key mismatch at index %d", i)
			assert.Equal(t, expected.value, actualValueStr, "Value mismatch at index %d", i)
			assert.Equal(t, expected.entryType, actualEntries[i].EntryType, "EntryType mismatch at index %d", i)
			assert.Equal(t, expected.pointID, actualEntries[i].PointID, "PointID mismatch at index %d", i)
		}
	})
}

// TestMemtableIterator_Empty tests the iterator on an empty memtable.
func TestMemtableIterator_Empty(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})

	// Ascending
	iterAsc := mt.NewIterator(nil, nil, core.Ascending)
	assert.False(t, iterAsc.Next(), "Next() on empty memtable should be false for ascending")
	key, val, et, pid := iterAsc.At()
	assert.Nil(t, key)
	assert.Nil(t, val)
	assert.Equal(t, core.EntryType(0), et)
	assert.Equal(t, uint64(0), pid)
	assert.NoError(t, iterAsc.Error())
	assert.NoError(t, iterAsc.Close())

	// Descending
	iterDesc := mt.NewIterator(nil, nil, core.Descending)
	assert.False(t, iterDesc.Next(), "Next() on empty memtable should be false for descending")
	key, val, et, pid = iterDesc.At()
	assert.Nil(t, key)
	assert.Nil(t, val)
	assert.Equal(t, core.EntryType(0), et)
	assert.Equal(t, uint64(0), pid)
	assert.NoError(t, iterDesc.Error())
	assert.NoError(t, iterDesc.Close())
}

// TestMemtableIterator_SingleItem tests the iterator on a memtable with one item.
func TestMemtableIterator_SingleItem(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})
	key1 := []byte("key1")
	val1 := makeTestEventValue(t, "val1")
	mt.Put(key1, val1, core.EntryTypePutEvent, 1)

	// Ascending
	t.Run("Ascending", func(t *testing.T) {
		iter := mt.NewIterator(nil, nil, core.Ascending)
		defer iter.Close()

		// First Next() should succeed
		require.True(t, iter.Next(), "First Next() should succeed")
		key, val, et, pid := iter.At()
		assert.Equal(t, key1, key)
		assert.Equal(t, val1, val)
		assert.Equal(t, core.EntryTypePutEvent, et)
		assert.Equal(t, uint64(1), pid)

		// Second Next() should fail
		assert.False(t, iter.Next(), "Second Next() should fail")
	})

	// Descending
	t.Run("Descending", func(t *testing.T) {
		iter := mt.NewIterator(nil, nil, core.Descending)
		defer iter.Close()

		// First Next() should succeed
		require.True(t, iter.Next(), "First Next() should succeed")
		key, val, et, pid := iter.At()
		assert.Equal(t, key1, key)
		assert.Equal(t, val1, val)
		assert.Equal(t, core.EntryTypePutEvent, et)
		assert.Equal(t, uint64(1), pid)

		// Second Next() should fail
		assert.False(t, iter.Next(), "Second Next() should fail")
	})
}

// TestMemtableIterator_DescendingBoundsEdgeCase tests a specific edge case for descending iteration
// where the initial seek lands on a key that is >= endKey, requiring the iterator to advance
// to the next distinct key before starting.
func TestMemtableIterator_DescendingBoundsEdgeCase(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})
	keys := []string{"a", "b", "d", "e"} // Note: "c" is missing
	for i, k := range keys {
		valueBytes := makeTestEventValue(t, "v"+k)
		mt.Put([]byte(k), valueBytes, core.EntryTypePutEvent, uint64(i+1))
	}

	// Range: startKey="b", endKey="d" (exclusive). Expected: "b"
	// The iterator should start by seeking to "d", realize it's out of bounds,
	// advance to the next distinct key ("b"), and yield that.
	startKey := []byte("b")
	endKey := []byte("d")
	iter := mt.NewIterator(startKey, endKey, core.Descending)
	defer iter.Close()

	// 1. Expect "b"
	require.True(t, iter.Next(), "Should find key 'b'")
	key, _, _, _ := iter.At()
	assert.Equal(t, []byte("b"), key)

	// 2. Expect no more items
	assert.False(t, iter.Next(), "Should be no more items in range")
}

// TestMemtableIterator_Close ensures the Close method works correctly.
func TestMemtableIterator_Close(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})
	mt.Put([]byte("a"), makeTestEventValue(t, "a"), core.EntryTypePutEvent, 1)

	iter := mt.NewIterator(nil, nil, core.Ascending)

	// First close and subsequent closes should be safe and not panic
	require.NoError(t, iter.Close())
	require.NoError(t, iter.Close())

	// After close, valid should be false and At() should return nils
	assert.False(t, iter.valid, "iterator should be invalid after close")
	key, _, _, _ := iter.At()
	assert.Nil(t, key, "key should be nil after close")
}

// TestMemtableIterator_Descending_MultiVersion_Correctness specifically tests the logic
// for descending iteration over a key with multiple versions. This logic was refactored
// to use an internal Prev() call to correctly position the iterator on the latest version
// without extra memory allocations.
func TestMemtableIterator_Descending_MultiVersion_Correctness(t *testing.T) {
	mt := NewMemtable(1024, &utils.SystemClock{})

	// Add multiple versions for "key-b"
	mt.Put([]byte("key-b"), makeTestEventValue(t, "v1"), core.EntryTypePutEvent, 10)
	mt.Put([]byte("key-b"), makeTestEventValue(t, "v2"), core.EntryTypePutEvent, 20)
	mt.Put([]byte("key-b"), makeTestEventValue(t, "v3_latest"), core.EntryTypePutEvent, 30) // This is the latest version

	// Add other keys around it
	mt.Put([]byte("key-a"), makeTestEventValue(t, "va"), core.EntryTypePutEvent, 5)
	mt.Put([]byte("key-c"), makeTestEventValue(t, "vc"), core.EntryTypePutEvent, 40)

	// Expected order for descending scan: key-c, key-b (latest version), key-a
	expected := []struct {
		key     []byte
		value   string
		pointID uint64
	}{
		{[]byte("key-c"), "vc", 40},
		{[]byte("key-b"), "v3_latest", 30},
		{[]byte("key-a"), "va", 5},
	}

	iter := mt.NewIterator(nil, nil, core.Descending)
	defer iter.Close()

	var actual []struct {
		key     []byte
		value   string
		pointID uint64
	}

	for iter.Next() {
		k, v, _, pid := iter.At()
		actual = append(actual, struct {
			key     []byte
			value   string
			pointID uint64
		}{
			key:     append([]byte(nil), k...),
			value:   getTestEventValue(t, v),
			pointID: pid,
		})
	}

	require.NoError(t, iter.Error())
	require.Len(t, actual, len(expected), "Should have iterated over the correct number of distinct keys")

	for i := range expected {
		assert.Equal(t, expected[i].key, actual[i].key, "Key mismatch at index %d", i)
		assert.Equal(t, expected[i].value, actual[i].value, "Value mismatch for key %s", string(expected[i].key))
		assert.Equal(t, expected[i].pointID, actual[i].pointID, "PointID mismatch for key %s", string(expected[i].key))
	}
}

// --- Benchmarks ---

// createBenchmarkMemtable creates and populates a memtable for benchmark tests.
func createBenchmarkMemtable(b *testing.B, numItems int, versionsPerKey int) *Memtable {
	b.Helper()
	mt := NewMemtable(int64(numItems*versionsPerKey*200), &utils.SystemClock{}) // Estimate size
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key-%09d", i))
		for v := 0; v < versionsPerKey; v++ {
			val := makeTestEventValue(b, fmt.Sprintf("value-%09d-v%d", i, v))
			// PointID is descending to make newer versions "smaller" in the skiplist
			pointID := uint64(i*versionsPerKey + (versionsPerKey - v))
			err := mt.Put(key, val, core.EntryTypePutEvent, pointID)
			if err != nil {
				b.Fatalf("Failed to setup benchmark memtable: %v", err)
			}
		}
	}
	return mt
}

func BenchmarkMemtableIterator_Ascending_FullScan(b *testing.B) {
	mt := createBenchmarkMemtable(b, 10000, 1) // 10,000 items, 1 version each
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator(nil, nil, core.Ascending)
		for iter.Next() {
			// Access the data to prevent the compiler from optimizing the loop away.
			_, _, _, _ = iter.At()
		}
		iter.Close() // Must close to release the lock
	}
}

func BenchmarkMemtableIterator_Descending_FullScan(b *testing.B) {
	mt := createBenchmarkMemtable(b, 10000, 1) // 10,000 items, 1 version each
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator(nil, nil, core.Descending)
		for iter.Next() {
			_, _, _, _ = iter.At()
		}
		iter.Close()
	}
}

func BenchmarkMemtableIterator_Ascending_RangeScan(b *testing.B) {
	const numItems = 10000
	mt := createBenchmarkMemtable(b, numItems, 1)
	// Scan about 10% of the keys in the middle
	startKey := []byte(fmt.Sprintf("key-%09d", numItems/2))
	endKey := []byte(fmt.Sprintf("key-%09d", numItems/2+numItems/10))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator(startKey, endKey, core.Ascending)
		for iter.Next() {
			_, _, _, _ = iter.At()
		}
		iter.Close()
	}
}

func BenchmarkMemtableIterator_Ascending_MultiVersion(b *testing.B) {
	// 10,000 distinct keys, but 5 versions each. The iterator must skip 4 versions for each key.
	mt := createBenchmarkMemtable(b, 10000, 5)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator(nil, nil, core.Ascending)
		for iter.Next() {
			_, _, _, _ = iter.At()
		}
		iter.Close()
	}
}
