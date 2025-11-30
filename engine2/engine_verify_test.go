package engine2

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/require"
)

// Ported focused subtests from engine/engine_test.go that exercise indexer
// helpers and LevelsManager consistency checks.
func TestStorageEngine_VerifyDataConsistency_Port(t *testing.T) {
	t.Run("corrupted_sstable_checksum_detected_on_verify", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := GetBaseOptsForTest(t, "verify_corrupted_checksum_")
		opts.DataDir = tempDir
		opts.ErrorOnSSTableLoadFailure = false

		// Create an SSTable directly to avoid relying on the engine WAL for
		// this focused index verification test. Writing the SSTable directly
		// eliminates WAL recovery from the equation and keeps the test
		// deterministic.
		sstDir := filepath.Join(tempDir, "sst")
		require.NoError(t, os.MkdirAll(sstDir, 0o755))

		id := uint64(time.Now().UnixNano())
		writerOpts := core.SSTableWriterOptions{
			DataDir:                      sstDir,
			ID:                           id,
			EstimatedKeys:                2,
			BloomFilterFalsePositiveRate: 0.01,
			BlockSize:                    32 * 1024,
			Compressor:                   &compressors.NoCompressionCompressor{},
			Logger:                       nil,
		}
		writer, werr := sstable.NewSSTableWriter(writerOpts)
		require.NoError(t, werr)
		// add two keys to establish min/max range
		require.NoError(t, writer.Add([]byte("a"), []byte("v1"), core.EntryTypePutEvent, 0))
		require.NoError(t, writer.Add([]byte("b"), []byte("v2"), core.EntryTypePutEvent, 0))
		require.NoError(t, writer.Finish())

		// Append manifest entry so engine startup can discover this SSTable
		manifestPath := filepath.Join(opts.DataDir, "sstables", "manifest.json")
		if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
			t.Fatalf("failed to create manifest directory: %v", err)
		}
		base := filepath.Base(writer.FilePath())
		base = strings.TrimSuffix(base, filepath.Ext(base))
		wid, perr := strconv.ParseUint(base, 10, 64)
		if perr != nil {
			t.Fatalf("failed to parse sstable id from filename %s: %v", base, perr)
		}
		entry := SSTableManifestEntry{ID: wid, FilePath: writer.FilePath(), KeyCount: uint64(2), CreatedAt: time.Now().UTC()}
		require.NoError(t, AppendManifestEntry(manifestPath, entry))

		// Find created SSTable file
		files, err := os.ReadDir(sstDir)
		require.NoError(t, err)
		require.NotEmpty(t, files)
		var validSSTPath string
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".sst") {
				validSSTPath = filepath.Join(sstDir, f.Name())
				break
			}
		}
		require.NotEmpty(t, validSSTPath)

		// Load SSTable to inspect blocks, then corrupt one byte in file
		sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: validSSTPath, ID: 1})
		require.NoError(t, err)
		require.NotEmpty(t, sst.GetIndex().GetEntries())
		firstBlockMeta := sst.GetIndex().GetEntries()[0]
		sst.Close()

		fileData, err := os.ReadFile(validSSTPath)
		require.NoError(t, err)
		offset := int(firstBlockMeta.BlockOffset) + 1
		if offset < len(fileData) {
			fileData[offset]++
		}
		require.NoError(t, os.WriteFile(validSSTPath, fileData, 0644))

		// Start engine and run VerifyDataConsistency
		engine := setupStorageEngineStart(t, opts)
		defer engine.Close()

		errs := engine.VerifyDataConsistency()
		require.NotEmpty(t, errs)
	})

	t.Run("inconsistent_l1_overlap_detected_by_levels_manager", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := GetBaseOptsForTest(t, "verify_inconsistent_l1_")
		opts.DataDir = tempDir
		opts.ErrorOnSSTableLoadFailure = false

		// Create three sstables (A,B,C) using separate temporary engines so
		// each flush produces an independent .sst file with desired keys.
		// A: keys "apple" -> "banana"
		// B: keys "banana" -> "cherry" (overlaps A)
		// C: keys "date" -> "elderberry" (no overlap)
		prefixes := [][]string{{"apple", "banana"}, {"banana", "cherry"}, {"date", "elderberry"}}
		dstSstDir := filepath.Join(opts.DataDir, "sst")
		require.NoError(t, os.MkdirAll(dstSstDir, 0o755))
		totalCopied := 0
		for i, pair := range prefixes {
			// create an sstable directly with simple literal keys so Min/Max
			// reflect the human-friendly strings and are deterministic.
			id := uint64(time.Now().UnixNano()) + uint64(i)
			writerOpts := core.SSTableWriterOptions{
				DataDir:                      dstSstDir,
				ID:                           id,
				EstimatedKeys:                2,
				BloomFilterFalsePositiveRate: 0.01,
				BlockSize:                    32 * 1024,
				Compressor:                   &compressors.NoCompressionCompressor{},
				Logger:                       nil,
			}
			writer, werr := sstable.NewSSTableWriter(writerOpts)
			require.NoError(t, werr)
			// add two keys to establish min/max range
			require.NoError(t, writer.Add([]byte(pair[0]), []byte("v1"), core.EntryTypePutEvent, 0))
			require.NoError(t, writer.Add([]byte(pair[1]), []byte("v2"), core.EntryTypePutEvent, 0))
			require.NoError(t, writer.Finish())
			// Append manifest entry so engine startup can discover this SSTable
			manifestPath := filepath.Join(opts.DataDir, "sstables", "manifest.json")
			// Ensure manifest directory exists before appending entries.
			if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
				t.Fatalf("failed to create manifest directory: %v", err)
			}
			// Writer interface does not expose ID; derive the ID from the final filename (<id>.sst).
			base := filepath.Base(writer.FilePath())
			base = strings.TrimSuffix(base, filepath.Ext(base))
			id, perr := strconv.ParseUint(base, 10, 64)
			if perr != nil {
				t.Fatalf("failed to parse sstable id from filename %s: %v", base, perr)
			}
			entry := SSTableManifestEntry{ID: id, FilePath: writer.FilePath(), KeyCount: uint64(2), CreatedAt: time.Now().UTC()}
			require.NoError(t, AppendManifestEntry(manifestPath, entry))
			totalCopied++
		}
		require.Greater(t, totalCopied, 0)

		// Now open the target engine adapter and Start it so the startup path
		// will reload the manifest and register discovered SSTables into L1.
		ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: opts.DataDir})
		a := ai.(*Engine2Adapter)
		defer a.Close()

		errs := a.GetLevelsManager().VerifyConsistency()
		require.NotEmpty(t, errs)
	})

	t.Run("corrupted_sstable_index_firstkey_mismatch_in_memory", func(t *testing.T) {
		tempDir := t.TempDir()
		localOpts := GetBaseOptsForTest(t, "verify_corrupted_index_")
		localOpts.DataDir = tempDir
		localOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_index_")
		localOpts.ErrorOnSSTableLoadFailure = false
		localOpts.CompactionIntervalSeconds = 3600
		localOpts.SSTableDefaultBlockSize = 32

		// Create a temporary engine to produce an SSTable with multiple blocks
		setupOpts := localOpts
		setupOpts.DataDir = t.TempDir()
		setupOpts.Metrics = NewEngineMetrics(false, "verify_corrupted_index_setup_")
		eng1 := setupStorageEngineStart(t, setupOpts)
		// Add points to create multiple blocks
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "A"}, 100, map[string]interface{}{"value": 1.0})))
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "B"}, 200, map[string]interface{}{"value": 2.0})))
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "C"}, 300, map[string]interface{}{"value": 3.0})))
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "X"}, 400, map[string]interface{}{"value": 4.0})))
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "Y"}, 500, map[string]interface{}{"value": 5.0})))
		require.NoError(t, eng1.Put(context.Background(), HelperDataPoint(t, "index.corrupt.metric", map[string]string{"id": "Z"}, 600, map[string]interface{}{"value": 6.0})))
		require.NoError(t, eng1.Close())

		// Locate the created SSTable file
		sstDir := filepath.Join(setupOpts.DataDir, "sst")
		files, err := os.ReadDir(sstDir)
		require.NoError(t, err)
		require.NotEmpty(t, files)
		var validSSTPath string
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".sst") {
				validSSTPath = filepath.Join(sstDir, f.Name())
				break
			}
		}
		require.NotEmpty(t, validSSTPath)

		// Load SSTable and corrupt the in-memory index first key
		sst, err := sstable.LoadSSTable(sstable.LoadSSTableOptions{FilePath: validSSTPath, ID: 1})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(sst.GetIndex().GetEntries()), 2)
		// Corrupt the first key of the second index entry
		sst.GetIndex().GetEntries()[1].FirstKey = []byte("corrupted_first_key")
		defer sst.Close()

		// Start a fresh engine and add the corrupted SSTable into its levels manager
		// Use the engine2 runtime so we can access LevelsManager to inject the corrupted SSTable
		ai := setupStorageEngineStart(t, StorageEngineOptions{DataDir: localOpts.DataDir})
		a := ai.(*Engine2Adapter)
		defer a.Close()

		lm := a.GetLevelsManager()
		require.NotNil(t, lm)
		require.NoError(t, lm.AddTableToLevel(1, sst))

		errs := lm.VerifyConsistency()
		require.NotEmpty(t, errs)
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), "mismatch with actual block first key") {
				found = true
				break
			}
		}
		require.True(t, found, "expected index-first-key mismatch error in VerifyDataConsistency errors")
	})
}
