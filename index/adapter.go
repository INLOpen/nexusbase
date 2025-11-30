package index

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"log/slog"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexuscore/types"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

// AdapterConfig holds dependencies for the adapter that converts index-style
// data into engine-compatible SSTables.
type AdapterConfig struct {
	// SSTableWriterFactory is required to create SSTable writers compatible with the engine.
	SSTableWriterFactory core.SSTableWriterFactory
	// DataDir is the directory where sstable files will be written.
	DataDir string
	// Optional tracer propagated into created writers (may be nil)
	Tracer trace.Tracer
	// Optional logger propagated into created writers (may be nil)
	Logger *slog.Logger
}

// WriteTagIndexSSTable writes a single SSTable containing the provided mapping
// of encoded tag index keys to roaring64.Bitmaps. It returns the created
// SSTable path and the ID used.
func WriteTagIndexSSTable(cfg AdapterConfig, mapping map[string]*roaring64.Bitmap, id uint64) (string, uint64, error) {
	// If no factory is provided, default to a factory that mirrors the
	// indexer's default writer factory behavior (propagate tracer/logger
	// and apply sane defaults). This keeps adapter-written SSTables
	// consistent with indexer-produced SSTables.
	factory := cfg.SSTableWriterFactory
	if factory == nil {
		factory = defaultIndexSSTableWriterFactory(cfg.Tracer, cfg.Logger)
	}

	writerOpts := core.SSTableWriterOptions{
		DataDir:                      cfg.DataDir,
		ID:                           id,
		Tracer:                       cfg.Tracer,
		Logger:                       cfg.Logger,
		EstimatedKeys:                1024,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    4 * 1024,
		Compressor:                   &compressors.NoCompressionCompressor{},
	}
	writer, err := factory(writerOpts)
	if err != nil {
		return "", 0, fmt.Errorf("create sstable writer: %w", err)
	}

	// Abort the writer on unexpected failures; mark finished when Finish succeeds.
	finished := false
	defer func() {
		if !finished {
			_ = writer.Abort()
		}
	}()

	for key, bm := range mapping {
		buf, err := bm.ToBytes()
		if err != nil {
			return "", 0, fmt.Errorf("serialize bitmap: %w", err)
		}
		if err := writer.Add([]byte(key), buf, core.EntryTypePutEvent, 0); err != nil {
			return "", 0, fmt.Errorf("writer add: %w", err)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Abort()
		return "", 0, fmt.Errorf("writer finish: %w", err)
	}
	finished = true

	// Try to determine final sstable path. Writer.FilePath() should point to
	// the final file after Finish(), but some writers may place files in
	// subdirectories; search for the expected filename as a fallback.
	expectedName := fmt.Sprintf("%d.sst", id)
	foundPath := ""
	// First try writer.FilePath() and ensure the file exists there
	wp := writer.FilePath()
	if wp != "" {
		if _, err := os.Stat(wp); err == nil {
			foundPath = wp
		}
	}
	// If file wasn't found at wp, walk the data dir to find expectedName
	if foundPath == "" {
		// fallback: search
		// Note: simple non-recursive approach first
		// Use filepath.Glob-like iteration
		// Walk the directory
		filepath.WalkDir(cfg.DataDir, func(p string, d os.DirEntry, err error) error {
			if err != nil || d == nil {
				return nil
			}
			if !d.IsDir() && d.Name() == expectedName {
				foundPath = p
				return filepath.SkipDir
			}
			return nil
		})
	}

	if foundPath == "" {
		return "", id, fmt.Errorf("sstable file not found after Finish: expected %s under %s", expectedName, cfg.DataDir)
	}

	// Verify we can load it via sstable.LoadSSTable
	loadOpts := sstable.LoadSSTableOptions{FilePath: foundPath, ID: id, Tracer: nil, Logger: nil}
	if _, err := sstable.LoadSSTable(loadOpts); err != nil {
		return foundPath, id, fmt.Errorf("verify load: %w", err)
	}
	return foundPath, id, nil
}

// ReadTagIndexSSTable loads the SSTable at `path` (with the given id) and
// returns a map of encoded index keys (as strings) to their deserialized
// `roaring64.Bitmap` values. Caller owns the returned bitmaps.
func ReadTagIndexSSTable(path string, id uint64) (map[string]*roaring64.Bitmap, error) {
	loadOpts := sstable.LoadSSTableOptions{FilePath: path, ID: id, Tracer: nil, Logger: nil}
	sst, err := sstable.LoadSSTable(loadOpts)
	if err != nil {
		return nil, fmt.Errorf("load sstable: %w", err)
	}
	defer sst.Close()

	it, err := sst.NewIterator(nil, nil, nil, types.Ascending)
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	defer it.Close()

	out := make(map[string]*roaring64.Bitmap)
	for it.Next() {
		cur, aerr := it.At()
		if aerr != nil {
			return nil, fmt.Errorf("iterator At(): %w", aerr)
		}
		if cur.EntryType != core.EntryTypePutEvent {
			continue
		}
		bm := roaring64.New()
		if _, rerr := bm.ReadFrom(bytes.NewReader(cur.Value)); rerr != nil {
			return nil, fmt.Errorf("failed to deserialize bitmap for key %s: %w", string(cur.Key), rerr)
		}
		out[string(cur.Key)] = bm
	}
	if it.Error() != nil {
		return nil, fmt.Errorf("iterator error: %w", it.Error())
	}
	return out, nil
}

// defaultIndexSSTableWriterFactory mirrors the behavior used by the
// indexer.TagIndexManager: it ensures tracer/logger are set on the
// writer options and applies sane defaults for block size, compressor
// and estimated keys if the caller didn't provide them.
func defaultIndexSSTableWriterFactory(tracer trace.Tracer, logger *slog.Logger) core.SSTableWriterFactory {
	return func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
		opts.Tracer = tracer
		if logger != nil {
			opts.Logger = logger.With("component", "TagIndexManager-SSTableWriter")
		}
		if opts.BlockSize == 0 {
			opts.BlockSize = 4 * 1024
		}
		if opts.Compressor == nil {
			opts.Compressor = &compressors.NoCompressionCompressor{}
		}
		if opts.EstimatedKeys == 0 {
			opts.EstimatedKeys = 1000
		}
		return sstable.NewSSTableWriter(opts)
	}
}
