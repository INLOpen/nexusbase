package engine2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/INLOpen/nexuscore/utils/clock"
)

var (
	// ErrAlreadyExists is returned when a database already exists.
	ErrAlreadyExists = errors.New("database already exists")
	// ErrInvalidName is returned when a database name fails validation.
	ErrInvalidName = errors.New("invalid database name")
)

// Engine2 is a minimal engine implementation that manages per-database filesystem layout.
type Engine2 struct {
	ctx     context.Context
	cancel  context.CancelFunc
	options StorageEngineOptions
	mu      sync.Mutex
	wal     *wal.WAL
	// recovered entries returned by wal.Open at startup
	walRecovered []core.WALEntry
	// walOpenErr stores any error returned by wal.Open during construction.
	// We defer surfacing this error until Start() so tests that expect
	// construction to succeed but Start() to fail can observe the error
	// at the correct lifecycle point.
	walOpenErr error
	mem        *memtable.Memtable2
}

// NewEngine2 constructs a new Engine2 rooted at dataRoot. The provided
// parent context is stored on the engine and used by consumers (adapters)
// as a parent for background goroutines. If ctx is nil, context.Background()
// is used to preserve previous behavior.
func NewEngine2(ctx context.Context, opts StorageEngineOptions) (*Engine2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// derive a cancellable context owned by the engine so callers can
	// request engine-level shutdown which will cancel contexts derived
	// from this parent.
	ctx, cancel := context.WithCancel(ctx)
	options := opts // copy
	dataRoot := opts.DataDir
	if dataRoot == "" {
		return nil, fmt.Errorf("dataRoot must be specified")
	}
	if err := sys.MkdirAll(dataRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data root: %w", err)
	}
	// initialize wal and memtable using the central wal package
	walDir := filepath.Join(dataRoot, "wal")
	wOpts := wal.Options{
		Dir:                walDir,
		Logger:             options.Logger,
		StartRecoveryIndex: options.InitialSequenceNumber,
		HookManager:        options.HookManager,
	}
	if options.WALMaxSegmentSize > 0 {
		wOpts.MaxSegmentSize = options.WALMaxSegmentSize
	}
	if options.WALBatchSize > 0 {
		wOpts.CommitMaxBatchSize = options.WALBatchSize
	}
	if options.WALFlushIntervalMs > 0 {
		wOpts.CommitMaxDelay = time.Duration(options.WALFlushIntervalMs) * time.Millisecond
	}
	if options.WriteBufferSize > 0 {
		wOpts.WriterBufferSize = options.WriteBufferSize
	}
	if options.WALPreallocSize > 0 {
		wOpts.PreallocSize = options.WALPreallocSize
	} else if options.WALMaxSegmentSize > 0 {
		wOpts.PreallocSize = options.WALMaxSegmentSize
	} else if options.SSTablePreallocMultiplier > 0 {
		wOpts.PreallocSize = int64(options.SSTablePreallocMultiplier * 128)
	}
	wOpts.PreallocateSegments = options.WALPreallocateSegments
	if options.Metrics != nil {
		wOpts.BytesWritten = options.Metrics.WALBytesWrittenTotal
		wOpts.EntriesWritten = options.Metrics.WALEntriesWrittenTotal
	}
	if options.WALSyncMode != "" {
		wOpts.SyncMode = options.WALSyncMode
	}

	w, recovered, err := wal.Open(wOpts)
	// Do not fail construction on WAL open errors; store the error and
	// continue. Tests expect NewStorageEngine() to succeed even when WAL
	// recovery later fails; Start() will surface the WAL error.
	var walOpenErr error
	if err != nil {
		walOpenErr = fmt.Errorf("failed to create WAL: %w", err)
	}
	// choose memtable threshold from options if provided
	memThreshold := int64(1 << 30)
	if opts.MemtableThreshold > 0 {
		memThreshold = opts.MemtableThreshold
	}
	var clk clock.Clock = clock.SystemClockDefault
	if opts.Clock != nil {
		clk = opts.Clock
	}
	m := memtable.NewMemtable2(memThreshold, clk)

	// replay recovered WALEntry values into memtable
	// Include all entry types (puts and deletes). Adapter-level replay will
	// perform higher-level index/tag work, but pre-populating the memtable
	// ensures in-memory state is available early. We avoid failing hard on
	// individual decode errors and surface construction error only on fatal
	// conditions.
	for _, e := range recovered {
		// Extract series identifier and components from the encoded key
		seriesID, serr := core.ExtractSeriesIdentifierFromTSDBKeyWithString(e.Key)
		if serr != nil {
			return nil, fmt.Errorf("failed to extract series id from WAL key: %w", serr)
		}
		metricPart, _ := core.ExtractMetricFromSeriesKeyWithString(seriesID)
		tagsMap, _ := core.ExtractTagsFromSeriesKeyWithString(seriesID)
		// timestamp is last 8 bytes
		if len(e.Key) < 8 {
			return nil, fmt.Errorf("wal key too short to contain timestamp")
		}
		tsBytes := e.Key[len(e.Key)-8:]
		ts := int64(0)
		for i := 0; i < 8; i++ {
			ts = (ts << 8) | int64(tsBytes[i])
		}

		// If entry is a delete (explicit or implied by empty value), write a
		// tombstone into the memtable. Otherwise, populate with the decoded
		// field values.
		if e.EntryType == core.EntryTypeDelete || len(e.Value) == 0 {
			// Use PutRaw to avoid re-encoding; memtable expects the full key as written on disk.
			if err := m.PutRaw(e.Key, nil, core.EntryTypeDelete, 0); err != nil {
				return nil, fmt.Errorf("failed to apply recovered WAL delete to memtable: %w", err)
			}
			continue
		}

		// Regular put event: write decoded fields into memtable. If decoding
		// fails, return an error as this indicates a corrupted WAL payload.
		var fv core.FieldValues
		if len(e.Value) > 0 {
			if decoded, derr := core.DecodeFieldsFromBytes(e.Value); derr == nil {
				fv = decoded
			} else {
				return nil, fmt.Errorf("failed to decode WAL entry value: %w", derr)
			}
		}
		dp := &core.DataPoint{Metric: string(metricPart), Tags: tagsMap, Timestamp: ts, Fields: fv}
		if err := m.Put(dp); err != nil {
			return nil, fmt.Errorf("failed to apply recovered WAL entry to memtable: %w", err)
		}
	}

	return &Engine2{ctx: ctx, cancel: cancel, options: options, wal: w, walRecovered: recovered, walOpenErr: walOpenErr, mem: m}, nil
}

// ReplayWal replays recovered WAL entries (from startup) into the provided
// callback. This replaces the old engine2.WAL Replay method that no longer
// exists now that the engine uses the central WAL package directly.
func (e *Engine2) ReplayWal(fn func(*core.DataPoint) error) error {
	for _, eEntry := range e.walRecovered {
		// Decode key into components for all entry types and pass through
		// as a DataPoint. The adapter's replay callback interprets deletes
		// and special range-delete markers based on timestamp/fields.
		seriesID, serr := core.ExtractSeriesIdentifierFromTSDBKeyWithString(eEntry.Key)
		if serr != nil {
			return fmt.Errorf("failed to extract series id from WAL key: %w", serr)
		}
		metricPart, _ := core.ExtractMetricFromSeriesKeyWithString(seriesID)
		tagsMap, _ := core.ExtractTagsFromSeriesKeyWithString(seriesID)
		if len(eEntry.Key) < 8 {
			return fmt.Errorf("wal key too short to contain timestamp")
		}
		tsBytes := eEntry.Key[len(eEntry.Key)-8:]
		ts := int64(0)
		for i := 0; i < 8; i++ {
			ts = (ts << 8) | int64(tsBytes[i])
		}
		var fv core.FieldValues
		if len(eEntry.Value) > 0 {
			if decoded, derr := core.DecodeFieldsFromBytes(eEntry.Value); derr == nil {
				fv = decoded
			} else {
				return fmt.Errorf("failed to decode WAL entry value: %w", derr)
			}
		}
		dp := &core.DataPoint{Metric: string(metricPart), Tags: tagsMap, Timestamp: ts, Fields: fv}
		if err := fn(dp); err != nil {
			return err
		}
	}
	return nil
}

// GetContext returns the engine-level context that was provided at
// construction. Callers may derive cancellable contexts from this as
// needed for background work tied to the engine lifecycle.
func (e *Engine2) GetContext() context.Context { return e.ctx }

// Close cancels the engine-level context, signalling background
// goroutines that derived contexts should stop. Close is safe to call
// multiple times.
func (e *Engine2) Close() error {
	if e == nil {
		return nil
	}
	if e.cancel != nil {
		e.cancel()
		// clear the cancel to indicate we've closed
		e.cancel = nil
	}
	return nil
}

// Shutdown is an alias for Close and provided for callers that prefer
// an explicit shutdown-style name.
func (e *Engine2) Shutdown() error { return e.Close() }

func (e *Engine2) GetDataRoot() string { return e.options.DataDir }

// Validate DB name: starts with letter, followed by letters, digits, underscore or hyphen, max 64 chars.
var dbNameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// CreateDatabase creates the directory layout and writes metadata.
func (e *Engine2) CreateDatabase(ctx context.Context, name string, opts CreateDBOptions) error {
	if !dbNameRe.MatchString(name) {
		return ErrInvalidName
	}
	// reserved names
	if name == "system" || name == "internal" {
		return ErrInvalidName
	}

	dbMetaPath := filepath.Join(e.GetDataRoot(), name, "metadata")

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := os.Stat(dbMetaPath); err == nil {
		// metadata already exists
		if opts.IfNotExists {
			return nil
		}
		return ErrAlreadyExists
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat metadata file: %w", err)
	}

	// create directories
	if err := EnsureDBDirs(e.GetDataRoot(), name); err != nil {
		return fmt.Errorf("failed to create db dirs: %w", err)
	}

	meta := &DatabaseMetadata{
		CreatedAt:    time.Now().Unix(),
		Version:      1,
		LastSequence: 0,
		Options:      opts.Options,
	}

	if err := SaveMetadataAtomic(dbMetaPath, meta); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}
