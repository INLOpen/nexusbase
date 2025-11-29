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

	// Note: recovered entries are preserved in `walRecovered` for callers to
	// replay into higher-level adapter state (indexes, memtable, sequence
	// numbers). We purposely avoid pre-populating the memtable here to ensure
	// a single well-defined replay path exists (the adapter's ReplayWal).
	// This prevents double-application of recovered entries and mismatches
	// in sequence-number allocation.

	return &Engine2{ctx: ctx, cancel: cancel, options: options, wal: w, walRecovered: recovered, walOpenErr: walOpenErr, mem: m}, nil
}

// ReplayWal replays recovered WALEntry objects (from startup) into the
// provided callback. The callback receives the raw *core.WALEntry so callers
// (adapters) with access to string stores can decode keys using their
// local dictionaries. This avoids forcing Engine2 to depend on adapter
// string mappings and preserves exact on-disk key bytes.
func (e *Engine2) ReplayWal(fn func(*core.WALEntry) error) error {
	for _, entry := range e.walRecovered {
		if err := fn(&entry); err != nil {
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
