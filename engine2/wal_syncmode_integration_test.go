package engine2

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
)

func TestEngine2_WALSyncAlwaysIntegration(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:            tmp,
		WALSyncMode:        core.WALSyncAlways,
		WALBatchSize:       1, // ensure one commit per append
		WALFlushIntervalMs: 0,
		WALMaxSegmentSize:  1024,
	}

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	const N = 10
	for i := 0; i < N; i++ {
		if err := e.wal.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	got := e.wal.TestingOnlySyncCount.Load()
	if got != N {
		t.Fatalf("expected %d per-payload Sync() calls in always mode, got %d", N, got)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("adapter Close failed: %v", err)
	}
}

func TestEngine2_WALSyncIntervalIntegration(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:            tmp,
		WALSyncMode:        core.WALSyncInterval,
		WALFlushIntervalMs: 25, // short interval for test
		WALBatchSize:       1,
		WALMaxSegmentSize:  1024,
	}

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	// Wait up to 1s for at least one periodic sync
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if e.wal.TestingOnlySyncCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e.wal.TestingOnlySyncCount.Load() == 0 {
		t.Fatalf("expected periodic WAL.Sync() to run in interval mode")
	}

	if err := a.Close(); err != nil {
		t.Fatalf("adapter Close failed: %v", err)
	}
}

func TestEngine2_WALSyncDisabledIntegration(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{
		DataDir:            tmp,
		WALSyncMode:        core.WALSyncDisabled,
		WALBatchSize:       1,
		WALFlushIntervalMs: 0,
		WALMaxSegmentSize:  1024,
	}

	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	if e == nil || e.wal == nil {
		t.Fatalf("expected engine with WAL to be created")
	}

	e.wal.TestingOnlySyncCount = &atomic.Uint64{}

	a := NewEngine2Adapter(e)
	if err := a.Start(); err != nil {
		t.Fatalf("adapter Start failed: %v", err)
	}

	const N = 10
	for i := 0; i < N; i++ {
		if err := e.wal.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if e.wal.TestingOnlySyncCount.Load() != 0 {
		t.Fatalf("expected no Sync() calls in disabled mode, got %d", e.wal.TestingOnlySyncCount.Load())
	}

	// explicit Sync() should also be a no-op in disabled mode
	if err := e.wal.Sync(); err != nil {
		t.Fatalf("Sync call failed: %v", err)
	}
	if e.wal.TestingOnlySyncCount.Load() != 0 {
		t.Fatalf("expected no Sync() calls after explicit Sync() in disabled mode")
	}

	if err := a.Close(); err != nil {
		t.Fatalf("adapter Close failed: %v", err)
	}
}
