package engine2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateDatabaseHappyPath(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_create")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: tmpDir})
	e := ai.(*Engine2Adapter).Engine2

	ctx := context.Background()
	if err := e.CreateDatabase(ctx, "testdb", CreateDBOptions{IfNotExists: false, Options: map[string]string{"a": "b"}}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// metadata should exist
	metaPath := filepath.Join(tmpDir, "testdb", "metadata")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected metadata file at %s, got err: %v", metaPath, err)
	}

	// dirs
	walDir := filepath.Join(tmpDir, "testdb", "wal")
	sstDir := filepath.Join(tmpDir, "testdb", "sstables")
	if _, err := os.Stat(walDir); err != nil {
		t.Fatalf("expected wal dir at %s, err: %v", walDir, err)
	}
	if _, err := os.Stat(sstDir); err != nil {
		t.Fatalf("expected sst dir at %s, err: %v", sstDir, err)
	}

	// load metadata
	meta, err := LoadMetadata(metaPath)
	if err != nil {
		t.Fatalf("LoadMetadata failed: %v", err)
	}
	if meta.Options["a"] != "b" {
		t.Fatalf("expected option a=b, got: %v", meta.Options)
	}
	if time.Now().Unix()-meta.CreatedAt > 10 {
		t.Fatalf("createdAt looks wrong: %d", meta.CreatedAt)
	}
}

func TestCreateDatabaseIdempotentIfNotExists(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_create_idempotent")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: tmpDir})
	e := ai.(*Engine2Adapter).Engine2

	ctx := context.Background()
	if err := e.CreateDatabase(ctx, "db1", CreateDBOptions{IfNotExists: false}); err != nil {
		t.Fatalf("first CreateDatabase failed: %v", err)
	}
	// second call with IfNotExists true should succeed
	if err := e.CreateDatabase(ctx, "db1", CreateDBOptions{IfNotExists: true}); err != nil {
		t.Fatalf("second CreateDatabase with IfNotExists failed: %v", err)
	}
}
