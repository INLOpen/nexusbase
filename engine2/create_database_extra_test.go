package engine2

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestInvalidDBNames(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_invalid")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: tmpDir})
	e := ai.(*Engine2Adapter).Engine2

	invalidNames := []string{"", "1abc", "abc!", strings.Repeat("a", 65)}
	for _, name := range invalidNames {
		err := e.CreateDatabase(context.Background(), name, CreateDBOptions{})
		if err == nil {
			t.Fatalf("expected error for invalid name '%s', got nil", name)
		}
		if err != ErrInvalidName {
			t.Fatalf("expected ErrInvalidName for '%s', got: %v", name, err)
		}
	}
}

func TestReservedNames(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_reserved")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: tmpDir})
	e := ai.(*Engine2Adapter).Engine2

	reserved := []string{"system", "internal"}
	for _, name := range reserved {
		err := e.CreateDatabase(context.Background(), name, CreateDBOptions{})
		if err == nil {
			t.Fatalf("expected error for reserved name '%s', got nil", name)
		}
		if err != ErrInvalidName {
			t.Fatalf("expected ErrInvalidName for '%s', got: %v", name, err)
		}
	}
}

func TestConcurrentCreate(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_concurrent")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	ai := setupStorageEngineNoStart(t, StorageEngineOptions{DataDir: tmpDir})
	e := ai.(*Engine2Adapter).Engine2

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	successCount := 0
	alreadyCount := 0
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			err := e.CreateDatabase(context.Background(), "concurrentdb", CreateDBOptions{IfNotExists: false})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else if err == ErrAlreadyExists {
				alreadyCount++
			} else {
				// unexpected error
				// avoid transient debug logging in tests
			}
		}()
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 success, got %d (already=%d)", successCount, alreadyCount)
	}

	// sanity: metadata exists
	metaPath := filepath.Join(tmpDir, "concurrentdb", "metadata")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected metadata at %s, stat error: %v", metaPath, err)
	}
}

func TestSaveMetadataAtomicLeavesNoTmp(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "nexusbase_engine2_test_atomic")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "db1", "metadata")
	meta := &DatabaseMetadata{
		CreatedAt:    1234567890,
		Version:      1,
		LastSequence: 42,
		Options:      map[string]string{"k": "v"},
	}
	if err := SaveMetadataAtomic(path, meta); err != nil {
		t.Fatalf("SaveMetadataAtomic failed: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatalf("unexpected .tmp file left behind")
	}
	loaded, err := LoadMetadata(path)
	if err != nil {
		t.Fatalf("LoadMetadata failed: %v", err)
	}
	if loaded.LastSequence != 42 || loaded.Options["k"] != "v" {
		t.Fatalf("metadata mismatch: %+v", loaded)
	}
}

// Ensure tests run with parallelism on multiple CPUs
func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
