package engine2

import (
	"testing"
)

// setupStorageEngineStart creates a StorageEngine from opts, starts it, and
// fatals on error. Caller is responsible for calling Close() when done.
func setupStorageEngineStart(t *testing.T, opts StorageEngineOptions) StorageEngineInterface {
	t.Helper()

	eng, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		t.Fatalf("Failed to start setup engine: %v", err)
	}
	return eng
}

// setupStorageEngineNoStart creates a StorageEngine but does not start it.
// Useful when tests need to manipulate on-disk layout before starting.
func setupStorageEngineNoStart(t *testing.T, opts StorageEngineOptions) StorageEngineInterface {
	t.Helper()

	eng, err := NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	return eng
}
