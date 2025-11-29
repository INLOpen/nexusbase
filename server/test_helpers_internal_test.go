package server

import (
	"testing"

	"github.com/INLOpen/nexusbase/engine2"
)

// setupEngineStart creates a StorageEngine from opts, starts it, and
// fatals on error. Caller is responsible for closing the engine when done.
func setupEngineStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
	t.Helper()
	eng, err := engine2.NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err = eng.Start(); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}
	return eng
}

// setupEngineNoStart creates a StorageEngine but does not start it.
// Useful when the test needs to hand the engine to other components
// which will manage starting it.
func setupEngineNoStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
	t.Helper()
	eng, err := engine2.NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	return eng
}
