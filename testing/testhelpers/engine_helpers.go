package testhelpers

import (
	"testing"

	"github.com/INLOpen/nexusbase/engine2"
)

// SetupEngineStart creates a StorageEngine from opts, starts it, and
// fatals on error. Caller is responsible for closing the engine when done.
func SetupEngineStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
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

// SetupEngineNoStart creates a StorageEngine but does not start it.
// Useful when the test needs to manipulate internals before Start()
// or assert Start() error behavior.
func SetupEngineNoStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
	t.Helper()
	eng, err := engine2.NewStorageEngine(opts)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}
	return eng
}
