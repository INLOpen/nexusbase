package server_test

import (
	"testing"

	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/testing/testhelpers"
)

// setupEngineStart creates and starts an engine2 instance for tests.
// Caller is responsible for calling Close() on the returned interface.
func setupEngineStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
	t.Helper()
	return testhelpers.SetupEngineStart(t, opts)
}

// setupEngineNoStart creates a StorageEngine but does not start it.
// Useful when the test needs to manipulate internals before Start()
// or assert Start() error behavior.
func setupEngineNoStart(t *testing.T, opts engine2.StorageEngineOptions) engine2.StorageEngineInterface {
	t.Helper()
	return testhelpers.SetupEngineNoStart(t, opts)
}
