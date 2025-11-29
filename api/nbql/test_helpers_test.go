package nbql

import (
	"testing"

	"github.com/INLOpen/nexusbase/engine2"
)

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
