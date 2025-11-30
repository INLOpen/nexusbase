package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

func TestValidateAfterStart_Debug(t *testing.T) {
	opts := GetBaseOptsForTest(t, "debug")
	eng := setupStorageEngineStart(t, opts)
	defer eng.Close()

	v := core.NewValidator()
	var err error
	err = core.ValidateMetricAndTags(v, "", map[string]string{"id": "A"})
	if err == nil {
		t.Fatalf("expected validation error after Start, got nil")
	}
}
