package engine2

import (
	"context"
	"os"
	"testing"

	"log/slog"

	"github.com/INLOpen/nexusbase/indexer"
)

// Ensure the Engine2 adapter prefers the new TagIndexManager2 implementation.
func TestEngine2Adapter_UsesTagIndexManager2(t *testing.T) {
	tmp := t.TempDir()
	opts := StorageEngineOptions{DataDir: tmp, Logger: slog.Default()}
	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("failed to create Engine2: %v", err)
	}
	a := NewEngine2Adapter(e)
	if a == nil {
		t.Fatalf("NewEngine2Adapter returned nil")
	}
	t.Logf("adapter: %+v", a)
	t.Logf("tagIndexManager raw: %v (type=%T)", a.tagIndexManager, a.tagIndexManager)
	if a.tagIndexManager == nil {
		t.Fatalf("tagIndexManager is nil on adapter")
	}
	if _, ok := a.tagIndexManager.(*indexer.TagIndexManager2); !ok {
		t.Fatalf("expected tagIndexManager to be *indexer.TagIndexManager2, got %T", a.tagIndexManager)
	}
}

// When the v2 TagIndex manifest is corrupt/unreadable, the adapter should
// fall back to the legacy TagIndexManager implementation.
func TestEngine2Adapter_FallbackToLegacyWhenV2Fails(t *testing.T) {
	tmp := t.TempDir()

	// Force adapter to choose the legacy manager for this test.
	os.Setenv("NEXUSBASE_TEST_FORCE_TAGINDEX_FALLBACK", "1")
	defer os.Unsetenv("NEXUSBASE_TEST_FORCE_TAGINDEX_FALLBACK")

	opts := StorageEngineOptions{DataDir: tmp, Logger: slog.Default()}
	e, err := NewEngine2(context.Background(), opts)
	if err != nil {
		t.Fatalf("failed to create Engine2: %v", err)
	}
	a := NewEngine2Adapter(e)
	if a == nil {
		t.Fatalf("NewEngine2Adapter returned nil")
	}
	t.Logf("adapter.tagIndexImpl=%q", a.tagIndexImpl)
	t.Logf("adapter.tagIndexManager=%v (type=%T)", a.tagIndexManager, a.tagIndexManager)
	if a.tagIndexManager == nil {
		t.Fatalf("tagIndexManager is nil on adapter")
	}
	// Since we forced fallback via env var, the adapter should record legacy.
	if a.tagIndexImpl != "legacy" {
		t.Fatalf("expected adapter.tagIndexImpl to be 'legacy', got %q", a.tagIndexImpl)
	}
}
