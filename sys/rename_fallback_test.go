package sys

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestRenameFallback_CopyAndRemoveSuccess verifies that when the platform
// rename fails, the fallback copy+remove path creates the destination and
// removes the source when Remove succeeds.
func TestRenameFallback_CopyAndRemoveSuccess(t *testing.T) {
	// Save/restore renameImpl and Remove
	origRename := GetRenameImpl()
	defer SetRenameImpl(origRename)
	origRemove := Remove
	defer func() { Remove = origRemove }()

	// Force rename to fail so copy fallback is exercised
	SetRenameImpl(func(oldpath, newpath string) error {
		return errors.New("simulated rename failure")
	})

	// Use default Remove behavior (do not override) so retry logic can run

	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.txt")
	dst := filepath.Join(tmp, "dst.txt")

	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := Rename(src, dst); err != nil {
		t.Fatalf("Rename returned error: %v", err)
	}

	// Destination should exist and contain data
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("expected destination to exist: %v", err)
	}
	// Source should have been removed by Remove
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("expected source to be removed, stat err: %v", err)
	}
}

// TestRenameFallback_RemoveFailsNonFatal verifies that when the fallback
// successfully copies but Remove fails (persistent sharing violation), the
// function treats removal as non-fatal and returns success while leaving the
// source file behind for later cleanup.
func TestRenameFallback_RemoveFailsNonFatal(t *testing.T) {
	origRename := GetRenameImpl()
	defer SetRenameImpl(origRename)
	origRemove := Remove
	defer func() { Remove = origRemove }()

	SetRenameImpl(func(oldpath, newpath string) error {
		return errors.New("simulated rename failure")
	})

	// Simulate persistent Remove failure (e.g., sharing violation). This
	// will exercise the non-fatal path introduced for Windows.
	Remove = func(name string) error {
		return errors.New("simulated remove failure")
	}

	tmp := t.TempDir()
	src := filepath.Join(tmp, "src2.txt")
	dst := filepath.Join(tmp, "dst2.txt")

	if err := os.WriteFile(src, []byte("world"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Should return nil (non-fatal) even though Remove failed
	if err := Rename(src, dst); err != nil {
		t.Fatalf("Rename returned error (should be non-fatal on remove fail): %v", err)
	}

	// Destination must exist (copy succeeded)
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("expected destination to exist: %v", err)
	}

	// Source may remain due to simulated persistent remove failure
	if _, err := os.Stat(src); err != nil && !os.IsNotExist(err) {
		// If Stat returned non-notexist error that's unexpected
		t.Fatalf("unexpected stat error for source: %v", err)
	}
}
