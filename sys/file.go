package sys

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// fileWrapper is a stable concrete type used to store the File interface
// inside an atomic.Value. atomic.Value requires that all stored values
// have the same concrete type; wrapping the File interface in this small
// struct ensures we can swap different File implementations safely.
type fileWrapper struct {
	f File
}

// defaultFile stores the current platform `File` implementation wrapped in a
// concrete `fileWrapper`. We store `fileWrapper` (not the interface) so that
// `atomic.Value` always sees the same concrete type across stores.
var defaultFile atomic.Value // stores fileWrapper
var debugMode atomic.Bool

// FileOpener defines an interface for opening files with specific sharing modes.
// This is used to abstract platform-specific file opening behaviors,
// especially for handling file locking on Windows.
type File interface {
	Create(name string) (*os.File, error)
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	OpenWithRetry(path string, flag int, perm os.FileMode, maxRetries int, retryInterval time.Duration) (*os.File, error)
	SafeRemove(name string) error
	SafeRemoveWithOption(name string, opts SafeRemoveOptions) error

	WriteFile(name string, data []byte, perm os.FileMode) error

	GC() error
	// Convenience helpers
	CreateTemp(dir, pattern string) (*os.File, error)
	NewFile(fd uintptr, name string) *os.File
	OpenInRoot(dir, name string) (*os.File, error)
}

type FileHandle interface {
	io.ReadWriteCloser
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.ReaderFrom
	io.WriterTo
	io.StringWriter

	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Name() string
}

type SafeRemoveOptions interface {
	GetRetry() int
	GetIntervalRetry() time.Duration
}

type CreateHandler func(name string) (FileHandle, error)
type OpenHandler func(name string) (FileHandle, error)
type OpenFileHandler func(name string, flag int, perm os.FileMode) (FileHandle, error)
type WriteFileHandler func(name string, data []byte, perm os.FileMode) error
type GCFileHandler func() error
type RemoveHandler func(name string) error

func init() {
	debugMode.Store(false)
	file := NewFile()
	defaultFile.Store(fileWrapper{f: file})
}

func SetDefaultFile(file File) {
	// Store a pointer to the provided File value. Using a pointer
	// Store the provided File value atomically wrapped in fileWrapper.
	defaultFile.Store(fileWrapper{f: file})
}

func SetDebugMode(mode bool) {
	debugMode.Store(mode)
}

var Create CreateHandler = (func(name string) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f

	if debugMode.Load() {
		return DCreate(file, name)
	}
	return RCreate(file, name)
})

var Open OpenHandler = (func(name string) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f
	if debugMode.Load() {
		return DOpen(file, name)
	}
	return ROpen(file, name)
})

var OpenFile OpenFileHandler = (func(name string, flag int, perm os.FileMode) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f
	if debugMode.Load() {
		return DOpenFile(file, name, flag, perm)
	}
	return ROpenFile(file, name, flag, perm)
})

var GC GCFileHandler = (func() error {
	p := defaultFile.Load()
	if p == nil {
		return os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return os.ErrInvalid
	}
	return fw.f.GC()
})

var WriteFile WriteFileHandler = (func(name string, data []byte, perm os.FileMode) error {
	p := defaultFile.Load()
	if p == nil {
		return os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return os.ErrInvalid
	}
	return fw.f.WriteFile(name, data, perm)
})

type ReadFileHandler func(name string) ([]byte, error)

var ReadFile ReadFileHandler = (func(name string) ([]byte, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	f, err := fw.f.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
})

var Remove RemoveHandler = (func(name string) error {
	// Retry a few times for transient file lock errors (common on Windows/macOS).
	const maxAttempts = 5
	const retryInterval = 100 * time.Millisecond
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		lastErr = os.Remove(name)
		if lastErr == nil || os.IsNotExist(lastErr) {
			return nil
		}
		time.Sleep(retryInterval)
	}
	return fmt.Errorf("failed to remove file %s after %d attempts: %w", name, maxAttempts, lastErr)
})

// MkdirAll creates a directory and parents, delegating to os.MkdirAll by default.
func MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }

// Rename moves a file or directory from old to new path.
func Rename(oldpath, newpath string) error {
	const maxAttempts = 5
	const retryInterval = 100 * time.Millisecond
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		lastErr = renameImpl(oldpath, newpath)
		if lastErr == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
	// If os.Rename fails (possible cross-device or simulated failures), attempt a copy+remove fallback for files.
	// This helps in environments where rename is not possible across mountpoints or when transient errors occur.
	// Only attempt fallback for regular files; for directories, return the original error.
	fi, statErr := os.Stat(oldpath)
	if statErr != nil {
		return fmt.Errorf("failed to rename %s -> %s after %d attempts: %w", oldpath, newpath, maxAttempts, lastErr)
	}
	if fi.IsDir() {
		return fmt.Errorf("failed to rename directory %s -> %s after %d attempts: %w", oldpath, newpath, maxAttempts, lastErr)
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(newpath), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory for rename fallback %s: %w", filepath.Dir(newpath), err)
	}

	// Attempt copy
	src, err := os.Open(oldpath)
	if err != nil {
		return fmt.Errorf("failed to open source for rename fallback %s -> %s: %w", oldpath, newpath, err)
	}
	defer src.Close()

	dst, err := os.Create(newpath)
	if err != nil {
		return fmt.Errorf("failed to create destination for rename fallback %s -> %s: %w", oldpath, newpath, err)
	}
	// If copy fails, ensure we close and remove partial dst
	copyErr := func() error {
		defer dst.Close()
		if _, err := io.Copy(dst, src); err != nil {
			_ = os.Remove(newpath)
			return err
		}
		if err := dst.Sync(); err != nil {
			// Not fatal; log by returning the error to caller
			return err
		}
		return nil
	}()
	if copyErr != nil {
		return fmt.Errorf("failed to copy file during rename fallback %s -> %s: %w", oldpath, newpath, copyErr)
	}

	// Close source file before attempting removal to reduce chance of
	// sharing violations on Windows. dst is closed inside the copy helper.
	_ = src.Close()
	// Attempt to remove the source. Run a GC to help release stale handles
	// (Windows can keep handles alive for a short time after Close()). Use
	// the package-level Remove helper which performs retries.
	_ = GC()
	if err := Remove(oldpath); err != nil {
		// Best-effort cleanup failed. On platforms like Windows transient
		// sharing violations can prevent immediate removal even though the
		// copied destination is valid. Treat this as a non-fatal warning
		// and return success so callers can proceed with the new file.
		return nil
	}

	return nil
}

// Stat returns file info for the given path.
func Stat(path string) (os.FileInfo, error) { return os.Stat(path) }

// renameImpl is the platform rename implementation used by Rename. It is
// a variable so tests can override it to simulate rename failures and exercise
// the copy-fallback path. By default it calls os.Rename.
var renameImpl = os.Rename

// SetRenameImpl sets the underlying rename implementation used by Rename.
// Tests may use this to simulate failures. Passing nil restores the default os.Rename.
func SetRenameImpl(fn func(oldpath, newpath string) error) {
	if fn == nil {
		renameImpl = os.Rename
	} else {
		renameImpl = fn
	}
}

// GetRenameImpl returns the currently configured rename implementation.
// Useful for tests to save/restore the previous value.
func GetRenameImpl() func(oldpath, newpath string) error { return renameImpl }
