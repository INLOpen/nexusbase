package internal

import (
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sys"
)

type PrivateManagerStore interface {
	GetLogFilePath() string
}

type PrivateTagIndexManager interface {
	GetShutdownChain() chan struct{}
	GetWaitGroup() *sync.WaitGroup
	GetLevelsManager() levels.Manager
}

type PrivateLevelManager interface {
	SetBaseTargetSize(size int64)
	GetBaseTargetSize() int64
}

type PrivateStorageEngine interface {
}

type PrivateWAL interface {
	SetTestingOnlyInjectAppendError(err error)
}

type PrivateSnapshotHelper interface {
	RemoveAll(path string) error
	ReadFile(name string) ([]byte, error)
	MkdirTemp(dir, pattern string) (string, error)
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	Open(name string) (sys.FileInterface, error)
	Create(name string) (sys.FileInterface, error)
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(name string, data []byte, perm os.FileMode) error

	CopyDirectoryContents(src, dst string) error
	LinkOrCopyFile(src, dst string) error
	LinkOrCopyDirectoryContents(src, dst string) error

	ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error)
	CopyFile(src, dst string) error
	CopyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error
	SaveJSON(v interface{}, path string) error
}
