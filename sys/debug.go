package sys

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

var _ FileHandle = (*DebugFile)(nil)
var nextID atomic.Uint64

var listFD *sync.Map = new(sync.Map)

type DebugFile struct {
	id     uint64
	f      *os.File
	logger *slog.Logger
}

func DCreate(sysFile File, name string) (FileHandle, error) {
	return DOpenFile(sysFile, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func DOpen(sysFile File, name string) (FileHandle, error) {
	return DOpenFile(sysFile, name, os.O_RDONLY, 0)
}

func DOpenFile(sysFile File, name string, flag int, perm os.FileMode) (FileHandle, error) {
	// Assume a default logger if none is provided, or you could pass it in.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("component", "DebugFile")

	f, err := sysFile.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// Atomically increment and use the returned value as the ID.
	id := nextID.Add(1)
	logger = logger.With("id", id)
	logger = logger.With("file_name", name)
	logger.Debug("Opening file")
	listFD.Store(id, f.Name())
	// logger.Debug(string(debug.Stack()))

	return &DebugFile{
		id:     id,
		f:      f,
		logger: logger,
	}, nil
}

func (df *DebugFile) Write(p []byte) (n int, err error) {
	n, err = df.f.Write(p)
	if err != nil {
		// For WAL files, include a short hex dump to aid debugging.
		name := df.f.Name()
		if strings.Contains(name, string(os.PathSeparator)+"wal"+string(os.PathSeparator)) || strings.HasSuffix(name, ".wal") {
			max := len(p)
			if max > 64 {
				max = 64
			}
			var sb strings.Builder
			for i := 0; i < max; i++ {
				fmt.Fprintf(&sb, "%02X ", p[i])
			}
			df.logger.Debug("Write failed", "bytes", len(p), "n", n, "error", err, "hex", sb.String())
		} else {
			df.logger.Debug("Write failed", "bytes", len(p), "n", n, "error", err)
		}
	} else {
		name := df.f.Name()
		if strings.Contains(name, string(os.PathSeparator)+"wal"+string(os.PathSeparator)) || strings.HasSuffix(name, ".wal") {
			max := len(p)
			if max > 64 {
				max = 64
			}
			var sb strings.Builder
			for i := 0; i < max; i++ {
				fmt.Fprintf(&sb, "%02X ", p[i])
			}
			df.logger.Debug("Write", "bytes", len(p), "n", n, "hex", sb.String())
		} else {
			df.logger.Debug("Write", "bytes", len(p), "n", n)
		}
	}
	return n, err
}

func (df *DebugFile) Read(p []byte) (n int, err error) {
	n, err = df.f.Read(p)
	if err != nil {
		df.logger.Debug("Read failed", "buflen", len(p), "n", n, "error", err)
	} else {
		df.logger.Debug("Read", "buflen", len(p), "n", n)
	}
	return n, err
}

func (df *DebugFile) Seek(offset int64, whence int) (int64, error) {
	res, err := df.f.Seek(offset, whence)
	if err != nil {
		df.logger.Debug("Seek failed", "offset", offset, "whence", whence, "result", res, "error", err)
	} else {
		df.logger.Debug("Seek", "offset", offset, "whence", whence, "result", res)
	}
	return res, err
}

func (df *DebugFile) Stat() (os.FileInfo, error) {
	fi, err := df.f.Stat()
	if err != nil {
		df.logger.Debug("Stat failed", "error", err)
	} else {
		df.logger.Debug("Stat", "name", df.f.Name(), "size", fi.Size())
	}
	return fi, err
}

func (df *DebugFile) Sync() error {
	err := df.f.Sync()
	if err != nil {
		df.logger.Debug("Sync failed", "error", err)
	} else {
		df.logger.Debug("Sync")
	}
	return err
}

func (df *DebugFile) Truncate(size int64) error {
	err := df.f.Truncate(size)
	if err != nil {
		df.logger.Debug("Truncate failed", "size", size, "error", err)
	} else {
		df.logger.Debug("Truncate", "size", size)
	}
	return err
}

func (df *DebugFile) Name() string {
	name := df.f.Name()
	df.logger.Debug("Name", "name", name)
	return name
}

func (df *DebugFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = df.f.WriteAt(p, off)
	if err != nil {
		name := df.f.Name()
		if strings.Contains(name, string(os.PathSeparator)+"wal"+string(os.PathSeparator)) || strings.HasSuffix(name, ".wal") {
			max := len(p)
			if max > 64 {
				max = 64
			}
			var sb strings.Builder
			for i := 0; i < max; i++ {
				fmt.Fprintf(&sb, "%02X ", p[i])
			}
			df.logger.Debug("WriteAt failed", "bytes", len(p), "n", n, "off", off, "error", err, "hex", sb.String())
		} else {
			df.logger.Debug("WriteAt failed", "bytes", len(p), "n", n, "off", off, "error", err)
		}
	} else {
		name := df.f.Name()
		if strings.Contains(name, string(os.PathSeparator)+"wal"+string(os.PathSeparator)) || strings.HasSuffix(name, ".wal") {
			max := len(p)
			if max > 64 {
				max = 64
			}
			var sb strings.Builder
			for i := 0; i < max; i++ {
				fmt.Fprintf(&sb, "%02X ", p[i])
			}
			df.logger.Debug("WriteAt", "bytes", len(p), "n", n, "off", off, "hex", sb.String())
		} else {
			df.logger.Debug("WriteAt", "bytes", len(p), "n", n, "off", off)
		}
	}
	return n, err
}

func (df *DebugFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = df.f.ReadAt(p, off)
	if err != nil {
		df.logger.Debug("ReadAt failed", "buflen", len(p), "n", n, "off", off, "error", err)
	} else {
		df.logger.Debug("ReadAt", "buflen", len(p), "n", n, "off", off)
	}
	return n, err
}

func (df *DebugFile) WriteString(s string) (n int, err error) {
	n, err = df.f.WriteString(s)
	if err != nil {
		df.logger.Debug("WriteString failed", "len", len(s), "n", n, "error", err)
	} else {
		df.logger.Debug("WriteString", "len", len(s), "n", n)
	}
	return n, err
}

func (df *DebugFile) WriteTo(w io.Writer) (n int64, err error) {
	n, err = df.f.WriteTo(w)
	if err != nil {
		df.logger.Debug("WriteTo failed", "n", n, "error", err)
	} else {
		df.logger.Debug("WriteTo", "n", n)
	}
	return n, err
}

func (df *DebugFile) ReadFrom(r io.Reader) (n int64, err error) {
	n, err = df.f.ReadFrom(r)
	if err != nil {
		df.logger.Debug("ReadFrom failed", "n", n, "error", err)
	} else {
		df.logger.Debug("ReadFrom", "n", n)
	}
	return n, err
}

func (df *DebugFile) Close() error {
	df.logger.Debug("Closing file")
	listFD.Delete(df.id)
	// df.logger.Debug(string(debug.Stack()))
	return df.f.Close()
}

func PrintMapFiles() {
	fmt.Println("List Files Opening")
	listFD.Range(func(key, value any) bool {
		fmt.Printf("Key: %v, Value: %v\n", key, value)
		return true
	})
}
