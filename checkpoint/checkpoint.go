package checkpoint

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/sys"
)

const (
	// MagicNumber is the magic number for a CHECKPOINT file.
	// It corresponds to the ASCII representation of "CKPT".
	MagicNumber uint32 = 0x54504B43
	// FileName is the name of the checkpoint file.
	FileName = "CHECKPOINT"
	// TempFileName is the temporary file used for atomic writes.
	TempFileName = "CHECKPOINT.tmp"
)

// Checkpoint holds the data stored in the CHECKPOINT file.
type Checkpoint struct {
	// LastSafeSegmentIndex is the index of the last WAL segment
	// whose data has been fully persisted to SSTables.
	LastSafeSegmentIndex uint64
}

// Write atomically writes the checkpoint data to a file in the given directory.
// It implements the "write-and-rename" strategy described in checkpointing.md
// to ensure atomicity and prevent corruption.
func Write(dir string, cp Checkpoint) error {
	// 1. Create a temporary file.
	tempPath := filepath.Join(dir, TempFileName)
	file, err := sys.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp checkpoint file: %w", err)
	}

	// 2. Write Magic Number and data to the temporary file.
	if err := binary.Write(file, binary.LittleEndian, MagicNumber); err != nil {
		file.Close() // Ensure file is closed on error
		return fmt.Errorf("failed to write checkpoint magic number: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, cp.LastSafeSegmentIndex); err != nil {
		file.Close() // Ensure file is closed on error
		return fmt.Errorf("failed to write last safe segment index: %w", err)
	}

	// 3. Fsync the temporary file to ensure it's on disk.
	if err := file.Sync(); err != nil {
		file.Close() // Ensure file is closed on error
		return fmt.Errorf("failed to sync temp checkpoint file: %w", err)
	}

	// 4. Close the file BEFORE renaming. This is crucial for Windows compatibility.
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temp checkpoint file before rename: %w", err)
	}

	// 5. Atomically rename the temporary file to the final name.
	finalPath := filepath.Join(dir, FileName)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp checkpoint file to final name: %w", err)
	}

	// NOTE: Syncing the parent directory after a rename is a good practice for ensuring
	// the filesystem metadata change is persisted. However, it's omitted here because
	// os.File.Sync() on a directory handle is not reliably supported across all
	// platforms (e.g., it causes "Access is denied" on Windows). The atomicity of
	// os.Rename() provides the primary guarantee against corruption.

	return nil
}

// Read reads the checkpoint data from the file in the given directory.
// It returns the checkpoint data and a boolean indicating if the file existed.
// If the file does not exist, it returns a zero-value Checkpoint and no error.
func Read(dir string) (Checkpoint, bool, error) {
	path := filepath.Join(dir, FileName)
	file, err := sys.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// File not existing is not an error, it just means no checkpoint has been made.
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, fmt.Errorf("failed to open checkpoint file: %w", err)
	}
	defer file.Close()

	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return Checkpoint{}, true, fmt.Errorf("failed to read checkpoint magic number: %w", err)
	}

	if magic != MagicNumber {
		return Checkpoint{}, true, fmt.Errorf("invalid checkpoint magic number: got %x, want %x", magic, MagicNumber)
	}

	var cp Checkpoint
	if err := binary.Read(file, binary.LittleEndian, &cp.LastSafeSegmentIndex); err != nil {
		return Checkpoint{}, true, fmt.Errorf("failed to read last safe segment index: %w", err)
	}

	return cp, true, nil
}
