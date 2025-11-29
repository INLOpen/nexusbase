package sys

import (
	"io/fs"
	"path/filepath"
	"time"
)

// CleanupOldTmpFiles scans the `sstables` directory under `dataDir` and
// removes files with the `.tmp` suffix older than `olderThan`. It returns
// the number of files removed and any error encountered while walking the
// tree (removal errors are logged via the returned error aggregation).
func CleanupOldTmpFiles(dataDir string, olderThan time.Duration) (int, error) {
	if dataDir == "" {
		return 0, nil
	}
	sstDir := filepath.Join(dataDir, "sstables")
	var removed int
	now := time.Now()
	var walkErr error
	_ = filepath.WalkDir(sstDir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			// capture and continue walking
			walkErr = err
			return nil
		}
		if de.IsDir() {
			return nil
		}
		if filepath.Ext(de.Name()) != ".tmp" {
			return nil
		}
		// Check mod time
		fi, err := de.Info()
		if err != nil {
			walkErr = err
			return nil
		}
		if now.Sub(fi.ModTime()) > olderThan {
			if err := Remove(path); err == nil {
				removed++
			} else {
				// record the error but continue cleaning other files
				walkErr = err
			}
		}
		return nil
	})
	return removed, walkErr
}
