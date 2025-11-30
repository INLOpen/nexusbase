package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
)

// BenchmarkWALAppend_SyncMode exercises WAL append performance under different
// SyncMode settings so we can compare latency and allocations for
// `WALSyncDisabled`, `WALSyncBatch`, and `WALSyncAlways`.
func BenchmarkWALAppend_SyncMode(b *testing.B) {
	cases := []struct {
		name string
		mode core.WALSyncMode
	}{
		{name: "SyncDisabled", mode: core.WALSyncDisabled},
		{name: "SyncBatch", mode: core.WALSyncBatch},
		{name: "SyncAlways", mode: core.WALSyncAlways},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			dir := filepath.Join(os.TempDir(), "nexusbase_wal_bench")
			_ = os.RemoveAll(dir)
			defer os.RemoveAll(dir)
			if err := os.MkdirAll(dir, 0755); err != nil {
				b.Fatalf("failed to create temp dir: %v", err)
			}

			opts := Options{
				Dir:              dir,
				MaxSegmentSize:   16 * 1024 * 1024,
				WriterBufferSize: 64 * 1024,
				SyncMode:         c.mode,
			}

			w, _, err := Open(opts)
			if err != nil {
				b.Fatalf("failed to open wal: %v", err)
			}
			defer w.Close()

			entry := core.WALEntry{
				EntryType: core.EntryTypePutEvent,
				SeqNum:    1,
				Key:       []byte("bench_key"),
				Value:     make([]byte, 128),
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry.SeqNum = uint64(i + 1)
				if err := w.Append(entry); err != nil {
					b.Fatalf("append failed: %v", err)
				}
			}
		})
	}
}
