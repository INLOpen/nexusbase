package wal

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/require"
)

// TestSegmentWriteReadRoundtrip verifies that a record written with
// SegmentWriter can be read back intact using SegmentReader. This catches
// regressions where written checksum bytes or record layout are incorrect.
func TestSegmentWriteReadRoundtrip(t *testing.T) {
	dir := t.TempDir()

	// create a new segment
	sw, err := CreateSegment(dir, 1, 4096, 0)
	require.NoError(t, err)

	payload := []byte("hello-roundtrip")

	// write a single record
	require.NoError(t, sw.WriteRecord(payload))
	require.NoError(t, sw.Sync())
	require.NoError(t, sw.Close())

	// open for read and verify
	path := filepath.Join(dir, core.FormatSegmentFileName(1))
	sr, err := OpenSegmentForRead(path)
	require.NoError(t, err)
	defer sr.Close()

	got, err := sr.ReadRecord()
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// subsequent ReadRecord should return io.EOF (clean end)
	_, err = sr.ReadRecord()
	require.Equal(t, io.EOF, err)
}
