package engine2

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

// corruptSegmentMagic overwrites the 4-byte magic at the start of a WAL
// segment file. Use this to simulate an invalid segment header.
func corruptSegmentMagic(path string, newMagic uint32) error {
	// Open file for read-write and modify the first 4 bytes in-place.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open segment for modify: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, newMagic)
	if _, err := f.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("write magic at offset 0: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync after corrupting magic: %w", err)
	}
	return nil
}

// setRecordLength sets the on-disk length field for the given record index
// (0-based). This can be used to set an absurdly large length to trigger
// sanity-limit checks.
func setRecordLength(path string, recordIndex int, newLen uint32) error {
	// Modify only the 4-byte length field in-place using WriteAt.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open segment for modify: %w", err)
	}
	defer f.Close()

	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	// Walk to the target record's length field by reading lengths one-by-one.
	for i := 0; i < recordIndex; i++ {
		lenBuf := make([]byte, 4)
		if _, err := f.ReadAt(lenBuf, int64(offset)); err != nil {
			return fmt.Errorf("read length while seeking record %d: %w", recordIndex, err)
		}
		recLen := int(binary.LittleEndian.Uint32(lenBuf))
		total := 4 + recLen + 4
		offset += total
	}
	// Write the new length at the computed offset
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, newLen)
	if _, err := f.WriteAt(lenBuf, int64(offset)); err != nil {
		return fmt.Errorf("write new length at offset %d: %w", offset, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync after writing new length: %w", err)
	}
	return nil
}

// truncateInsideRecord truncates the segment file at an offset inside the
// specified record (0-based). The truncateOffset is relative into the
// record body (0 == start of body). This simulates an incomplete write.
func truncateInsideRecord(path string, recordIndex int, truncateOffsetIntoBody int) error {
	// Read only the length headers needed to compute truncate point, then truncate.
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment for read: %w", err)
	}
	_ = f.Close()

	// We still compute offsets by reading the length fields from disk.
	// Open for read-only to walk lengths.
	f2, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open segment for seek: %w", err)
	}
	defer f2.Close()

	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	for i := 0; i < recordIndex; i++ {
		lenBuf := make([]byte, 4)
		if _, err := f2.ReadAt(lenBuf, int64(offset)); err != nil {
			return fmt.Errorf("read length while seeking record %d: %w", recordIndex, err)
		}
		recLen := int(binary.LittleEndian.Uint32(lenBuf))
		total := 4 + recLen + 4
		offset += total
	}
	lenBuf := make([]byte, 4)
	if _, err := f2.ReadAt(lenBuf, int64(offset)); err != nil {
		return fmt.Errorf("read target record length: %w", err)
	}
	recLen := int(binary.LittleEndian.Uint32(lenBuf))
	bodyStart := offset + 4
	if truncateOffsetIntoBody < 0 || truncateOffsetIntoBody >= recLen {
		return fmt.Errorf("truncate offset out of bounds")
	}
	truncateAt := int64(bodyStart + truncateOffsetIntoBody)
	if err := os.Truncate(path, truncateAt); err != nil {
		return fmt.Errorf("truncate segment: %w", err)
	}
	return nil
}

// flipByteInRecordBody flips a single byte (xor with mask) at the given
// position inside the specified record body to cause a CRC mismatch.
func flipByteInRecordBody(path string, recordIndex int, offsetIntoBody int, mask byte) error {
	// Open file for read-write and modify a single byte in-place.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open segment for modify: %w", err)
	}
	defer f.Close()

	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	for i := 0; i < recordIndex; i++ {
		lenBuf := make([]byte, 4)
		if _, err := f.ReadAt(lenBuf, int64(offset)); err != nil {
			return fmt.Errorf("read length while seeking record %d: %w", recordIndex, err)
		}
		recLen := int(binary.LittleEndian.Uint32(lenBuf))
		total := 4 + recLen + 4
		offset += total
	}
	lenBuf := make([]byte, 4)
	if _, err := f.ReadAt(lenBuf, int64(offset)); err != nil {
		return fmt.Errorf("read record length at target: %w", err)
	}
	recLen := int(binary.LittleEndian.Uint32(lenBuf))
	bodyStart := offset + 4
	if offsetIntoBody < 0 || offsetIntoBody >= recLen {
		return fmt.Errorf("offsetIntoBody out of range")
	}
	pos := int64(bodyStart + offsetIntoBody)
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, pos); err != nil {
		return fmt.Errorf("read target byte at %d: %w", pos, err)
	}
	b[0] = b[0] ^ mask
	if _, err := f.WriteAt(b, pos); err != nil {
		return fmt.Errorf("write flipped byte at %d: %w", pos, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync after flipping byte: %w", err)
	}
	return nil
}
