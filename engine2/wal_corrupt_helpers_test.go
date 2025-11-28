package engine2

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

// corruptSegmentMagic overwrites the 4-byte magic at the start of a WAL
// segment file. Use this to simulate an invalid segment header.
func corruptSegmentMagic(path string, newMagic uint32) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}
	if len(data) < 4 {
		return fmt.Errorf("segment too small to contain magic")
	}
	binary.LittleEndian.PutUint32(data[0:4], newMagic)
	if err := os.WriteFile(path, data, fs.FileMode(0o644)); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	return nil
}

// setRecordLength sets the on-disk length field for the given record index
// (0-based). This can be used to set an absurdly large length to trigger
// sanity-limit checks.
func setRecordLength(path string, recordIndex int, newLen uint32) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}
	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	for i := 0; i < recordIndex; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("file too small while seeking record %d", recordIndex)
		}
		recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		total := 4 + recLen + 4
		offset += total
	}
	if offset+4 > len(data) {
		return fmt.Errorf("segment too small to set record length")
	}
	binary.LittleEndian.PutUint32(data[offset:offset+4], newLen)
	if err := os.WriteFile(path, data, fs.FileMode(0o644)); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	return nil
}

// truncateInsideRecord truncates the segment file at an offset inside the
// specified record (0-based). The truncateOffset is relative into the
// record body (0 == start of body). This simulates an incomplete write.
func truncateInsideRecord(path string, recordIndex int, truncateOffsetIntoBody int) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}
	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	for i := 0; i < recordIndex; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("file too small while seeking record %d", recordIndex)
		}
		recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		total := 4 + recLen + 4
		offset += total
	}
	if offset+4 > len(data) {
		return fmt.Errorf("segment too small to truncate record")
	}
	recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
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
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}
	fileHeaderSize := binary.Size(core.FileHeader{})
	offset := fileHeaderSize
	for i := 0; i < recordIndex; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("file too small while seeking record %d", recordIndex)
		}
		recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		total := 4 + recLen + 4
		offset += total
	}
	if offset+4 > len(data) {
		return fmt.Errorf("segment too small to flip byte in record")
	}
	recLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	bodyStart := offset + 4
	if offsetIntoBody < 0 || offsetIntoBody >= recLen {
		return fmt.Errorf("offsetIntoBody out of range")
	}
	pos := bodyStart + offsetIntoBody
	if pos >= len(data) {
		return fmt.Errorf("calculated position beyond file bounds")
	}
	data[pos] = data[pos] ^ mask
	if err := os.WriteFile(path, data, fs.FileMode(0o644)); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	return nil
}
