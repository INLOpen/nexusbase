package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

func main() {
	path := flag.String("file", "", "path to WAL segment file")
	flag.Parse()
	if *path == "" {
		log.Fatalf("missing -file argument")
	}

	f, err := os.Open(*path)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer f.Close()

	var header core.FileHeader
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		if err == io.EOF {
			log.Fatalf("file too small to contain header: %v", err)
		}
		log.Fatalf("read header: %v", err)
	}
	headerSize := int64(header.Size())
	fmt.Printf("HeaderSize: %d bytes\n", headerSize)
	fmt.Printf("Magic: 0x%08x\n", header.Magic)
	fmt.Printf("Version: %d\n", header.Version)
	fmt.Printf("CreatedAt: %d\n", header.CreatedAt)
	fmt.Printf("CompressorType: %d (%s)\n", header.CompressorType, header.CompressorType.String())

	var recIdx int
	offset := headerSize
	for {
		// Read length
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				fmt.Printf("Reached EOF after %d records\n", recIdx)
				return
			}
			log.Fatalf("read length at offset %d: %v", offset, err)
		}
		offset += 4

		if length > uint32(core.WALMaxSegmentSize) {
			log.Fatalf("record length %d exceeds sanity limit %d", length, core.WALMaxSegmentSize)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			log.Fatalf("read record body at idx %d (offset %d) expected %d bytes: %v", recIdx, offset, length, err)
		}
		offset += int64(length)

		var stored uint32
		if err := binary.Read(f, binary.LittleEndian, &stored); err != nil {
			log.Fatalf("read stored checksum at idx %d (offset %d): %v", recIdx, offset, err)
		}
		offset += 4

		computed := crc32.ChecksumIEEE(data)
		fmt.Printf("Record %d: length=%d body_offset=%d..%d checksum_offset=%d stored=0x%08x computed=0x%08x\n",
			recIdx, length, offset-int64(length)-4, offset-5, offset-4, stored, computed)
		if stored != computed {
			fmt.Printf("--> MISMATCH at record %d: stored 0x%08x != computed 0x%08x\n", recIdx, stored, computed)
			// Dump a short hexdiff around the checksum and last bytes of body
			printHexContext(data, stored, computed)
			os.Exit(2)
		}

		recIdx++
	}
}

func printHexContext(data []byte, stored, computed uint32) {
	// Show last 32 bytes of body and the stored checksum bytes
	start := 0
	if len(data) > 32 {
		start = len(data) - 32
	}
	fmt.Printf("Body tail (hex):\n")
	for i := start; i < len(data); i++ {
		fmt.Printf("%02X ", data[i])
	}
	fmt.Printf("\n")
	// print stored and computed as bytes little-endian
	bStored := make([]byte, 4)
	bComputed := make([]byte, 4)
	binary.LittleEndian.PutUint32(bStored, stored)
	binary.LittleEndian.PutUint32(bComputed, computed)
	fmt.Printf("Stored checksum bytes:   %02X %02X %02X %02X\n", bStored[0], bStored[1], bStored[2], bStored[3])
	fmt.Printf("Computed checksum bytes: %02X %02X %02X %02X\n", bComputed[0], bComputed[1], bComputed[2], bComputed[3])
}
