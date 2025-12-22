package segment

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg-0001")

	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	header := ChunkRecordHeader{
		Hash: [32]byte{1, 2, 3},
		Len:  4,
	}
	if _, err := writer.AppendRecord(header, []byte{10, 11, 12, 13}); err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}

	footer := NewFooter(1)
	if err := writer.Seal(footer); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if got := reader.Header(); got.Magic != segmentMagic || got.Version != 1 {
		t.Fatalf("Header: unexpected header: %+v", got)
	}

	gotHeader, gotData, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if gotHeader.Len != 4 || gotData[0] != 10 {
		t.Fatalf("ReadRecord: unexpected data")
	}

	gotFooter, err := reader.ReadFooter()
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if gotFooter.Magic != footerMagic || gotFooter.Version != 1 {
		t.Fatalf("ReadFooter: unexpected footer: %+v", gotFooter)
	}

	if _, _, err := reader.ReadRecord(); err != io.EOF {
		t.Fatalf("expected EOF after last record, got %v", err)
	}
}

func TestNewReaderWithMissingHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad")

	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := NewReader(path); err == nil {
		t.Fatalf("expected error for missing header")
	}
}

func TestReadFooterWithMissingFooter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg-0002")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := EncodeSegmentHeader(file, NewSegmentHeader(1)); err != nil {
		t.Fatalf("EncodeSegmentHeader: %v", err)
	}
	if _, err := file.Write(make([]byte, footerLen)); err != nil {
		t.Fatalf("Write footer padding: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := NewReader(path); err == nil {
		t.Fatalf("expected error for invalid footer")
	}
}

func TestFooterRoundTripFields(t *testing.T) {
	footer := NewFooter(2)
	footer.BloomOffset = 1234
	footer.IndexOffset = 5678
	footer.BloomBytes = 64
	footer.IndexBytes = 128
	footer = FinalizeFooter(footer)

	var buf bytes.Buffer
	if err := EncodeFooter(&buf, footer); err != nil {
		t.Fatalf("EncodeFooter: %v", err)
	}
	got, err := DecodeFooter(&buf)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	if err := ValidateFooter(got); err != nil {
		t.Fatalf("ValidateFooter: %v", err)
	}
	if got.BloomOffset != footer.BloomOffset || got.IndexOffset != footer.IndexOffset {
		t.Fatalf("offsets mismatch: %+v", got)
	}
	if got.BloomBytes != footer.BloomBytes || got.IndexBytes != footer.IndexBytes {
		t.Fatalf("bytes mismatch: %+v", got)
	}
	if got.ChecksumHash != footer.ChecksumHash {
		t.Fatalf("checksum mismatch")
	}
}

func TestSealWithIndexAndReaderBounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg-idx")

	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	data := []byte("abcdef")
	hash := HashChunk(data)
	offset, err := writer.AppendRecord(ChunkRecordHeader{Hash: hash, Len: uint32(len(data))}, data)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}

	entries := []IndexEntry{{Offset: offset, Hash: hash}}
	bloom := BuildBloom(entries)
	index := EncodeIndex(entries)
	if _, err := writer.SealWithIndex(NewFooter(1), bloom, index); err != nil {
		t.Fatalf("SealWithIndex: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if _, _, err := reader.ReadRecord(); err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if _, _, err := reader.ReadRecord(); err != io.EOF {
		t.Fatalf("expected EOF after last record, got %v", err)
	}

	footer, err := reader.ReadFooter()
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if footer.BloomBytes == 0 || footer.IndexBytes == 0 {
		t.Fatalf("expected bloom/index bytes, got %+v", footer)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()
	bloomBuf := make([]byte, footer.BloomBytes)
	if _, err := file.ReadAt(bloomBuf, footer.BloomOffset); err != nil {
		t.Fatalf("ReadAt bloom: %v", err)
	}
	indexBuf := make([]byte, footer.IndexBytes)
	if _, err := file.ReadAt(indexBuf, footer.IndexOffset); err != nil {
		t.Fatalf("ReadAt index: %v", err)
	}
	decoded, err := DecodeIndex(indexBuf)
	if err != nil {
		t.Fatalf("DecodeIndex: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Offset != offset {
		t.Fatalf("decoded index mismatch: %+v", decoded)
	}
}
