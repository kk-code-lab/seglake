package segment

import (
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
	footer.BloomOffset = 123
	footer.IndexOffset = 456
	footer = FinalizeFooter(footer)

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

	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if _, err := reader.ReadFooter(); err == nil {
		t.Fatalf("expected error for invalid footer")
	}
}
