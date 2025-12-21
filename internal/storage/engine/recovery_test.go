package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func TestRecoverOpenSegmentSealsClean(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	segID := "seg-test-clean"
	segPath := layout.SegmentPath(segID)
	writer, err := segment.NewWriter(segPath, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := writer.AppendRecord(segment.ChunkRecordHeader{Len: 3}, []byte("abc")); err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	info, _ := os.Stat(segPath)
	if err := store.RecordSegment(context.Background(), segID, segPath, string(segment.StateOpen), info.Size(), nil); err != nil {
		t.Fatalf("RecordSegment: %v", err)
	}

	_, err = New(Options{Layout: layout, MetaStore: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	seg, err := store.GetSegment(context.Background(), segID)
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if seg.State != string(segment.StateSealed) {
		t.Fatalf("expected sealed, got %s", seg.State)
	}

	reader, err := segment.NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := reader.ReadFooter(); err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	_ = reader.Close()
}

func TestRecoverOpenSegmentSkipsPartial(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "objects"))
	if err := os.MkdirAll(layout.SegmentsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	segID := "seg-test-partial"
	segPath := layout.SegmentPath(segID)
	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := segment.EncodeSegmentHeader(file, segment.NewSegmentHeader(1)); err != nil {
		t.Fatalf("EncodeSegmentHeader: %v", err)
	}
	if err := segment.EncodeHeader(file, segment.ChunkRecordHeader{Len: 10}); err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}
	if _, err := file.Write([]byte("short")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = file.Close()

	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	info, _ := os.Stat(segPath)
	if err := store.RecordSegment(context.Background(), segID, segPath, string(segment.StateOpen), info.Size(), nil); err != nil {
		t.Fatalf("RecordSegment: %v", err)
	}

	_, err = New(Options{Layout: layout, MetaStore: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	seg, err := store.GetSegment(context.Background(), segID)
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if seg.State != string(segment.StateOpen) {
		t.Fatalf("expected open, got %s", seg.State)
	}
}
