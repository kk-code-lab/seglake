package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestSegmentRotationBySize(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout:          fs.NewLayout(filepath.Join(dir, "data")),
		SegmentMaxBytes: 128,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Each put ~ 64 bytes payload + headers, so two puts should rotate.
	payload := make([]byte, 64)
	if _, _, err := engine.Put(context.Background(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := engine.Put(context.Background(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	count, err := countFiles(engine.layout.SegmentsDir)
	if err != nil {
		t.Fatalf("countFiles: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected rotation, got %d segments", count)
	}
}

func TestSegmentRotationByAge(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout:        fs.NewLayout(filepath.Join(dir, "data")),
		SegmentMaxAge: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload := make([]byte, 8)
	if _, _, err := engine.Put(context.Background(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, _, err := engine.Put(context.Background(), bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	count, err := countFiles(engine.layout.SegmentsDir)
	if err != nil {
		t.Fatalf("countFiles: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected rotation, got %d segments", count)
	}
}

func countFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		count++
	}
	return count, nil
}
