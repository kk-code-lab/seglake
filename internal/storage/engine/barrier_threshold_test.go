package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestBarrierFlushByBytes(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout:          fs.NewLayout(filepath.Join(dir, "data")),
		BarrierInterval: 1 * time.Second,
		BarrierMaxBytes: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	start := time.Now()
	if _, _, err := engine.Put(context.Background(), bytes.NewReader([]byte("abcd"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected flush by bytes before interval, took %v", elapsed)
	}
}
