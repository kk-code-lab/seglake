package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReplWatermark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hlc, err := store.GetReplWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplWatermark: %v", err)
	}
	if hlc != "" {
		t.Fatalf("expected empty watermark, got %q", hlc)
	}
	if err := store.SetReplWatermark(context.Background(), "0000000000000000001-0000000001"); err != nil {
		t.Fatalf("SetReplWatermark: %v", err)
	}
	hlc, err = store.GetReplWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplWatermark: %v", err)
	}
	if hlc != "0000000000000000001-0000000001" {
		t.Fatalf("unexpected watermark %q", hlc)
	}
}
