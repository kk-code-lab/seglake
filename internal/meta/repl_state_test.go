package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReplWatermark(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	hlc, err := store.GetReplPullWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplWatermark: %v", err)
	}
	if hlc != "" {
		t.Fatalf("expected empty watermark, got %q", hlc)
	}
	if err := store.SetReplPullWatermark(context.Background(), "0000000000000000001-0000000001"); err != nil {
		t.Fatalf("SetReplWatermark: %v", err)
	}
	hlc, err = store.GetReplPullWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplWatermark: %v", err)
	}
	if hlc != "0000000000000000001-0000000001" {
		t.Fatalf("unexpected watermark %q", hlc)
	}
}

func TestReplPushWatermark(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	hlc, err := store.GetReplPushWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplPushWatermark: %v", err)
	}
	if hlc != "" {
		t.Fatalf("expected empty watermark, got %q", hlc)
	}
	if err := store.SetReplPushWatermark(context.Background(), "0000000000000000002-0000000001"); err != nil {
		t.Fatalf("SetReplPushWatermark: %v", err)
	}
	hlc, err = store.GetReplPushWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetReplPushWatermark: %v", err)
	}
	if hlc != "0000000000000000002-0000000001" {
		t.Fatalf("unexpected watermark %q", hlc)
	}
}

func TestReplRemoteWatermark(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	if err := store.SetReplRemotePullWatermark(context.Background(), "http://peer-a:9000", "0000000000000000010-0000000001"); err != nil {
		t.Fatalf("SetReplRemotePullWatermark: %v", err)
	}
	if err := store.SetReplRemotePushWatermark(context.Background(), "http://peer-a:9000", "0000000000000000011-0000000001"); err != nil {
		t.Fatalf("SetReplRemotePushWatermark: %v", err)
	}
	pull, err := store.GetReplRemotePullWatermark(context.Background(), "http://peer-a:9000")
	if err != nil {
		t.Fatalf("GetReplRemotePullWatermark: %v", err)
	}
	push, err := store.GetReplRemotePushWatermark(context.Background(), "http://peer-a:9000")
	if err != nil {
		t.Fatalf("GetReplRemotePushWatermark: %v", err)
	}
	if pull == "" || push == "" {
		t.Fatalf("expected pull/push watermarks, got pull=%q push=%q", pull, push)
	}
	state, err := store.GetReplRemoteState(context.Background(), "http://peer-a:9000")
	if err != nil {
		t.Fatalf("GetReplRemoteState: %v", err)
	}
	if state.LastPullHLC == "" || state.LastPushHLC == "" {
		t.Fatalf("expected pull/push HLC, got %+v", state)
	}
	list, err := store.ListReplRemoteStates(context.Background())
	if err != nil {
		t.Fatalf("ListReplRemoteStates: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 remote state, got %d", len(list))
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
