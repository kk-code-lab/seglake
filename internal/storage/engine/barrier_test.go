package engine

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestBarrierConcurrentPuts(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(dir + "/meta.db")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	engine, err := New(Options{
		Layout:          fs.NewLayout(dir + "/data"),
		MetaStore:       store,
		SegmentMaxAge:   1 * time.Second,
		SegmentMaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const puts = 10
	var wg sync.WaitGroup
	wg.Add(puts)
	for i := 0; i < puts; i++ {
		go func(i int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('a' + i)}, 32)
			if _, _, err := engine.PutObject(context.Background(), "b", "k"+string(rune('a'+i)), bytes.NewReader(payload)); err != nil {
				t.Errorf("PutObject: %v", err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < puts; i++ {
		key := "k" + string(rune('a'+i))
		if _, err := store.CurrentVersion(context.Background(), "b", key); err != nil {
			t.Fatalf("CurrentVersion for %s: %v", key, err)
		}
	}
}

func TestBarrierWaitFlushesOnInterval(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout:          fs.NewLayout(dir + "/data"),
		BarrierInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start := time.Now()
	if err := engine.barrier.wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("wait took too long: %v", elapsed)
	}
}

func TestBarrierWaitContextCanceled(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout:          fs.NewLayout(dir + "/data"),
		BarrierInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.barrier.wait(ctx); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
