package engine

import (
	"context"
	"sync"
	"time"
)

type writeBarrier struct {
	engine       *Engine
	interval     time.Duration
	maxBytes     int64
	mu           sync.Mutex
	pendingBytes int64
	waiters      []chan error
	timer        *time.Timer
	flushRunning bool
}

func newWriteBarrier(engine *Engine, interval time.Duration, maxBytes int64) *writeBarrier {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	if maxBytes <= 0 {
		maxBytes = 128 << 20
	}
	return &writeBarrier{
		engine:   engine,
		interval: interval,
		maxBytes: maxBytes,
	}
}

func (b *writeBarrier) wait(ctx context.Context) error {
	ch := make(chan error, 1)
	b.mu.Lock()
	b.waiters = append(b.waiters, ch)
	if b.timer == nil {
		b.timer = time.AfterFunc(b.interval, b.flush)
	}
	b.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *writeBarrier) addBytes(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pendingBytes += n
	if b.pendingBytes >= b.maxBytes && !b.flushRunning {
		go b.flush()
	}
}

func (b *writeBarrier) flush() {
	b.mu.Lock()
	if b.flushRunning {
		b.mu.Unlock()
		return
	}
	b.flushRunning = true
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	waiters := b.waiters
	b.waiters = nil
	b.pendingBytes = 0
	b.mu.Unlock()

	err := b.engine.flushMeta()

	b.mu.Lock()
	b.flushRunning = false
	b.mu.Unlock()

	for _, ch := range waiters {
		ch <- err
		close(ch)
	}
}
