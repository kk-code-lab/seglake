package s3

import (
	"testing"
	"time"
)

func TestReplayCacheAllowsOnce(t *testing.T) {
	cache := newReplayCache(2*time.Second, 10)
	now := time.Now().UTC()
	key := "sig:test"
	if !cache.allow(key, now) {
		t.Fatalf("expected first allow to pass")
	}
	if cache.allow(key, now.Add(500*time.Millisecond)) {
		t.Fatalf("expected replay to be blocked within ttl")
	}
	if !cache.allow(key, now.Add(3*time.Second)) {
		t.Fatalf("expected allow after ttl")
	}
}

func TestReplayCacheEvictsByMaxEntries(t *testing.T) {
	cache := newReplayCache(10*time.Second, 2)
	now := time.Now().UTC()
	if !cache.allow("sig:a", now) {
		t.Fatalf("expected first allow to pass")
	}
	if !cache.allow("sig:b", now.Add(10*time.Millisecond)) {
		t.Fatalf("expected second allow to pass")
	}
	if !cache.allow("sig:c", now.Add(20*time.Millisecond)) {
		t.Fatalf("expected third allow to pass")
	}
	if !cache.allow("sig:a", now.Add(30*time.Millisecond)) {
		t.Fatalf("expected evicted key to be allowed again")
	}
}
