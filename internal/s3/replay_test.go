package s3

import (
	"testing"
	"time"
)

func TestReplayCacheAllowsOnce(t *testing.T) {
	cache := newReplayCache(2 * time.Second)
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
