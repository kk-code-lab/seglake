package s3

import (
	"net/http"
	"testing"
	"time"
)

func TestAuthLimiterBlocksAfterBurst(t *testing.T) {
	limiter := &AuthLimiter{
		perIP:   newTokenBucket(1, 1, time.Second),
		perKey:  newTokenBucket(1, 1, time.Second),
		cleanup: time.Minute,
	}
	ip := "127.0.0.1"
	key := "test"
	if !limiter.Allow(ip, key) {
		t.Fatalf("expected initial allow")
	}
	limiter.ObserveFailure(ip, key)
	if limiter.Allow(ip, key) {
		t.Fatalf("expected blocked after consume")
	}
	time.Sleep(1100 * time.Millisecond)
	if !limiter.Allow(ip, key) {
		t.Fatalf("expected allow after refill")
	}
}

func TestExtractAccessKey(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "test", "testsecret", "us-east-1")
	if got := extractAccessKey(req); got != "test" {
		t.Fatalf("expected access key, got %q", got)
	}
}
