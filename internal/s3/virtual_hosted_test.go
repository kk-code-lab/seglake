package s3

import (
	"net/http/httptest"
	"testing"
)

func TestHostBucketIgnoresIP(t *testing.T) {
	h := &Handler{VirtualHosted: true}
	req := httptest.NewRequest("GET", "http://127.0.0.1:9000/bucket/key", nil)
	req.Host = "127.0.0.1:9000"
	if got := h.hostBucket(req); got != "" {
		t.Fatalf("expected no host bucket, got %q", got)
	}
}

func TestHostBucketParsesSubdomain(t *testing.T) {
	h := &Handler{VirtualHosted: true}
	req := httptest.NewRequest("GET", "http://bucket.localhost/key", nil)
	req.Host = "bucket.localhost"
	if got := h.hostBucket(req); got != "bucket" {
		t.Fatalf("expected bucket, got %q", got)
	}
}
