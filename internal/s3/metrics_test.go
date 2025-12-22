package s3

import (
	"testing"
	"time"
)

func TestMetricsBucketKeyStats(t *testing.T) {
	m := NewMetrics()
	m.Record("get", 200, 10*time.Millisecond, "bucket", "key")
	m.Record("get", 404, 5*time.Millisecond, "bucket", "key")

	reqs, _, _, _, latency, bucketReqs, bucketLatency, keyReqs, keyLatency := m.Snapshot()
	if reqs["get"]["2xx"] != 1 || reqs["get"]["4xx"] != 1 {
		t.Fatalf("unexpected op class counts: %#v", reqs["get"])
	}
	if bucketReqs["bucket"]["2xx"] != 1 || bucketReqs["bucket"]["4xx"] != 1 {
		t.Fatalf("unexpected bucket class counts: %#v", bucketReqs["bucket"])
	}
	if keyReqs["bucket/key"]["2xx"] != 1 || keyReqs["bucket/key"]["4xx"] != 1 {
		t.Fatalf("unexpected key class counts: %#v", keyReqs["bucket/key"])
	}
	if latency["get"].N != 2 {
		t.Fatalf("expected op latency samples, got %d", latency["get"].N)
	}
	if bucketLatency["bucket"].N != 2 {
		t.Fatalf("expected bucket latency samples, got %d", bucketLatency["bucket"].N)
	}
	if keyLatency["bucket/key"].N != 2 {
		t.Fatalf("expected key latency samples, got %d", keyLatency["bucket/key"].N)
	}
}
