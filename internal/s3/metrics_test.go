package s3

import (
	"strconv"
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

func TestMetricsBucketKeyLimits(t *testing.T) {
	m := NewMetrics()
	for i := 0; i < 101; i++ {
		m.Record("get", 200, time.Millisecond, "bucket-"+strconv.Itoa(i), "key")
	}
	for i := 0; i < 1001; i++ {
		m.Record("get", 200, time.Millisecond, "bucket", "key-"+strconv.Itoa(i))
	}
	_, _, _, _, _, bucketReqs, _, keyReqs, _ := m.Snapshot()
	if len(bucketReqs) != 100 {
		t.Fatalf("expected 100 buckets, got %d", len(bucketReqs))
	}
	if len(keyReqs) != 1000 {
		t.Fatalf("expected 1000 keys, got %d", len(keyReqs))
	}
	m.Record("get", 500, time.Millisecond, "bucket-0", "key")
	m.Record("get", 500, time.Millisecond, "bucket", "key-0")
	_, _, _, _, _, bucketReqs, _, keyReqs, _ = m.Snapshot()
	if bucketReqs["bucket-0"]["5xx"] != 1 {
		t.Fatalf("expected bucket-0 to be updated")
	}
	if keyReqs["bucket/key-0"]["5xx"] != 1 {
		t.Fatalf("expected key-0 to be updated")
	}
}
