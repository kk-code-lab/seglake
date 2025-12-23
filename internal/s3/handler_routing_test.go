package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBucketListKindForRequest(t *testing.T) {
	cases := []struct {
		name          string
		method        string
		target        string
		hostBucket    string
		hasBucketOnly bool
		want          bucketListKind
	}{
		{
			name:          "list-buckets",
			method:        http.MethodGet,
			target:        "/",
			hostBucket:    "",
			hasBucketOnly: false,
			want:          bucketListBuckets,
		},
		{
			name:          "list-buckets-hosted-style",
			method:        http.MethodGet,
			target:        "/",
			hostBucket:    "demo",
			hasBucketOnly: false,
			want:          bucketListNone,
		},
		{
			name:          "list-v2",
			method:        http.MethodGet,
			target:        "/demo?list-type=2",
			hostBucket:    "",
			hasBucketOnly: true,
			want:          bucketListV2,
		},
		{
			name:          "list-location",
			method:        http.MethodGet,
			target:        "/demo?location",
			hostBucket:    "",
			hasBucketOnly: true,
			want:          bucketListLocation,
		},
		{
			name:          "list-uploads",
			method:        http.MethodGet,
			target:        "/demo?uploads",
			hostBucket:    "",
			hasBucketOnly: true,
			want:          bucketListUploads,
		},
		{
			name:          "list-v2-missing-bucket",
			method:        http.MethodGet,
			target:        "/?list-type=2",
			hostBucket:    "",
			hasBucketOnly: false,
			want:          bucketListNone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			got := bucketListKindForRequest(req, tc.hostBucket, tc.hasBucketOnly)
			if got != tc.want {
				t.Fatalf("expected %v got %v", tc.want, got)
			}
		})
	}
}

func TestIsListV1Request(t *testing.T) {
	cases := []struct {
		name          string
		method        string
		target        string
		hasBucketOnly bool
		want          bool
	}{
		{
			name:          "list-v1",
			method:        http.MethodGet,
			target:        "/demo",
			hasBucketOnly: true,
			want:          true,
		},
		{
			name:          "list-v1-ignored-on-list-v2",
			method:        http.MethodGet,
			target:        "/demo?list-type=2",
			hasBucketOnly: true,
			want:          false,
		},
		{
			name:          "list-v1-ignored-on-location",
			method:        http.MethodGet,
			target:        "/demo?location",
			hasBucketOnly: true,
			want:          false,
		},
		{
			name:          "list-v1-ignored-on-uploads",
			method:        http.MethodGet,
			target:        "/demo?uploads",
			hasBucketOnly: true,
			want:          false,
		},
		{
			name:          "list-v1-missing-bucket",
			method:        http.MethodGet,
			target:        "/",
			hasBucketOnly: false,
			want:          false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			got := isListV1Request(req, tc.hasBucketOnly)
			if got != tc.want {
				t.Fatalf("expected %v got %v", tc.want, got)
			}
		})
	}
}
