//go:build e2e

package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestS3E2EDeleteBucket(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/obj", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(putReq, "test", "testsecret", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	// Bucket not empty -> 409
	delBucketReq, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delBucketReq, "test", "testsecret", "us-east-1")
	delBucketResp, err := http.DefaultClient.Do(delBucketReq)
	if err != nil {
		t.Fatalf("DELETE bucket error: %v", err)
	}
	delBucketResp.Body.Close()
	if delBucketResp.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE bucket status: %d", delBucketResp.StatusCode)
	}

	// Delete object then bucket
	delObjReq, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket/obj", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delObjReq, "test", "testsecret", "us-east-1")
	delObjResp, err := http.DefaultClient.Do(delObjReq)
	if err != nil {
		t.Fatalf("DELETE object error: %v", err)
	}
	delObjResp.Body.Close()
	if delObjResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE object status: %d", delObjResp.StatusCode)
	}

	delBucketReq, err = http.NewRequest(http.MethodDelete, server.URL+"/bucket", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delBucketReq, "test", "testsecret", "us-east-1")
	delBucketResp, err = http.DefaultClient.Do(delBucketReq)
	if err != nil {
		t.Fatalf("DELETE bucket error: %v", err)
	}
	delBucketResp.Body.Close()
	if delBucketResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE bucket status: %d", delBucketResp.StatusCode)
	}
}

func TestS3E2EVirtualHosted(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	handler := &Handler{
		Engine:        eng,
		Meta:          store,
		VirtualHosted: true,
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/vhost-key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	putReq.Host = "bucket.localhost"
	signRequest(putReq, "test", "testsecret", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/vhost-key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	getReq.Host = "bucket.localhost"
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "data" {
		t.Fatalf("GET body mismatch")
	}
}
