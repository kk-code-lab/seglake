//go:build e2e

package s3

import (
	"bytes"
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

func TestS3E2ERangeGet(t *testing.T) {
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

	putURL := server.URL + "/bucket/range"
	data := []byte("0123456789")
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(data))
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

	getReq, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	getReq.Header.Set("Range", "bytes=3-6")
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "3456" {
		t.Fatalf("range body mismatch: %q", string(body))
	}
	if getResp.Header.Get("Content-Range") == "" {
		t.Fatalf("missing Content-Range")
	}
}

func TestS3E2ESignedHeadersRange(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/range-signed", strings.NewReader("0123456789"))
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

	rangeReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/range-signed", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	rangeReq.Header.Set("Range", "bytes=3-5")
	rangeReq.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	rangeReq.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	rangeReq.Header.Set("Host", rangeReq.URL.Host)
	rangeReq.Header.Set("Authorization", signWithCustomHeader(rangeReq, "test", "testsecret", "us-east-1", "range"))
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("range error: %v", err)
	}
	defer rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status: %d", rangeResp.StatusCode)
	}
	body, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "345" {
		t.Fatalf("range body mismatch")
	}
}

func TestS3E2ERangeGetNestedKey(t *testing.T) {
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

	putURL := server.URL + "/bucket/prefix/range"
	data := []byte("0123456789")
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(data))
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

	getReq, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
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
	if string(body) != string(data) {
		t.Fatalf("GET body mismatch")
	}

	rangeReq, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	rangeReq.Header.Set("Range", "bytes=3-6")
	signRequest(rangeReq, "test", "testsecret", "us-east-1")
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("range GET error: %v", err)
	}
	defer rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range GET status: %d", rangeResp.StatusCode)
	}
	rangeBody, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(rangeBody) != "3456" {
		t.Fatalf("range body mismatch: %q", string(rangeBody))
	}
	if rangeResp.Header.Get("Content-Range") == "" {
		t.Fatalf("missing Content-Range")
	}
}

func TestS3E2EMultiRangeGet(t *testing.T) {
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

	putURL := server.URL + "/bucket/range"
	data := []byte("0123456789")
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(data))
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

	getReq, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	getReq.Header.Set("Range", "bytes=0-1,3-4")
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	ct := getResp.Header.Get("Content-Type")
	if !strings.Contains(ct, "multipart/byteranges") {
		t.Fatalf("expected multipart content-type, got %q", ct)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("Content-Range: bytes 0-1/10")) {
		t.Fatalf("missing range 0-1")
	}
	if !bytes.Contains(body, []byte("Content-Range: bytes 3-4/10")) {
		t.Fatalf("missing range 3-4")
	}
}

func TestS3E2EConditionalGet(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/cond", strings.NewReader("data"))
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
	etag := putResp.Header.Get("ETag")

	matchReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/cond", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	matchReq.Header.Set("If-Match", etag)
	signRequest(matchReq, "test", "testsecret", "us-east-1")
	matchResp, err := http.DefaultClient.Do(matchReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	matchResp.Body.Close()
	if matchResp.StatusCode != http.StatusOK {
		t.Fatalf("If-Match status: %d", matchResp.StatusCode)
	}

	badMatchReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/cond", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	badMatchReq.Header.Set("If-Match", "\"deadbeef\"")
	signRequest(badMatchReq, "test", "testsecret", "us-east-1")
	badMatchResp, err := http.DefaultClient.Do(badMatchReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	badMatchResp.Body.Close()
	if badMatchResp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match status: %d", badMatchResp.StatusCode)
	}

	noneReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/cond", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	noneReq.Header.Set("If-None-Match", etag)
	signRequest(noneReq, "test", "testsecret", "us-east-1")
	noneResp, err := http.DefaultClient.Do(noneReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	noneResp.Body.Close()
	if noneResp.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match status: %d", noneResp.StatusCode)
	}
}

func TestS3E2EDeleteObject(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/delete", strings.NewReader("data"))
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

	delReq, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket/delete", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delReq, "test", "testsecret", "us-east-1")
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	deleteMarker := delResp.Header.Get("x-amz-delete-marker")
	deleteVersion := delResp.Header.Get("x-amz-version-id")
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", delResp.StatusCode)
	}
	if deleteMarker != "true" {
		t.Fatalf("expected delete marker header, got %q", deleteMarker)
	}
	if deleteVersion == "" {
		t.Fatalf("expected delete marker version id")
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/delete", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	if got := getResp.Header.Get("x-amz-delete-marker"); got != "true" {
		t.Fatalf("expected delete marker header on GET, got %q", got)
	}
	if got := getResp.Header.Get("x-amz-version-id"); got == "" {
		t.Fatalf("expected delete marker version id on GET")
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
}

func TestS3E2EUnversionedBucketDeleteNoMarker(t *testing.T) {
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

	createReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket-unversioned", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	createReq.Header.Set("x-seglake-versioning", "unversioned")
	signRequest(createReq, "test", "testsecret", "us-east-1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("PUT bucket error: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT bucket status: %d", createResp.StatusCode)
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket-unversioned/key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(putReq, "test", "testsecret", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	if got := putResp.Header.Get("x-amz-version-id"); got != "" {
		t.Fatalf("expected no version id, got %q", got)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	delReq, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket-unversioned/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delReq, "test", "testsecret", "us-east-1")
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	if got := delResp.Header.Get("x-amz-delete-marker"); got != "" {
		t.Fatalf("expected no delete marker header, got %q", got)
	}
	if got := delResp.Header.Get("x-amz-version-id"); got != "" {
		t.Fatalf("expected no version id, got %q", got)
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", delResp.StatusCode)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket-unversioned/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	if got := getResp.Header.Get("x-amz-delete-marker"); got != "" {
		t.Fatalf("expected no delete marker header on GET, got %q", got)
	}
	if got := getResp.Header.Get("x-amz-version-id"); got != "" {
		t.Fatalf("expected no version id on GET, got %q", got)
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
}

func TestS3E2ESuspendedBucketNullVersion(t *testing.T) {
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

	createReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket-suspended", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(createReq, "test", "testsecret", "us-east-1")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("PUT bucket error: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT bucket status: %d", createResp.StatusCode)
	}

	putBody := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>`
	setReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket-suspended?versioning", strings.NewReader(putBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	setReq.Header.Set("Content-Type", "application/xml")
	signRequest(setReq, "test", "testsecret", "us-east-1")
	setResp, err := http.DefaultClient.Do(setReq)
	if err != nil {
		t.Fatalf("PUT versioning error: %v", err)
	}
	setResp.Body.Close()
	if setResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT versioning status: %d", setResp.StatusCode)
	}

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket-suspended/key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(putReq, "test", "testsecret", "us-east-1")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	if got := putResp.Header.Get("x-amz-version-id"); got != "null" {
		t.Fatalf("expected null version id, got %q", got)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	getNullReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket-suspended/key?versionId=null", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(getNullReq, "test", "testsecret", "us-east-1")
	getNullResp, err := http.DefaultClient.Do(getNullReq)
	if err != nil {
		t.Fatalf("GET null error: %v", err)
	}
	getNullResp.Body.Close()
	if getNullResp.StatusCode != http.StatusOK {
		t.Fatalf("GET null status: %d", getNullResp.StatusCode)
	}

	delReq, err := http.NewRequest(http.MethodDelete, server.URL+"/bucket-suspended/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(delReq, "test", "testsecret", "us-east-1")
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	if got := delResp.Header.Get("x-amz-delete-marker"); got != "true" {
		t.Fatalf("expected delete marker header, got %q", got)
	}
	if got := delResp.Header.Get("x-amz-version-id"); got == "" {
		t.Fatalf("expected delete marker version id")
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", delResp.StatusCode)
	}
}

func TestS3E2ECopyObject(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/src", strings.NewReader("copy-data"))
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

	copyReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/dst", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	copyReq.Header.Set("X-Amz-Copy-Source", "/bucket/src")
	signRequest(copyReq, "test", "testsecret", "us-east-1")
	copyResp, err := http.DefaultClient.Do(copyReq)
	if err != nil {
		t.Fatalf("COPY error: %v", err)
	}
	copyResp.Body.Close()
	if copyResp.StatusCode != http.StatusOK {
		t.Fatalf("COPY status: %d", copyResp.StatusCode)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/dst", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(getReq, "test", "testsecret", "us-east-1")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "copy-data" {
		t.Fatalf("copy body mismatch: %q", string(body))
	}
}
