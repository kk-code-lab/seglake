//go:build e2e

package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestS3E2EUnsignedPayload(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/key"
	body := []byte("hello world")
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(req, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", resp.StatusCode)
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
	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body mismatch")
	}
}

func TestS3E2ENoAuth(t *testing.T) {
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
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket-noauth/key"
	body := []byte("hello no auth")
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", resp.StatusCode)
	}

	headReq, err := http.NewRequest(http.MethodHead, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	headResp, err := http.DefaultClient.Do(headReq)
	if err != nil {
		t.Fatalf("HEAD error: %v", err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status: %d", headResp.StatusCode)
	}
	if headResp.Header.Get("ETag") == "" {
		t.Fatalf("expected ETag")
	}

	getReq, err := http.NewRequest(http.MethodGet, putURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body mismatch")
	}
}

func TestS3E2EBadSignature(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/badsig"
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(req, "test", "wrongsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestS3E2ETimeSkew(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   1 * time.Second,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/skew"
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithTime(req, "test", "testsecret", "us-east-1", time.Now().UTC().Add(-10*time.Second))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestS3E2EPresignedGet(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/presigned"
	putReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("presigned")))
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

	presigned, err := handler.Auth.Presign(http.MethodGet, putURL, 5*time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	getResp, err := http.Get(presigned)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "presigned" {
		t.Fatalf("GET body mismatch")
	}
}

func TestS3E2EPresignedPut(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL := server.URL + "/bucket/presigned-put"
	presigned, err := handler.Auth.Presign(http.MethodPut, putURL, 5*time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}

	req, err := http.NewRequest(http.MethodPut, presigned, bytes.NewReader([]byte("presigned-put")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", resp.StatusCode)
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
	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "presigned-put" {
		t.Fatalf("GET body mismatch")
	}
}

func TestS3E2EListV2(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a/one.txt")
	put("a/two.txt")
	put("b/three.txt")

	listURL := server.URL + "/bucket?list-type=2&prefix=a&delimiter=/&max-keys=10"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("List status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>")) {
		t.Fatalf("expected common prefix")
	}
	if bytes.Contains(body, []byte("<Key>a/one.txt</Key>")) {
		t.Fatalf("expected keys under prefix to be grouped by delimiter")
	}
}

func TestS3E2EListV2Continuation(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&max-keys=1"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) {
		t.Fatalf("expected truncated response")
	}
	tokenStart := bytes.Index(body, []byte("<NextContinuationToken>"))
	if tokenStart < 0 {
		t.Fatalf("missing token")
	}
	tokenStart += len("<NextContinuationToken>")
	tokenEnd := bytes.Index(body[tokenStart:], []byte("</NextContinuationToken>"))
	if tokenEnd < 0 {
		t.Fatalf("missing token end")
	}
	token := string(body[tokenStart : tokenStart+tokenEnd])

	listURL2 := server.URL + "/bucket?list-type=2&max-keys=2&continuation-token=" + url.QueryEscape(token)
	listReq2, err := http.NewRequest(http.MethodGet, listURL2, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq2, "test", "testsecret", "us-east-1")
	resp2, err := http.DefaultClient.Do(listReq2)
	if err != nil {
		t.Fatalf("List2 error: %v", err)
	}
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body2, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("unexpected first key repeated")
	}
	if !bytes.Contains(body2, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("expected next key")
	}
}

func TestS3E2EListV2StartAfter(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&start-after=a.txt"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("start-after should exclude a.txt")
	}
	if !bytes.Contains(body, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("expected b.txt")
	}
}

func TestS3E2EListV2RawContinuationToken(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	put := func(key string) {
		url := server.URL + "/bucket/" + key
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status: %d", resp.StatusCode)
		}
	}

	put("a.txt")
	put("b.txt")
	put("c.txt")

	listURL := server.URL + "/bucket?list-type=2&continuation-token=b.txt"
	listReq, err := http.NewRequest(http.MethodGet, listURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if bytes.Contains(body, []byte("<Key>a.txt</Key>")) {
		t.Fatalf("continuation token should skip a.txt")
	}
	if bytes.Contains(body, []byte("<Key>b.txt</Key>")) {
		t.Fatalf("continuation token should skip b.txt")
	}
	if !bytes.Contains(body, []byte("<Key>c.txt</Key>")) {
		t.Fatalf("expected c.txt")
	}
}

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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
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

func TestS3E2EMultipart(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/key?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	body, _ := io.ReadAll(initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}
	var initResult initiateMultipartResult
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("init decode: %v", err)
	}

	putPart := func(n int, data string) string {
		req, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/key?partNumber="+strconv.Itoa(n)+"&uploadId="+initResult.UploadID, strings.NewReader(data))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT part error: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT part status: %d", resp.StatusCode)
		}
		return resp.Header.Get("ETag")
	}

	etag1 := putPart(1, strings.Repeat("a", 5<<20))
	etag2 := putPart(2, "tail")

	completeBody := `<CompleteMultipartUpload>` +
		`<Part><PartNumber>1</PartNumber><ETag>` + etag1 + `</ETag></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>` + etag2 + `</ETag></Part>` +
		`</CompleteMultipartUpload>`
	completeReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/key?uploadId="+initResult.UploadID, strings.NewReader(completeBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(completeReq, "test", "testsecret", "us-east-1")
	completeResp, err := http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatalf("complete error: %v", err)
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status: %d", completeResp.StatusCode)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list uploads error: %v", err)
	}
	listBody, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list uploads status: %d", listResp.StatusCode)
	}
	if bytes.Contains(listBody, []byte(initResult.UploadID)) {
		t.Fatalf("completed upload should not be listed")
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/key", nil)
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
	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !strings.HasPrefix(string(got), strings.Repeat("a", 5<<20)) {
		t.Fatalf("GET body prefix mismatch")
	}
}

func TestS3E2EListMultipartUploads(t *testing.T) {
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
			AccessKey: "test",
			SecretKey: "testsecret",
			Region:    "us-east-1",
			MaxSkew:   5 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	initReq, err := http.NewRequest(http.MethodPost, server.URL+"/bucket/a?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, "test", "testsecret", "us-east-1")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}

	listReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(listReq, "test", "testsecret", "us-east-1")
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	body, _ := io.ReadAll(listResp.Body)
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", listResp.StatusCode)
	}
	if !bytes.Contains(body, []byte("<ListMultipartUploadsResult>")) {
		t.Fatalf("expected xml response")
	}
}
func signRequest(r *http.Request, accessKey, secretKey, region string) {
	signRequestWithTime(r, accessKey, secretKey, region, time.Now().UTC())
}

func signRequestWithTime(r *http.Request, accessKey, secretKey, region string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequest(r)
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQueryFromURL(r.URL),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		"UNSIGNED-PAYLOAD",
	}, "\n")

	hash := sha256.Sum256([]byte(canonicalRequest))
	scope := dateScope + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + strings.Join(signedHeaders, ";") + "," +
		"Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func canonicalHeadersForRequest(r *http.Request) (string, []string) {
	headers := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	var b strings.Builder
	for _, h := range headers {
		var value string
		if h == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(h)
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('\n')
	}
	return b.String(), headers
}

func canonicalQueryFromURL(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	values := u.Query()
	var pairs []string
	for k, vs := range values {
		for _, v := range vs {
			pairs = append(pairs, encodeRfc3986(k)+"="+encodeRfc3986(v))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}
