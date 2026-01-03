//go:build e2e

package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
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
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
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
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              1 * time.Second,
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
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
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
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
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

func TestS3E2ESignedHeadersCustomHeader(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/signed-header", strings.NewReader("signed"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	putReq.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	putReq.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	putReq.Header.Set("X-Custom", "alpha")
	putReq.Header.Set("Host", putReq.URL.Host)
	putReq.Header.Set("Authorization", signWithCustomHeader(putReq, "test", "testsecret", "us-east-1", "x-custom"))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/signed-header", nil)
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
	if string(data) != "signed" {
		t.Fatalf("GET body mismatch")
	}
}

func TestS3E2EPayloadHashMismatch(t *testing.T) {
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

	putURL := server.URL + "/bucket/hash"
	badReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("wrong")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	payloadHash := sha256HexE2E([]byte("correct"))
	signRequestWithPayloadHash(badReq, "test", "testsecret", "us-east-1", payloadHash)
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	defer badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT status: %d", badResp.StatusCode)
	}
	body, _ := io.ReadAll(badResp.Body)
	if !bytes.Contains(body, []byte("XAmzContentSHA256Mismatch")) {
		t.Fatalf("expected hash mismatch error")
	}

	goodReq, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader([]byte("correct")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithPayloadHash(goodReq, "test", "testsecret", "us-east-1", payloadHash)
	goodResp, err := http.DefaultClient.Do(goodReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	goodResp.Body.Close()
	if goodResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", goodResp.StatusCode)
	}
}

func TestS3E2EMissingContentSHA256(t *testing.T) {
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

	req, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/missing-sha", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", req.URL.Host)

	canonicalHeaders := "host:" + req.Host + "\n" + "x-amz-date:" + amzDate + "\n"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req),
		canonicalQueryFromURL(req.URL),
		canonicalHeaders,
		"host;x-amz-date",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}, "\n")
	hash := sha256.Sum256([]byte(canonicalRequest))
	scope := dateScope + "/us-east-1/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")
	signingKey := deriveSigningKey("testsecret", dateScope, "us-east-1", "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=test/" + scope + "," +
		"SignedHeaders=host;x-amz-date," +
		"Signature=" + signature
	req.Header.Set("Authorization", auth)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("InvalidRequest")) {
		t.Fatalf("expected InvalidRequest error")
	}
	if !bytes.Contains(body, []byte("x-amz-content-sha256")) {
		t.Fatalf("expected missing header message")
	}
}

func TestS3E2EPresignedExpiry(t *testing.T) {
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

	auth := &AuthConfig{
		AccessKey: "test",
		SecretKey: "testsecret",
		Region:    "us-east-1",
		MaxSkew:   5 * time.Minute,
	}
	handler := &Handler{Engine: eng, Meta: store, Auth: auth}

	server := httptest.NewServer(handler)
	defer server.Close()

	putURL, err := auth.Presign(http.MethodPut, server.URL+"/bucket/presign-expire", 1*time.Second)
	if err != nil {
		t.Fatalf("presign put: %v", err)
	}
	getURL, err := auth.Presign(http.MethodGet, server.URL+"/bucket/presign-expire", 1*time.Second)
	if err != nil {
		t.Fatalf("presign get: %v", err)
	}

	putReq, err := http.NewRequest(http.MethodPut, putURL, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", putResp.StatusCode)
	}

	time.Sleep(2 * time.Second)
	getResp, err := http.Get(getURL)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", getResp.StatusCode)
	}
	body, _ := io.ReadAll(getResp.Body)
	if !bytes.Contains(body, []byte("SignatureDoesNotMatch")) {
		t.Fatalf("expected signature mismatch")
	}
}

func TestS3E2EPresignedRange(t *testing.T) {
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

	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/bucket/presigned-range", strings.NewReader("abcdef"))
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

	getURL, err := handler.Auth.Presign(http.MethodGet, server.URL+"/bucket/presigned-range", 5*time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	rangeReq, err := http.NewRequest(http.MethodGet, getURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	rangeReq.Header.Set("Range", "bytes=1-3")
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status: %d", rangeResp.StatusCode)
	}
	body, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "bcd" {
		t.Fatalf("range body mismatch")
	}
}

func TestS3E2ERequestTimeSkewed(t *testing.T) {
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
			MaxSkew:              2 * time.Minute,
		},
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/hello", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithTime(req, "test", "testsecret", "us-east-1", time.Now().Add(-10*time.Minute))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("RequestTimeTooSkewed")) {
		t.Fatalf("expected skew error")
	}
}
