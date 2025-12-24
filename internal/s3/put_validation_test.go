package s3

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPutValidatesContentMD5(t *testing.T) {
	h := newTestHandler(t)
	body := "hello"
	sum := md5.Sum([]byte(body))
	md5b64 := base64.StdEncoding.EncodeToString(sum[:])

	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader(body))
	req.Header.Set("Content-MD5", md5b64)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	reqBad := httptest.NewRequest(http.MethodPut, "/bucket/key-bad", strings.NewReader(body))
	badSum := md5.Sum([]byte("world"))
	reqBad.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(badSum[:]))
	wBad := httptest.NewRecorder()
	h.ServeHTTP(wBad, reqBad)
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", wBad.Code)
	}
	if !strings.Contains(wBad.Body.String(), "BadDigest") {
		t.Fatalf("expected BadDigest error, got %s", wBad.Body.String())
	}
}

func TestPutRequiresContentLength(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", io.NopCloser(strings.NewReader("x")))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.Header.Del("Content-Length")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusLengthRequired {
		t.Fatalf("expected 411, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "MissingContentLength") {
		t.Fatalf("expected MissingContentLength error, got %s", w.Body.String())
	}
}

func TestPutEnforcesMaxObjectSize(t *testing.T) {
	h := newTestHandler(t)
	h.MaxObjectSize = 3
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("abcd"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "EntityTooLarge") {
		t.Fatalf("expected EntityTooLarge error, got %s", w.Body.String())
	}
}

func TestGetSetsContentTypeAndConditionals(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status: %d", w.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	getReq.Header.Set("If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(time.RFC1123))
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", getW.Code)
	}

	unmodReq := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	unmodReq.Header.Set("If-Unmodified-Since", time.Now().Add(-time.Hour).UTC().Format(time.RFC1123))
	unmodW := httptest.NewRecorder()
	h.ServeHTTP(unmodW, unmodReq)
	if unmodW.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", unmodW.Code)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/bucket/key", nil)
	headW := httptest.NewRecorder()
	h.ServeHTTP(headW, headReq)
	if got := headW.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("expected content-type text/plain, got %q", got)
	}
}

func TestOptionsCORS(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodOptions, "/bucket/key", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "PUT")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got == "" {
		t.Fatalf("expected Access-Control-Allow-Origin to be set")
	}
	if got := w.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatalf("expected Access-Control-Allow-Methods to be set")
	}
}
