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

func TestSSECustomerHeadersRejected(t *testing.T) {
	h := newTestHandler(t)

	cases := []struct {
		name   string
		method string
		target string
		body   string
		header string
		setup  func(*http.Request)
	}{
		{
			name:   "put object",
			method: http.MethodPut,
			target: "/bucket/key",
			body:   "data",
			header: "X-Amz-Server-Side-Encryption-Customer-Algorithm",
		},
		{
			name:   "get object",
			method: http.MethodGet,
			target: "/bucket/key",
			header: "X-Amz-Server-Side-Encryption-Customer-Key",
		},
		{
			name:   "head object",
			method: http.MethodHead,
			target: "/bucket/key",
			header: "X-Amz-Server-Side-Encryption-Customer-Key-MD5",
		},
		{
			name:   "copy object destination",
			method: http.MethodPut,
			target: "/bucket/copy",
			header: "X-Amz-Server-Side-Encryption-Customer-Algorithm",
			setup: func(r *http.Request) {
				r.Header.Set("X-Amz-Copy-Source", "/bucket/key")
			},
		},
		{
			name:   "copy object source",
			method: http.MethodPut,
			target: "/bucket/copy",
			header: "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm",
			setup: func(r *http.Request) {
				r.Header.Set("X-Amz-Copy-Source", "/bucket/key")
			},
		},
		{
			name:   "initiate multipart",
			method: http.MethodPost,
			target: "/bucket/key?uploads",
			header: "X-Amz-Server-Side-Encryption-Customer-Algorithm",
		},
		{
			name:   "upload part",
			method: http.MethodPut,
			target: "/bucket/key?partNumber=1&uploadId=upload-id",
			body:   "part",
			header: "X-Amz-Server-Side-Encryption-Customer-Key",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.target, body)
			req.Header.Set(tc.header, "AES256")
			if tc.setup != nil {
				tc.setup(req)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusNotImplemented {
				t.Fatalf("expected 501, got %d; body=%s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "NotImplemented") {
				t.Fatalf("expected NotImplemented error, got %s", w.Body.String())
			}
			if !strings.Contains(w.Body.String(), http.CanonicalHeaderKey(tc.header)) {
				t.Fatalf("expected header name in error, got %s", w.Body.String())
			}
		})
	}
}

func TestSSES3PutGetHeadAndInvalidHeaders(t *testing.T) {
	h := newTestHandler(t)
	put := httptest.NewRequest(http.MethodPut, "/bucket/encrypted", strings.NewReader("secret payload"))
	put.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT status: %d body=%s", putW.Code, putW.Body.String())
	}
	if got := putW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected SSE header on PUT, got %q", got)
	}

	get := httptest.NewRequest(http.MethodGet, "/bucket/encrypted", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET status: %d body=%s", getW.Code, getW.Body.String())
	}
	if getW.Body.String() != "secret payload" {
		t.Fatalf("GET body mismatch: %q", getW.Body.String())
	}
	if got := getW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected SSE header on GET, got %q", got)
	}

	head := httptest.NewRequest(http.MethodHead, "/bucket/encrypted", nil)
	headW := httptest.NewRecorder()
	h.ServeHTTP(headW, head)
	if headW.Code != http.StatusOK {
		t.Fatalf("HEAD status: %d", headW.Code)
	}
	if got := headW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected SSE header on HEAD, got %q", got)
	}

	badGet := httptest.NewRequest(http.MethodGet, "/bucket/encrypted", nil)
	badGet.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	badGetW := httptest.NewRecorder()
	h.ServeHTTP(badGetW, badGet)
	if badGetW.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", badGetW.Code)
	}

	badPut := httptest.NewRequest(http.MethodPut, "/bucket/bad", strings.NewReader("x"))
	badPut.Header.Set("X-Amz-Server-Side-Encryption", "AES128")
	badPutW := httptest.NewRecorder()
	h.ServeHTTP(badPutW, badPut)
	if badPutW.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", badPutW.Code)
	}

	kmsPut := httptest.NewRequest(http.MethodPut, "/bucket/kms", strings.NewReader("x"))
	kmsPut.Header.Set("X-Amz-Server-Side-Encryption", "aws:kms")
	kmsPutW := httptest.NewRecorder()
	h.ServeHTTP(kmsPutW, kmsPut)
	if kmsPutW.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", kmsPutW.Code)
	}
}

func TestSSES3RequestFailsWhenDisabled(t *testing.T) {
	h := newTestHandlerWithoutSSE(t)

	put := httptest.NewRequest(http.MethodPut, "/bucket/encrypted", strings.NewReader("secret"))
	put.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusBadRequest {
		t.Fatalf("expected PUT 400, got %d body=%s", putW.Code, putW.Body.String())
	}
	if !strings.Contains(putW.Body.String(), "InvalidRequest") {
		t.Fatalf("expected InvalidRequest, got %s", putW.Body.String())
	}

	init := httptest.NewRequest(http.MethodPost, "/bucket/encrypted?uploads", nil)
	init.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	initW := httptest.NewRecorder()
	h.ServeHTTP(initW, init)
	if initW.Code != http.StatusBadRequest {
		t.Fatalf("expected initiate 400, got %d body=%s", initW.Code, initW.Body.String())
	}
}

func TestSSES3CopyModes(t *testing.T) {
	h := newTestHandler(t)
	put := httptest.NewRequest(http.MethodPut, "/bucket/src", strings.NewReader("copy me"))
	put.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("source PUT status: %d", putW.Code)
	}

	copyPlain := httptest.NewRequest(http.MethodPut, "/bucket/plain-copy", nil)
	copyPlain.Header.Set("X-Amz-Copy-Source", "/bucket/src")
	copyPlainW := httptest.NewRecorder()
	h.ServeHTTP(copyPlainW, copyPlain)
	if copyPlainW.Code != http.StatusOK {
		t.Fatalf("copy plaintext status: %d body=%s", copyPlainW.Code, copyPlainW.Body.String())
	}
	if got := copyPlainW.Header().Get("x-amz-server-side-encryption"); got != "" {
		t.Fatalf("expected plaintext copy without SSE header, got %q", got)
	}

	copyEncrypted := httptest.NewRequest(http.MethodPut, "/bucket/encrypted-copy", nil)
	copyEncrypted.Header.Set("X-Amz-Copy-Source", "/bucket/src")
	copyEncrypted.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	copyEncryptedW := httptest.NewRecorder()
	h.ServeHTTP(copyEncryptedW, copyEncrypted)
	if copyEncryptedW.Code != http.StatusOK {
		t.Fatalf("copy encrypted status: %d body=%s", copyEncryptedW.Code, copyEncryptedW.Body.String())
	}
	if got := copyEncryptedW.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("expected encrypted copy SSE header, got %q", got)
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

func TestPutRequiresIfMatchOnOverwrite(t *testing.T) {
	h := newTestHandler(t)
	h.RequireIfMatchBuckets = map[string]struct{}{"bucket": {}}

	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("first"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("initial PUT status: %d", w.Code)
	}
	etag := w.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("expected ETag")
	}

	overwrite := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("second"))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, overwrite)
	if w2.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", w2.Code)
	}

	bad := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("third"))
	bad.Header.Set("If-Match", "\"deadbeef\"")
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, bad)
	if w3.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", w3.Code)
	}

	star := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("star"))
	star.Header.Set("If-Match", "*")
	wStar := httptest.NewRecorder()
	h.ServeHTTP(wStar, star)
	if wStar.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wStar.Code)
	}
	starETag := wStar.Header().Get("ETag")
	if starETag == "" {
		t.Fatalf("expected star ETag")
	}

	ok := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("fourth"))
	ok.Header.Set("If-Match", starETag)
	w4 := httptest.NewRecorder()
	h.ServeHTTP(w4, ok)
	if w4.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w4.Code)
	}
}

func TestPutRequiresIfMatchSkipsDeleteMarker(t *testing.T) {
	h := newTestHandler(t)
	h.RequireIfMatchBuckets = map[string]struct{}{"bucket": {}}

	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("first"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("initial PUT status: %d", w.Code)
	}

	del := httptest.NewRequest(http.MethodDelete, "/bucket/key", nil)
	wDel := httptest.NewRecorder()
	h.ServeHTTP(wDel, del)
	if wDel.Code != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", wDel.Code)
	}

	overwrite := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("second"))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, overwrite)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}
}

func TestCopyRequiresIfMatchOnOverwrite(t *testing.T) {
	h := newTestHandler(t)
	h.RequireIfMatchBuckets = map[string]struct{}{"bucket": {}}

	putSrc := httptest.NewRequest(http.MethodPut, "/bucket/src", strings.NewReader("src"))
	putSrcW := httptest.NewRecorder()
	h.ServeHTTP(putSrcW, putSrc)
	if putSrcW.Code != http.StatusOK {
		t.Fatalf("source PUT status: %d", putSrcW.Code)
	}

	putDst := httptest.NewRequest(http.MethodPut, "/bucket/dst", strings.NewReader("dst"))
	putDstW := httptest.NewRecorder()
	h.ServeHTTP(putDstW, putDst)
	if putDstW.Code != http.StatusOK {
		t.Fatalf("dest PUT status: %d", putDstW.Code)
	}
	dstETag := putDstW.Header().Get("ETag")
	if dstETag == "" {
		t.Fatalf("expected dest ETag")
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/bucket/dst", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/bucket/src")
	copyW := httptest.NewRecorder()
	h.ServeHTTP(copyW, copyReq)
	if copyW.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", copyW.Code)
	}

	copyReqOK := httptest.NewRequest(http.MethodPut, "/bucket/dst", nil)
	copyReqOK.Header.Set("X-Amz-Copy-Source", "/bucket/src")
	copyReqOK.Header.Set("If-Match", dstETag)
	copyWOK := httptest.NewRecorder()
	h.ServeHTTP(copyWOK, copyReqOK)
	if copyWOK.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", copyWOK.Code)
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
