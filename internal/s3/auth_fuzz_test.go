package s3

import (
	"net/http"
	"net/url"
	"testing"
)

func FuzzAuthParsing(f *testing.F) {
	// Future: add seed corpus with tricky signedHeaders ordering and duplicate query params.
	f.Add("AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef")
	f.Add("SignedHeaders=host;x-amz-date,Signature=deadbeef,Credential=AKIA/20240101/us-east-1/s3/aws4_request")
	f.Fuzz(func(t *testing.T, authHeader string) {
		_ = parseAuthParams(authHeader)
	})
}

func FuzzCanonicalQuery(f *testing.F) {
	// Future: add seed corpus with reserved characters and repeated keys.
	f.Add("a=1&b=2", "/")
	f.Add("a=1&a=2", "/bucket/key")
	f.Add("a=%2F&b=%2B", "/space here")
	f.Fuzz(func(t *testing.T, rawQuery string, path string) {
		u := &url.URL{Path: path, RawQuery: rawQuery}
		req := &http.Request{Method: http.MethodGet, URL: u}
		_ = canonicalQuery(req)
		_ = canonicalQueryPresigned(req)
	})
}

func FuzzCanonicalHeaders(f *testing.F) {
	// Future: add seed corpus with unusual whitespace and mixed-case headers.
	f.Add("host;x-amz-date", "example.com", "20250101T000000Z")
	f.Add("host;content-type", "example.com", "text/plain")
	f.Fuzz(func(t *testing.T, signedHeaders string, host string, headerValue string) {
		req := &http.Request{
			Method: http.MethodGet,
			URL:    &url.URL{Path: "/"},
			Host:   host,
			Header: http.Header{},
		}
		req.Header.Set("x-amz-date", headerValue)
		req.Header.Set("content-type", headerValue)
		_, _, _ = buildCanonicalHeaders(req, signedHeaders)
	})
}
