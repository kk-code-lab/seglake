package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSigV4AcceptsDateHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Date", time.Date(2025, 12, 21, 19, 0, 0, 0, time.UTC).Format(time.RFC1123))
	signRequestTest(req, "test", "testsecret", "us-east-1")

	auth := &AuthConfig{
		AccessKey:            "test",
		SecretKey:            "testsecret",
		Region:               "us-east-1",
		AllowUnsignedPayload: true,
	}
	if err := auth.VerifyRequest(req); err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
}

func TestSigV4CanonicalizesHeaderSpacing(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://example.com/bucket/key", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("X-Custom", "  a   b  c ")
	req.Header.Set("Host", req.URL.Host)

	// Sign with canonical header spacing normalized.
	req.Header.Set("Authorization", signWithCustomHeader(req, "test", "testsecret", "us-east-1", "x-custom"))

	auth := &AuthConfig{
		AccessKey:            "test",
		SecretKey:            "testsecret",
		Region:               "us-east-1",
		AllowUnsignedPayload: true,
	}
	if err := auth.VerifyRequest(req); err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
}

func signWithCustomHeader(r *http.Request, accessKey, secretKey, region, customHeader string) string {
	amzDate := r.Header.Get("X-Amz-Date")
	dateScope := amzDate[:8]
	scope := dateScope + "/" + region + "/s3/aws4_request"

	canonicalHeaders, signedHeaders := canonicalHeadersForRequestWith(r, []string{"host", "x-amz-content-sha256", "x-amz-date", customHeader})
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		"UNSIGNED-PAYLOAD",
	}, "\n")
	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")
	signingKey := deriveSigningKey(secretKey, dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	return "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + strings.Join(signedHeaders, ";") + "," +
		"Signature=" + signature
}

func signRequestTest(r *http.Request, accessKey, secretKey, region string) {
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequestWith(r, []string{"host", "x-amz-content-sha256", "x-amz-date"})
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
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

func canonicalHeadersForRequestWith(r *http.Request, headers []string) (string, []string) {
	var b strings.Builder
	for _, h := range headers {
		value := r.Header.Get(h)
		if h == "host" {
			value = r.Host
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(normalizeSpaces(value))
		b.WriteByte('\n')
	}
	return b.String(), headers
}
