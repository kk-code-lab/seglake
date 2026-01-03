package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSigV4RejectsWrongRegion(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "test", "testsecret", "us-east-1")
	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: "eu-west-1", AllowUnsignedPayload: true}
	if err := auth.VerifyRequest(req); err == nil {
		t.Fatalf("expected signature mismatch")
	}
}

func TestSigV4RejectsWrongService(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithService(req, "test", "testsecret", "us-east-1", "ec2")
	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: "us-east-1", AllowUnsignedPayload: true}
	if err := auth.VerifyRequest(req); err == nil {
		t.Fatalf("expected signature mismatch")
	}
}

func TestSigV4RejectsSkewedTime(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	amzDate := time.Now().Add(-10 * time.Minute).UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("Host", req.URL.Host)
	signRequestWithDate(req, "test", "testsecret", "us-east-1", amzDate)

	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: "us-east-1", MaxSkew: 2 * time.Minute, AllowUnsignedPayload: true}
	if err := auth.VerifyRequest(req); err == nil {
		t.Fatalf("expected time skew error")
	}
}

func TestSigV4RejectsUnsignedPayloadWhenDisallowed(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestTest(req, "test", "testsecret", "us-east-1")
	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: "us-east-1", AllowUnsignedPayload: false}
	if err := auth.VerifyRequest(req); err == nil {
		t.Fatalf("expected signature mismatch")
	}
}

func TestSigV4RejectsMissingContentSHA256Header(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequestWithoutPayloadHash(req, "test", "testsecret", "us-east-1")
	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: "us-east-1", AllowUnsignedPayload: true}
	if err := auth.VerifyRequest(req); err != errMissingContentSHA256 {
		t.Fatalf("expected missing content sha256, got %v", err)
	}
}

func TestSigV4RejectsPresignFutureSkew(t *testing.T) {
	rawURL := "http://example.com/bucket/key"
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	amzDate := time.Now().Add(10 * time.Minute).UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	region := "us-east-1"
	scope := dateScope + "/" + region + "/s3/aws4_request"

	query := u.Query()
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test/"+scope)
	query.Set("X-Amz-Date", amzDate)
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(300), 10))
	query.Set("X-Amz-SignedHeaders", "host")
	u.RawQuery = query.Encode()

	canonicalRequest := strings.Join([]string{
		http.MethodGet,
		u.EscapedPath(),
		canonicalQueryPresignedFromValues(u.Query()),
		"host:" + u.Host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")
	signingKey := deriveSigningKey("testsecret", dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	query = u.Query()
	query.Set("X-Amz-Signature", signature)
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	auth := &AuthConfig{AccessKey: "test", SecretKey: "testsecret", Region: region, MaxSkew: 2 * time.Minute, AllowUnsignedPayload: true}
	if err := auth.VerifyRequest(req); err != errTimeSkew {
		t.Fatalf("expected time skew error, got %v", err)
	}
}

func signRequestWithDate(r *http.Request, accessKey, secretKey, region, amzDate string) {
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequestWith(r, []string{"host", "x-amz-content-sha256", "x-amz-date"})
	canonicalRequest := stringsJoinLines(
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
		canonicalHeaders,
		stringsJoin(signedHeaders, ";"),
		"UNSIGNED-PAYLOAD",
	)
	hash := sha256SumHex(canonicalRequest)
	scope := dateScope + "/" + region + "/s3/aws4_request"
	stringToSign := stringsJoinLines(
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hash,
	)
	signingKey := deriveSigningKey(secretKey, dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + stringsJoin(signedHeaders, ";") + "," +
		"Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func signRequestWithService(r *http.Request, accessKey, secretKey, region, service string) {
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequestWith(r, []string{"host", "x-amz-content-sha256", "x-amz-date"})
	canonicalRequest := stringsJoinLines(
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
		canonicalHeaders,
		stringsJoin(signedHeaders, ";"),
		"UNSIGNED-PAYLOAD",
	)
	hash := sha256SumHex(canonicalRequest)
	scope := dateScope + "/" + region + "/" + service + "/aws4_request"
	stringToSign := stringsJoinLines(
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hash,
	)
	signingKey := deriveSigningKey(secretKey, dateScope, region, service)
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + stringsJoin(signedHeaders, ";") + "," +
		"Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func signRequestWithoutPayloadHash(r *http.Request, accessKey, secretKey, region string) {
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequestWith(r, []string{"host", "x-amz-date"})
	canonicalRequest := stringsJoinLines(
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
		canonicalHeaders,
		stringsJoin(signedHeaders, ";"),
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	)
	hash := sha256SumHex(canonicalRequest)
	scope := dateScope + "/" + region + "/s3/aws4_request"
	stringToSign := stringsJoinLines(
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hash,
	)
	signingKey := deriveSigningKey(secretKey, dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + stringsJoin(signedHeaders, ";") + "," +
		"Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func stringsJoin(items []string, sep string) string {
	if len(items) == 0 {
		return ""
	}
	out := items[0]
	for i := 1; i < len(items); i++ {
		out += sep + items[i]
	}
	return out
}

func stringsJoinLines(lines ...string) string {
	if len(lines) == 0 {
		return ""
	}
	out := lines[0]
	for i := 1; i < len(lines); i++ {
		out += "\n" + lines[i]
	}
	return out
}

func sha256SumHex(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}
