//go:build e2e

package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

func extractXMLTag(body, tag string) string {
	start := "<" + tag + ">"
	end := "</" + tag + ">"
	i := strings.Index(body, start)
	if i < 0 {
		return ""
	}
	j := strings.Index(body[i+len(start):], end)
	if j < 0 {
		return ""
	}
	return body[i+len(start) : i+len(start)+j]
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

func signRequestWithPayloadHash(r *http.Request, accessKey, secretKey, region, payloadHash string) {
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequest(r)
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQueryFromURL(r.URL),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
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

func sha256HexE2E(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
