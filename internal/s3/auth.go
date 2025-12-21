package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// AuthConfig configures SigV4 validation.
type AuthConfig struct {
	AccessKey            string
	SecretKey            string
	Region               string
	MaxSkew              time.Duration
	AllowUnsignedPayload bool
}

// VerifyRequest validates AWS SigV4 Authorization headers.
func (c *AuthConfig) VerifyRequest(r *http.Request) error {
	if c == nil || c.AccessKey == "" || c.SecretKey == "" {
		return nil
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return errAccessDenied
	}
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return errSignatureMismatch
	}
	params := parseAuthParams(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "))
	credential := params["Credential"]
	signedHeaders := params["SignedHeaders"]
	signature := params["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return errSignatureMismatch
	}

	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 {
		return errSignatureMismatch
	}
	accessKey := credParts[0]
	dateScope := credParts[1]
	region := credParts[2]
	service := credParts[3]
	term := credParts[4]
	if accessKey != c.AccessKey || term != "aws4_request" || service != "s3" {
		return errSignatureMismatch
	}
	if c.Region != "" && region != c.Region {
		return errSignatureMismatch
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return errSignatureMismatch
	}
	reqTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return errSignatureMismatch
	}
	maxSkew := c.MaxSkew
	if maxSkew == 0 {
		maxSkew = 5 * time.Minute
	}
	if delta := time.Since(reqTime); delta > maxSkew || delta < -maxSkew {
		return errTimeSkew
	}
	if !strings.HasPrefix(amzDate, dateScope) {
		return errSignatureMismatch
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	if payloadHash != "UNSIGNED-PAYLOAD" && !c.AllowUnsignedPayload {
		return errSignatureMismatch
	}
	if payloadHash != "UNSIGNED-PAYLOAD" && c.AllowUnsignedPayload {
		// We do not stream-hash in the handler yet.
		return errSignatureMismatch
	}

	canonicalHeaders, signedHeadersLower, err := buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return errSignatureMismatch
	}
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQuery(r),
		canonicalHeaders,
		strings.Join(signedHeadersLower, ";"),
		payloadHash,
	}, "\n")

	hashed := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateScope, region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hashed[:]),
	}, "\n")

	signingKey := deriveSigningKey(c.SecretKey, dateScope, region, "s3")
	expected := hmacSHA256Hex(signingKey, stringToSign)
	if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(strings.ToLower(expected))) {
		return errSignatureMismatch
	}
	return nil
}

var (
	errSignatureMismatch = errors.New("signature mismatch")
	errAccessDenied      = errors.New("access denied")
	errTimeSkew          = errors.New("request time too skewed")
)

func parseAuthParams(s string) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(s, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out[kv[0]] = strings.Trim(kv[1], " ")
	}
	return out
}

func canonicalURI(r *http.Request) string {
	uri := r.URL.EscapedPath()
	if uri == "" {
		return "/"
	}
	return uri
}

func canonicalQuery(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return ""
	}
	values := r.URL.Query()
	type pair struct {
		k string
		v string
	}
	var pairs []pair
	for k, vs := range values {
		for _, v := range vs {
			pairs = append(pairs, pair{encodeRfc3986(k), encodeRfc3986(v)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k == pairs[j].k {
			return pairs[i].v < pairs[j].v
		}
		return pairs[i].k < pairs[j].k
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func buildCanonicalHeaders(r *http.Request, signedHeaders string) (string, []string, error) {
	parts := strings.Split(signedHeaders, ";")
	headers := make([]string, 0, len(parts))
	for _, h := range parts {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == "" {
			continue
		}
		headers = append(headers, h)
	}
	sort.Strings(headers)
	var b strings.Builder
	for _, h := range headers {
		var value string
		if h == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(h)
		}
		if value == "" {
			return "", nil, fmt.Errorf("missing signed header %s", h)
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(normalizeSpaces(value))
		b.WriteByte('\n')
	}
	return b.String(), headers, nil
}

func normalizeSpaces(s string) string {
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

func encodeRfc3986(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteString(fmt.Sprintf("%%%02X", c))
	}
	return b.String()
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}
