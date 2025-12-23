package s3

import (
	"context"
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
	SecretLookup         func(ctx context.Context, accessKey string) (string, bool, error)
}

// VerifyRequest validates AWS SigV4 Authorization headers.
func (c *AuthConfig) VerifyRequest(r *http.Request) error {
	if c == nil || (c.AccessKey == "" && c.SecretKey == "" && c.SecretLookup == nil) {
		return nil
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return c.verifyPresigned(r)
	}
	if r.Header.Get("X-Amz-Date") == "" {
		if date := r.Header.Get("Date"); date != "" {
			if t, err := time.Parse(time.RFC1123, date); err == nil {
				r.Header.Set("X-Amz-Date", t.UTC().Format("20060102T150405Z"))
			}
		}
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
	regionRaw := credParts[2]
	region := normalizeRegion(regionRaw)
	service := credParts[3]
	term := credParts[4]
	if term != "aws4_request" || service != "s3" {
		return errSignatureMismatch
	}
	if c.Region != "" && region != normalizeRegion(c.Region) {
		return errSignatureMismatch
	}
	secretKey, err := c.secretFor(r.Context(), accessKey)
	if err != nil {
		return err
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

	payloadHashHeader := r.Header.Get("X-Amz-Content-Sha256")
	payloadHash := payloadHashHeader
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	// We do not stream-hash payloads yet; accept provided hash values for now.
	if !c.AllowUnsignedPayload && strings.EqualFold(payloadHash, "UNSIGNED-PAYLOAD") && payloadHashHeader != "" {
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
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateScope, regionRaw)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hashed[:]),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateScope, regionRaw, "s3")
	expected := hmacSHA256Hex(signingKey, stringToSign)
	if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(strings.ToLower(expected))) {
		return errSignatureMismatch
	}
	return nil
}

func (c *AuthConfig) verifyPresigned(r *http.Request) error {
	query := r.URL.Query()
	alg := query.Get("X-Amz-Algorithm")
	cred := query.Get("X-Amz-Credential")
	amzDate := query.Get("X-Amz-Date")
	signedHeaders := query.Get("X-Amz-SignedHeaders")
	signature := query.Get("X-Amz-Signature")
	expires := query.Get("X-Amz-Expires")
	if alg == "" || cred == "" || amzDate == "" || signedHeaders == "" || signature == "" || expires == "" {
		return errSignatureMismatch
	}
	if alg != "AWS4-HMAC-SHA256" {
		return errSignatureMismatch
	}

	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return errSignatureMismatch
	}
	accessKey := credParts[0]
	dateScope := credParts[1]
	regionRaw := credParts[2]
	region := normalizeRegion(regionRaw)
	service := credParts[3]
	term := credParts[4]
	if term != "aws4_request" || service != "s3" {
		return errSignatureMismatch
	}
	if c.Region != "" && region != normalizeRegion(c.Region) {
		return errSignatureMismatch
	}
	secretKey, err := c.secretFor(r.Context(), accessKey)
	if err != nil {
		return err
	}

	reqTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return errSignatureMismatch
	}
	maxSkew := c.MaxSkew
	if maxSkew == 0 {
		maxSkew = 5 * time.Minute
	}
	expSeconds, err := parseInt(expires)
	if err != nil || expSeconds < 1 || expSeconds > 604800 {
		return errSignatureMismatch
	}
	delta := time.Since(reqTime)
	if delta > time.Duration(expSeconds)*time.Second {
		return errSignatureMismatch
	}
	if delta < -maxSkew {
		return errTimeSkew
	}
	if !strings.HasPrefix(amzDate, dateScope) {
		return errSignatureMismatch
	}

	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalHeaders, signedHeadersLower, err := buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return errSignatureMismatch
	}

	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQueryPresigned(r),
		canonicalHeaders,
		strings.Join(signedHeadersLower, ";"),
		payloadHash,
	}, "\n")

	hashed := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateScope, regionRaw)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hashed[:]),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateScope, regionRaw, "s3")
	expected := hmacSHA256Hex(signingKey, stringToSign)
	if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(strings.ToLower(expected))) {
		return errSignatureMismatch
	}
	return nil
}

func (c *AuthConfig) secretFor(ctx context.Context, accessKey string) (string, error) {
	if accessKey == "" {
		return "", errAccessDenied
	}
	if c.AccessKey != "" && accessKey == c.AccessKey {
		if c.SecretKey == "" {
			return "", errAccessDenied
		}
		return c.SecretKey, nil
	}
	if c.SecretLookup == nil {
		return "", errAccessDenied
	}
	secret, enabled, err := c.SecretLookup(ctx, accessKey)
	if err != nil {
		return "", errAccessDenied
	}
	if !enabled || secret == "" {
		return "", errAccessDenied
	}
	return secret, nil
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

func canonicalQueryPresigned(r *http.Request) string {
	values := r.URL.Query()
	values.Del("X-Amz-Signature")
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

func normalizeRegion(r string) string {
	r = strings.ToLower(r)
	if r == "us" {
		return "us-east-1"
	}
	return r
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

func parseInt(s string) (int64, error) {
	var v int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, errors.New("invalid integer")
		}
		v = v*10 + int64(s[i]-'0')
	}
	return v, nil
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
