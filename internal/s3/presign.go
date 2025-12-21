package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Presign builds a presigned URL for a request.
func (c *AuthConfig) Presign(method, rawURL string, expires time.Duration) (string, error) {
	if c == nil || c.AccessKey == "" || c.SecretKey == "" {
		return "", errAccessDenied
	}
	if expires <= 0 || expires > 7*24*time.Hour {
		return "", errSignatureMismatch
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateScope := amzDate[:8]
	scope := dateScope + "/" + c.Region + "/s3/aws4_request"

	query := u.Query()
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", c.AccessKey+"/"+scope)
	query.Set("X-Amz-Date", amzDate)
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	query.Set("X-Amz-SignedHeaders", "host")
	u.RawQuery = query.Encode()

	canonicalRequest := strings.Join([]string{
		method,
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
	signingKey := deriveSigningKey(c.SecretKey, dateScope, c.Region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	query = u.Query()
	query.Set("X-Amz-Signature", signature)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func canonicalQueryPresignedFromValues(values url.Values) string {
	copied := make(url.Values, len(values))
	for k, vs := range values {
		list := make([]string, len(vs))
		copy(list, vs)
		copied[k] = list
	}
	copied.Del("X-Amz-Signature")
	type pair struct {
		k string
		v string
	}
	var pairs []pair
	for k, vs := range copied {
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
