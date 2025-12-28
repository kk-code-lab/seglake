package s3

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func newRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(buf[:])
}

func intToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func ioCopy(dst io.Writer, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}

var httpTimeZone = time.FixedZone("GMT", 0)

func formatHTTPTime(t time.Time) string {
	return t.In(httpTimeZone).Format(time.RFC1123)
}

func parseHTTPTime(value string) (time.Time, error) {
	return time.Parse(time.RFC1123, value)
}

var errPayloadHashMismatch = errors.New("payload hash mismatch")
var errPayloadHashInvalid = errors.New("invalid payload hash")
var errBadDigest = errors.New("bad digest")
var errInvalidDigest = errors.New("invalid digest")
var errMissingContentLength = errors.New("missing content length")
var errInvalidContentLength = errors.New("invalid content length")
var errEntityTooLarge = errors.New("entity too large")

func parsePayloadHash(header string) (string, bool, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", false, nil
	}
	if header == "UNSIGNED-PAYLOAD" {
		return header, false, nil
	}
	if header == "STREAMING-UNSIGNED-PAYLOAD" || header == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" {
		return "UNSIGNED-PAYLOAD", false, nil
	}
	if strings.HasPrefix(header, "STREAMING-") {
		return "", false, errPayloadHashInvalid
	}
	if len(header) != 64 {
		return "", false, errPayloadHashInvalid
	}
	if _, err := hex.DecodeString(header); err != nil {
		return "", false, errPayloadHashInvalid
	}
	return strings.ToLower(header), true, nil
}

// aws-chunked support lives in streaming.go.

type validatingReader struct {
	reader      io.Reader
	shaExpected string
	shaVerify   bool
	md5Expected []byte
	shaHasher   hash.Hash
	md5Hasher   hash.Hash
	done        bool
}

func newValidatingReader(reader io.Reader, shaExpected string, shaVerify bool, md5Expected []byte) *validatingReader {
	v := &validatingReader{
		reader:      reader,
		shaExpected: strings.ToLower(shaExpected),
		shaVerify:   shaVerify,
		md5Expected: md5Expected,
	}
	if shaVerify {
		v.shaHasher = sha256.New()
	}
	if len(md5Expected) > 0 {
		v.md5Hasher = md5.New()
	}
	return v
}

func (r *validatingReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		if r.shaHasher != nil {
			_, _ = r.shaHasher.Write(p[:n])
		}
		if r.md5Hasher != nil {
			_, _ = r.md5Hasher.Write(p[:n])
		}
	}
	if err == io.EOF {
		r.done = true
		if r.shaVerify {
			sum := hex.EncodeToString(r.shaHasher.Sum(nil))
			if sum != r.shaExpected {
				return 0, errPayloadHashMismatch
			}
		}
		if r.md5Hasher != nil {
			sum := r.md5Hasher.Sum(nil)
			if !hmacEqual(sum, r.md5Expected) {
				return 0, errBadDigest
			}
		}
		return n, io.EOF
	}
	return n, err
}

type sizeLimitReader struct {
	reader    io.Reader
	remaining int64
}

func newSizeLimitReader(reader io.Reader, maxBytes int64) io.Reader {
	if maxBytes <= 0 {
		return reader
	}
	return &sizeLimitReader{reader: reader, remaining: maxBytes}
}

func (r *sizeLimitReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errEntityTooLarge
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		r.remaining -= int64(n)
	}
	if err == io.EOF {
		return n, err
	}
	if r.remaining <= 0 {
		return n, errEntityTooLarge
	}
	return n, err
}

func parseContentMD5(header string) ([]byte, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(decoded) != md5.Size {
		return nil, errInvalidDigest
	}
	return decoded, nil
}

func contentLengthFromRequest(r *http.Request) (int64, bool, error) {
	if r == nil {
		return 0, false, errMissingContentLength
	}
	if r.ContentLength >= 0 {
		return r.ContentLength, true, nil
	}
	decoded := strings.TrimSpace(r.Header.Get("X-Amz-Decoded-Content-Length"))
	if decoded == "" {
		return 0, false, errMissingContentLength
	}
	v, err := parseInt(decoded)
	if err != nil {
		return 0, false, errInvalidContentLength
	}
	return v, true, nil
}

func hmacEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	result := byte(0)
	for i := 0; i < len(a); i++ {
		result |= a[i] ^ b[i]
	}
	return result == 0
}

func parseRange(header string, size int64) (start int64, length int64, ok bool) {
	if !strings.HasPrefix(header, "bytes=") || size < 0 {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	return parseRangeSpec(parts[0], parts[1], size)
}

type byteRange struct {
	start  int64
	length int64
}

func parseRanges(header string, size int64) ([]byteRange, bool) {
	if !strings.HasPrefix(header, "bytes=") || size < 0 {
		return nil, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.Split(spec, ",")
	ranges := make([]byteRange, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		specParts := strings.SplitN(part, "-", 2)
		if len(specParts) != 2 {
			return nil, false
		}
		start, length, ok := parseRangeSpec(specParts[0], specParts[1], size)
		if !ok {
			return nil, false
		}
		ranges = append(ranges, byteRange{start: start, length: length})
	}
	return ranges, true
}

func parseRangeSpec(startStr, endStr string, size int64) (start int64, length int64, ok bool) {
	if startStr == "" {
		// suffix: -N
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, n, true
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if endStr == "" {
		return start, size - start, true
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end - start + 1, true
}

func formatContentRange(start, length, size int64) string {
	end := start + length - 1
	return "bytes " + strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end, 10) + "/" + strconv.FormatInt(size, 10)
}

var hostIDOnce sync.Once
var hostIDValue string

func hostID() string {
	hostIDOnce.Do(func() {
		host, _ := os.Hostname()
		sum := sha256.Sum256([]byte(host))
		hostIDValue = hex.EncodeToString(sum[:8])
		if hostIDValue == "" {
			hostIDValue = "seglake"
		}
	})
	return hostIDValue
}
