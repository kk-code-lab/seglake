package s3

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
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
	if parts[0] == "" {
		// suffix: -N
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, n, true
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if parts[1] == "" {
		return start, size - start, true
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
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
