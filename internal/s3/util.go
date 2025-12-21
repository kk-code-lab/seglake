package s3

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"strconv"
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
