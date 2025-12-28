package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func FuzzAWSChunkedReader(f *testing.F) {
	f.Add([]byte("0\r\n\r\n"))
	f.Add([]byte("5\r\nhello\r\n0\r\n\r\n"))
	f.Add([]byte("5;chunk-signature=deadbeef\r\nhello\r\n0;chunk-signature=deadbeef\r\n\r\n"))
	f.Add([]byte("1\r\nx\r\n0\r\nx-amz-checksum-sha256: Zm9v\r\n\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := awsChunkedConfig{
			mode:        streamingUnsigned,
			expectedLen: int64(len(data)),
		}
		r := newAWSChunkedReader(bytes.NewReader(data), cfg)
		_, _ = io.ReadAll(r)

		cfg = awsChunkedConfig{
			mode:        streamingUnsignedTrailer,
			trailerKeys: []string{"x-amz-checksum-sha256"},
			expectedLen: int64(len(data)),
		}
		r = newAWSChunkedReader(bytes.NewReader(data), cfg)
		_, _ = io.ReadAll(r)

		cfg = awsChunkedConfig{
			mode:        streamingSigned,
			expectedLen: int64(len(data)),
			sigv4:       &sigv4Context{signingKey: []byte("test"), seedSignature: strings.Repeat("0", 64), amzDate: "20240101T000000Z", scope: "20240101/us-east-1/s3/aws4_request"},
		}
		r = newAWSChunkedReader(bytes.NewReader(data), cfg)
		_, _ = io.ReadAll(r)

		maybeSaveFuzzInput(data)
	})
}

func maybeSaveFuzzInput(data []byte) {
	if os.Getenv("SEGLAKE_FUZZ_SAVE") == "" {
		return
	}
	if shouldSkipFuzzSave(data) {
		return
	}
	sum := sha256.Sum256(data)
	name := "seed-" + hex.EncodeToString(sum[:]) + ".bin"
	dir := filepath.Join("internal", "s3", "testdata", "fuzz", "FuzzAWSChunkedReader")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err == nil {
		return
	}
	_ = os.WriteFile(path, data, 0o644)
}

func shouldSkipFuzzSave(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	dir := filepath.Join("internal", "s3", "testdata", "fuzz", "FuzzAWSChunkedReader")
	entries, err := os.ReadDir(dir)
	if err == nil && len(entries) >= 20 {
		return true
	}
	return false
}
