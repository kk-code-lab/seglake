package s3

import (
	"bytes"
	"io"
	"strings"
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
	})
}
