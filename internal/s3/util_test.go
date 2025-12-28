package s3

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParsePayloadHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		header     string
		wantHash   string
		wantVerify bool
		wantErr    bool
	}{
		{
			name:       "empty",
			header:     "",
			wantHash:   "",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "unsigned",
			header:     "UNSIGNED-PAYLOAD",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "streaming unsigned",
			header:     "STREAMING-UNSIGNED-PAYLOAD",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "streaming unsigned trailer",
			header:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "valid hash",
			header:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantHash:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantVerify: true,
			wantErr:    false,
		},
		{
			name:       "invalid streaming",
			header:     "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			wantHash:   "",
			wantVerify: false,
			wantErr:    true,
		},
		{
			name:       "invalid length",
			header:     "abcd",
			wantHash:   "",
			wantVerify: false,
			wantErr:    true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotHash, gotVerify, err := parsePayloadHash(tc.header)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotHash != tc.wantHash || gotVerify != tc.wantVerify {
				t.Fatalf("got hash=%q verify=%v, want hash=%q verify=%v", gotHash, gotVerify, tc.wantHash, tc.wantVerify)
			}
		})
	}
}

func TestAWSChunkedReaderUnsignedTrailerChecksum(t *testing.T) {
	t.Parallel()

	payload := "hello"
	sum := sha256.Sum256([]byte(payload))
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	body := strings.Join([]string{
		"5",
		payload,
		"0",
		"x-amz-checksum-sha256: " + checksum,
		"",
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode:        streamingUnsignedTrailer,
		trailerKeys: []string{"x-amz-checksum-sha256"},
		expectedLen: int64(len(payload)),
	})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("expected decoded payload, got %q", string(got))
	}
}

func TestAWSChunkedReaderSigned(t *testing.T) {
	t.Parallel()

	payload := "hello"
	amzDate := "20251228T000000Z"
	dateScope := "20251228"
	region := "us-east-1"
	signingKey := deriveSigningKey("testsecret", dateScope, region, "s3")
	seed := "seed-signature"
	scope := dateScope + "/" + region + "/s3/aws4_request"

	chunkSig := signStreamingChunk(signingKey, amzDate, scope, seed, sha256Hex(payload))
	finalSig := signStreamingChunk(signingKey, amzDate, scope, chunkSig, emptySHA256Hex)

	body := strings.Join([]string{
		"5;chunk-signature=" + chunkSig,
		payload,
		"0;chunk-signature=" + finalSig,
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode: streamingSigned,
		sigv4: &sigv4Context{
			signingKey:    signingKey,
			seedSignature: seed,
			amzDate:       amzDate,
			scope:         scope,
		},
		expectedLen: int64(len(payload)),
	})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("expected decoded payload, got %q", string(got))
	}
}

func TestAWSChunkedReaderSignedTrailer(t *testing.T) {
	t.Parallel()

	payload := "hi"
	amzDate := "20251228T000000Z"
	dateScope := "20251228"
	region := "us-east-1"
	signingKey := deriveSigningKey("testsecret", dateScope, region, "s3")
	seed := "seed-signature"
	scope := dateScope + "/" + region + "/s3/aws4_request"

	sum := sha256.Sum256([]byte(payload))
	checksum := base64.StdEncoding.EncodeToString(sum[:])

	chunkSig := signStreamingChunk(signingKey, amzDate, scope, seed, sha256Hex(payload))
	finalSig := signStreamingChunk(signingKey, amzDate, scope, chunkSig, emptySHA256Hex)
	trailerSig := signStreamingTrailer(&sigv4Context{
		signingKey: signingKey,
		amzDate:    amzDate,
		scope:      scope,
	}, finalSig, "x-amz-checksum-sha256:"+checksum+"\n")

	body := strings.Join([]string{
		"2;chunk-signature=" + chunkSig,
		payload,
		"0;chunk-signature=" + finalSig,
		"x-amz-checksum-sha256: " + checksum,
		"x-amz-trailer-signature: " + trailerSig,
		"",
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode:        streamingSignedTrailer,
		trailerKeys: []string{"x-amz-checksum-sha256"},
		expectedLen: int64(len(payload)),
		sigv4: &sigv4Context{
			signingKey:    signingKey,
			seedSignature: seed,
			amzDate:       amzDate,
			scope:         scope,
		},
	})
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("expected decoded payload, got %q", string(got))
	}
}

func TestAWSChunkedReaderRejectsBadChunkSignature(t *testing.T) {
	t.Parallel()

	payload := "hello"
	amzDate := "20251228T000000Z"
	dateScope := "20251228"
	region := "us-east-1"
	signingKey := deriveSigningKey("testsecret", dateScope, region, "s3")
	seed := "seed-signature"
	scope := dateScope + "/" + region + "/s3/aws4_request"

	chunkSig := signStreamingChunk(signingKey, amzDate, scope, seed, sha256Hex(payload))
	finalSig := signStreamingChunk(signingKey, amzDate, scope, chunkSig, emptySHA256Hex)

	body := strings.Join([]string{
		"5;chunk-signature=deadbeef",
		payload,
		"0;chunk-signature=" + finalSig,
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode: streamingSigned,
		sigv4: &sigv4Context{
			signingKey:    signingKey,
			seedSignature: seed,
			amzDate:       amzDate,
			scope:         scope,
		},
		expectedLen: int64(len(payload)),
	})
	if _, err := io.ReadAll(r); err == nil {
		t.Fatalf("expected signature error")
	}
}

func TestAWSChunkedReaderRejectsBadTrailerSignature(t *testing.T) {
	t.Parallel()

	payload := "hi"
	amzDate := "20251228T000000Z"
	dateScope := "20251228"
	region := "us-east-1"
	signingKey := deriveSigningKey("testsecret", dateScope, region, "s3")
	seed := "seed-signature"
	scope := dateScope + "/" + region + "/s3/aws4_request"

	sum := sha256.Sum256([]byte(payload))
	checksum := base64.StdEncoding.EncodeToString(sum[:])

	chunkSig := signStreamingChunk(signingKey, amzDate, scope, seed, sha256Hex(payload))
	finalSig := signStreamingChunk(signingKey, amzDate, scope, chunkSig, emptySHA256Hex)

	body := strings.Join([]string{
		"2;chunk-signature=" + chunkSig,
		payload,
		"0;chunk-signature=" + finalSig,
		"x-amz-checksum-sha256: " + checksum,
		"x-amz-trailer-signature: deadbeef",
		"",
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode:        streamingSignedTrailer,
		trailerKeys: []string{"x-amz-checksum-sha256"},
		expectedLen: int64(len(payload)),
		sigv4: &sigv4Context{
			signingKey:    signingKey,
			seedSignature: seed,
			amzDate:       amzDate,
			scope:         scope,
		},
	})
	if _, err := io.ReadAll(r); err == nil {
		t.Fatalf("expected trailer signature error")
	}
}

func TestCRC64NVME(t *testing.T) {
	t.Parallel()

	data := []byte("123456789")
	crc := crc64nvmeUpdate(0xffffffffffffffff, data) ^ 0xffffffffffffffff
	if got := crc; got != 0xAE8B14860A799888 {
		t.Fatalf("crc64nvme mismatch: got %016x", got)
	}
}

func sha256Hex(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func TestValidateTrailerKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		keys    []string
		wantErr bool
	}{
		{
			name:    "none",
			keys:    nil,
			wantErr: false,
		},
		{
			name:    "single checksum",
			keys:    []string{"x-amz-checksum-sha256"},
			wantErr: false,
		},
		{
			name:    "mixed non-checksum",
			keys:    []string{"x-amz-meta-foo", "x-amz-trailer-signature"},
			wantErr: false,
		},
		{
			name:    "multiple checksums",
			keys:    []string{"x-amz-checksum-crc32", "x-amz-checksum-sha256"},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateTrailerKeys(tc.keys)
			if tc.wantErr {
				if !errors.Is(err, errInvalidDigest) {
					t.Fatalf("expected errInvalidDigest, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestAWSChunkedReaderLineTooLong(t *testing.T) {
	t.Parallel()

	line := strings.Repeat("a", maxChunkLineLen+10)
	body := line + "\r\n" + "0\r\n\r\n"
	r := newAWSChunkedReader(strings.NewReader(body), awsChunkedConfig{
		mode: streamingUnsigned,
	})
	_, err := io.ReadAll(r)
	if !errors.Is(err, errInvalidDigest) {
		t.Fatalf("expected errInvalidDigest, got %v", err)
	}
}

func TestDecodedContentLength(t *testing.T) {
	t.Parallel()

	r, _ := http.NewRequest(http.MethodPut, "http://example.com", nil)
	r.ContentLength = -1
	r.Header.Set("X-Amz-Decoded-Content-Length", "123")
	got, ok, err := decodedContentLength(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || got != 123 {
		t.Fatalf("expected ok=true and length=123, got ok=%v len=%d", ok, got)
	}
}
