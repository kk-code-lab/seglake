package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func FuzzAWSChunkedReader(f *testing.F) {
	// Future: add corpus entries with valid signed+trailer chunks to reach deeper verification paths.
	f.Add([]byte("0\r\n\r\n"))
	f.Add([]byte("5\r\nhello\r\n0\r\n\r\n"))
	f.Add([]byte("5;chunk-signature=deadbeef\r\nhello\r\n0;chunk-signature=deadbeef\r\n\r\n"))
	f.Add([]byte("1\r\nx\r\n0\r\nx-amz-checksum-sha256: Zm9v\r\n\r\n"))
	f.Add(makeUnsignedTrailerSeed([]byte("hello")))
	f.Add(makeSignedSeed([]byte("hello")))
	f.Add(makeSignedTrailerSeed([]byte("hello")))

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

		cfg = awsChunkedConfig{
			mode:        streamingSignedTrailer,
			expectedLen: int64(len(data)),
			trailerKeys: []string{"x-amz-checksum-sha256"},
			sigv4:       &sigv4Context{signingKey: []byte("test"), seedSignature: strings.Repeat("0", 64), amzDate: "20240101T000000Z", scope: "20240101/us-east-1/s3/aws4_request"},
		}
		r = newAWSChunkedReader(bytes.NewReader(data), cfg)
		_, _ = io.ReadAll(r)

		maybeSaveFuzzInput(data)
	})
}

func makeUnsignedTrailerSeed(payload []byte) []byte {
	checksum := sha256.Sum256(payload)
	trailers := map[string]string{
		"x-amz-checksum-sha256": base64.StdEncoding.EncodeToString(checksum[:]),
	}
	return buildChunkedPayload(payload, trailers, nil, "")
}

func makeSignedSeed(payload []byte) []byte {
	ctx := testSigv4Context()
	prev := strings.Repeat("0", 64)
	chunkSig := signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, prev, sha256HexBytes(payload))
	prev = strings.ToLower(chunkSig)
	zeroSig := signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, prev, emptySHA256Hex)
	zeroLine := "0;chunk-signature=" + zeroSig + "\r\n\r\n"
	header := chunkHeader(payload, chunkSig)
	return append([]byte(header+string(payload)+"\r\n"), []byte(zeroLine)...)
}

func makeSignedTrailerSeed(payload []byte) []byte {
	ctx := testSigv4Context()
	prev := strings.Repeat("0", 64)
	chunkSig := signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, prev, sha256HexBytes(payload))
	prev = strings.ToLower(chunkSig)
	zeroSig := signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, prev, emptySHA256Hex)
	prev = strings.ToLower(zeroSig)
	checksum := sha256.Sum256(payload)
	trailers := map[string]string{
		"x-amz-checksum-sha256": base64.StdEncoding.EncodeToString(checksum[:]),
	}
	canonical, err := canonicalizeTrailers(trailers, []string{"x-amz-checksum-sha256"})
	if err != nil {
		return nil
	}
	trailerSig := signStreamingTrailer(ctx, prev, canonical)
	trailers["x-amz-trailer-signature"] = trailerSig
	return buildChunkedPayload(payload, trailers, ctx, zeroSig)
}

func buildChunkedPayload(payload []byte, trailers map[string]string, ctx *sigv4Context, zeroSig string) []byte {
	if ctx == nil {
		header := chunkHeader(payload, "")
		return append([]byte(header+string(payload)+"\r\n"), []byte(trailerBlock(trailers))...)
	}
	chunkSig := signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, strings.Repeat("0", 64), sha256HexBytes(payload))
	header := chunkHeader(payload, chunkSig)
	if zeroSig == "" {
		zeroSig = signStreamingChunk(ctx.signingKey, ctx.amzDate, ctx.scope, strings.ToLower(chunkSig), emptySHA256Hex)
	}
	return append([]byte(header+string(payload)+"\r\n"), []byte("0;chunk-signature="+zeroSig+"\r\n"+trailerBlock(trailers))...)
}

func chunkHeader(payload []byte, sig string) string {
	line := strconv.FormatInt(int64(len(payload)), 16)
	if sig != "" {
		line += ";chunk-signature=" + sig
	}
	return line + "\r\n"
}

func trailerBlock(trailers map[string]string) string {
	if len(trailers) == 0 {
		return "\r\n"
	}
	var b strings.Builder
	for k, v := range trailers {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	return b.String()
}

func sha256HexBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func testSigv4Context() *sigv4Context {
	return &sigv4Context{
		signingKey:    []byte("test"),
		seedSignature: strings.Repeat("0", 64),
		amzDate:       "20240101T000000Z",
		scope:         "20240101/us-east-1/s3/aws4_request",
	}
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
