package s3

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

type streamingMode int

const (
	streamingNone streamingMode = iota
	streamingUnsigned
	streamingUnsignedTrailer
	streamingSigned
	streamingSignedTrailer
)

const (
	streamingPayloadSig = "AWS4-HMAC-SHA256-PAYLOAD"
	streamingTrailerSig = "AWS4-HMAC-SHA256-TRAILER"
)

const emptySHA256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func parseStreamingMode(header string) (streamingMode, error) {
	header = strings.TrimSpace(header)
	switch header {
	case "":
		return streamingNone, nil
	case "STREAMING-UNSIGNED-PAYLOAD":
		return streamingUnsigned, nil
	case "STREAMING-UNSIGNED-PAYLOAD-TRAILER":
		return streamingUnsignedTrailer, nil
	case "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
		return streamingSigned, nil
	case "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":
		return streamingSignedTrailer, nil
	}
	if strings.HasPrefix(header, "STREAMING-") {
		return streamingNone, errPayloadHashInvalid
	}
	return streamingNone, nil
}

func parseTrailerNames(header string) []string {
	if strings.TrimSpace(header) == "" {
		return nil
	}
	parts := strings.Split(header, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func hasAWSChunkedEncoding(header string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), "aws-chunked") {
			return true
		}
	}
	return false
}

type sigv4ContextKey struct{}

type sigv4Context struct {
	accessKey     string
	signingKey    []byte
	seedSignature string
	dateScope     string
	regionRaw     string
	amzDate       string
	scope         string
}

func sigv4ContextFromRequest(r *http.Request) (*sigv4Context, bool) {
	if r == nil {
		return nil, false
	}
	ctx, ok := r.Context().Value(sigv4ContextKey{}).(*sigv4Context)
	if !ok || ctx == nil {
		return nil, false
	}
	return ctx, true
}

type awsChunkedConfig struct {
	mode        streamingMode
	sigv4       *sigv4Context
	trailerKeys []string
	expectedLen int64
}

type awsChunkedReader struct {
	r           *bufio.Reader
	remaining   int64
	done        bool
	mode        streamingMode
	sigv4       *sigv4Context
	prevSig     string
	expectedSig string
	hasSig      bool
	chunkHasher hash.Hash
	checksum    *checksumValidator
	totalRead   int64
	expectedLen int64
	trailerKeys []string
}

func newAWSChunkedReader(reader io.Reader, cfg awsChunkedConfig) io.Reader {
	r := &awsChunkedReader{
		r:           bufio.NewReader(reader),
		mode:        cfg.mode,
		sigv4:       cfg.sigv4,
		expectedLen: cfg.expectedLen,
		trailerKeys: cfg.trailerKeys,
	}
	if cfg.sigv4 != nil {
		r.prevSig = strings.ToLower(cfg.sigv4.seedSignature)
	}
	if cfg.mode == streamingSigned || cfg.mode == streamingSignedTrailer {
		r.chunkHasher = sha256.New()
	}
	if len(cfg.trailerKeys) > 0 {
		if v, ok := newChecksumValidator(cfg.trailerKeys); ok {
			r.checksum = v
		}
	}
	return r
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		size, sig, hasSig, err := r.readChunkHeader()
		if err != nil {
			return 0, err
		}
		if size == 0 {
			if err := r.verifyChunkSignature(nil, sig, hasSig); err != nil {
				return 0, err
			}
			if err := r.readTrailers(); err != nil {
				return 0, err
			}
			if r.expectedLen > 0 && r.totalRead != r.expectedLen {
				return 0, errInvalidContentLength
			}
			r.done = true
			return 0, io.EOF
		}
		r.remaining = size
		r.expectedSig = sig
		r.hasSig = hasSig
		if r.chunkHasher != nil {
			r.chunkHasher.Reset()
		}
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := io.ReadFull(r.r, p)
	if n > 0 {
		r.totalRead += int64(n)
		if r.chunkHasher != nil {
			_, _ = r.chunkHasher.Write(p[:n])
		}
		if r.checksum != nil {
			r.checksum.Write(p[:n])
		}
	}
	if err != nil {
		return n, err
	}
	r.remaining -= int64(n)
	if r.remaining == 0 {
		if err := r.readCRLF(); err != nil {
			return 0, err
		}
		if err := r.verifyChunkSignature(r.chunkHasher, r.expectedSig, r.hasSig); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (r *awsChunkedReader) readChunkHeader() (int64, string, bool, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, "", false, err
	}
	sizePart := line
	var sig string
	if semi := strings.IndexByte(line, ';'); semi >= 0 {
		sizePart = line[:semi]
		ext := line[semi+1:]
		for _, field := range strings.Split(ext, ";") {
			field = strings.TrimSpace(field)
			if strings.HasPrefix(field, "chunk-signature=") {
				sig = strings.TrimPrefix(field, "chunk-signature=")
			}
		}
	}
	sizePart = strings.TrimSpace(sizePart)
	if sizePart == "" {
		return 0, "", false, errInvalidDigest
	}
	size, err := strconv.ParseInt(sizePart, 16, 64)
	if err != nil || size < 0 {
		return 0, "", false, errInvalidDigest
	}
	hasSig := sig != ""
	return size, sig, hasSig, nil
}

func (r *awsChunkedReader) verifyChunkSignature(hasher hash.Hash, sig string, hasSig bool) error {
	if r.mode != streamingSigned && r.mode != streamingSignedTrailer {
		return nil
	}
	if r.sigv4 == nil || r.sigv4.signingKey == nil || r.sigv4.seedSignature == "" {
		return errSignatureMismatch
	}
	if !hasSig {
		return errPayloadHashInvalid
	}
	chunkHashHex := emptySHA256Hex
	if hasher != nil {
		sum := hasher.Sum(nil)
		chunkHashHex = hex.EncodeToString(sum)
	}
	expected := signStreamingChunk(r.sigv4.signingKey, r.sigv4.amzDate, r.sigv4.scope, r.prevSig, chunkHashHex)
	if !hmac.Equal([]byte(strings.ToLower(sig)), []byte(strings.ToLower(expected))) {
		return errPayloadHashMismatch
	}
	r.prevSig = strings.ToLower(expected)
	return nil
}

func (r *awsChunkedReader) readTrailers() error {
	trailers := make(map[string]string)
	for {
		line, err := r.readLine()
		if err != nil {
			return err
		}
		if line == "" {
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return errInvalidDigest
		}
		name := strings.ToLower(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])
		if name == "" {
			return errInvalidDigest
		}
		trailers[name] = value
	}
	if r.mode == streamingSignedTrailer || r.mode == streamingUnsignedTrailer {
		if len(r.trailerKeys) == 0 {
			return errInvalidDigest
		}
	}
	if len(r.trailerKeys) > 0 {
		if err := r.verifyTrailers(trailers); err != nil {
			return err
		}
	}
	return nil
}

func (r *awsChunkedReader) verifyTrailers(trailers map[string]string) error {
	canonical, err := canonicalizeTrailers(trailers, r.trailerKeys)
	if err != nil {
		return errInvalidDigest
	}
	if r.checksum != nil {
		if err := r.checksum.Verify(trailers); err != nil {
			return err
		}
	}
	if r.mode == streamingSignedTrailer {
		sig, ok := trailers["x-amz-trailer-signature"]
		if !ok || sig == "" {
			return errPayloadHashInvalid
		}
		expected := signStreamingTrailer(r.sigv4, r.prevSig, canonical)
		if !hmac.Equal([]byte(strings.ToLower(sig)), []byte(strings.ToLower(expected))) {
			return errPayloadHashMismatch
		}
	}
	return nil
}

func (r *awsChunkedReader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\n") {
		return "", io.ErrUnexpectedEOF
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func (r *awsChunkedReader) readCRLF() error {
	b1, err := r.r.ReadByte()
	if err != nil {
		return err
	}
	b2, err := r.r.ReadByte()
	if err != nil {
		return err
	}
	if b1 != '\r' || b2 != '\n' {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func signStreamingChunk(signingKey []byte, amzDate, scope, prevSignature, chunkHashHex string) string {
	stringToSign := strings.Join([]string{
		streamingPayloadSig,
		amzDate,
		scope,
		prevSignature,
		emptySHA256Hex,
		chunkHashHex,
	}, "\n")
	return hmacSHA256Hex(signingKey, stringToSign)
}

func signStreamingTrailer(ctx *sigv4Context, prevSignature, canonicalTrailers string) string {
	if ctx == nil {
		return ""
	}
	hash := sha256.Sum256([]byte(canonicalTrailers))
	stringToSign := strings.Join([]string{
		streamingTrailerSig,
		ctx.amzDate,
		ctx.scope,
		prevSignature,
		hex.EncodeToString(hash[:]),
	}, "\n")
	return hmacSHA256Hex(ctx.signingKey, stringToSign)
}

func canonicalizeTrailers(trailers map[string]string, expected []string) (string, error) {
	if len(expected) == 0 {
		return "", nil
	}
	normalized := make(map[string]string, len(trailers))
	for k, v := range trailers {
		normalized[strings.ToLower(k)] = v
	}
	keys := make([]string, 0, len(expected))
	for _, name := range expected {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		if _, ok := normalized[name]; !ok {
			return "", errInvalidDigest
		}
		if name == "x-amz-trailer-signature" {
			continue
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, name := range keys {
		value := normalizeSpaces(normalized[name])
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

type checksumValidator struct {
	name      string
	hash      hash.Hash
	crc32Hash hash.Hash32
	crc64Sum  uint64
}

func newChecksumValidator(trailers []string) (*checksumValidator, bool) {
	var name string
	for _, t := range trailers {
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "x-amz-checksum-crc32", "x-amz-checksum-crc32c", "x-amz-checksum-crc64nvme", "x-amz-checksum-sha1", "x-amz-checksum-sha256":
			if name != "" && name != t {
				return nil, false
			}
			name = strings.ToLower(strings.TrimSpace(t))
		}
	}
	if name == "" {
		return nil, false
	}
	cv := &checksumValidator{name: name}
	switch name {
	case "x-amz-checksum-crc32":
		cv.crc32Hash = crc32.New(crc32.MakeTable(crc32.IEEE))
	case "x-amz-checksum-crc32c":
		cv.crc32Hash = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "x-amz-checksum-crc64nvme":
		cv.crc64Sum = 0xffffffffffffffff
	case "x-amz-checksum-sha1":
		cv.hash = sha1.New()
	case "x-amz-checksum-sha256":
		cv.hash = sha256.New()
	}
	return cv, true
}

func (c *checksumValidator) Write(p []byte) {
	if c == nil {
		return
	}
	switch c.name {
	case "x-amz-checksum-crc32", "x-amz-checksum-crc32c":
		_, _ = c.crc32Hash.Write(p)
	case "x-amz-checksum-crc64nvme":
		c.crc64Sum = crc64nvmeUpdate(c.crc64Sum, p)
	default:
		_, _ = c.hash.Write(p)
	}
}

func (c *checksumValidator) Verify(trailers map[string]string) error {
	if c == nil {
		return nil
	}
	value, ok := trailers[c.name]
	if !ok {
		return errBadDigest
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return errBadDigest
	}
	var expected []byte
	switch c.name {
	case "x-amz-checksum-crc32", "x-amz-checksum-crc32c":
		sum := c.crc32Hash.Sum32()
		expected = []byte{byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum)}
	case "x-amz-checksum-crc64nvme":
		sum := ^c.crc64Sum
		expected = []byte{
			byte(sum >> 56), byte(sum >> 48), byte(sum >> 40), byte(sum >> 32),
			byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum),
		}
	case "x-amz-checksum-sha1", "x-amz-checksum-sha256":
		expected = c.hash.Sum(nil)
	}
	if !hmac.Equal(decoded, expected) {
		return errBadDigest
	}
	return nil
}

const crc64NVMEPoly = 0x9A6C9329AC4BC9B5

var crc64NVMETable = makeCRC64Table(crc64NVMEPoly)

func makeCRC64Table(poly uint64) [256]uint64 {
	var table [256]uint64
	for i := 0; i < 256; i++ {
		crc := uint64(i)
		for j := 0; j < 8; j++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

func crc64nvmeUpdate(crc uint64, p []byte) uint64 {
	for _, b := range p {
		crc = crc64NVMETable[byte(crc)^b] ^ (crc >> 8)
	}
	return crc
}
