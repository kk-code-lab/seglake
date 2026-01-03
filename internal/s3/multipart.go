package s3

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

type initiateMultipartResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type completeMultipartRequest struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []completePartItem `xml:"Part"`
}

type completePartItem struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type listPartsResult struct {
	XMLName  xml.Name          `xml:"ListPartsResult"`
	Bucket   string            `xml:"Bucket"`
	Key      string            `xml:"Key"`
	UploadID string            `xml:"UploadId"`
	Parts    []listPartContent `xml:"Part"`
}

type listPartContent struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

const (
	minPartSize   int64 = 5 << 20
	maxPartSize   int64 = 5 << 30
	maxPartNumber       = 10000
)

func (h *Handler) handleInitiateMultipart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID, resource string) {
	uploadID := newRequestID() + newRequestID()
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if err := h.Engine.CommitMeta(ctx, func(tx *sql.Tx) error {
		if h.Meta == nil {
			return errors.New("meta store not configured")
		}
		return h.Meta.CreateMultipartUploadTx(ctx, tx, bucket, key, uploadID, contentType)
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	resp := initiateMultipartResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleUploadPart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	partNumber, ok := parsePartNumber(r.URL.Query().Get("partNumber"))
	if !ok {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid part number", requestID, r.URL.Path)
		return
	}
	if _, err := h.Meta.GetMultipartUpload(ctx, uploadID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	contentLength, hasLength, err := contentLengthFromRequest(r)
	if err != nil {
		switch err {
		case errMissingContentLength:
			writeErrorWithResource(w, http.StatusLengthRequired, "MissingContentLength", "missing content length", requestID, r.URL.Path)
		default:
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid content length", requestID, r.URL.Path)
		}
		return
	}
	reader := io.Reader(r.Body)
	reader, streamingMode, decodedLen, hasDecoded, reqErr := setupStreamingReader(r, reader)
	if reqErr != nil {
		writeErrorWithResource(w, reqErr.status, reqErr.code, reqErr.message, requestID, r.URL.Path)
		return
	}
	if streamingMode != streamingNone && hasDecoded && decodedLen > maxPartSize {
		writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
		return
	}
	if hasLength && contentLength > maxPartSize {
		writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
		return
	}
	if h.MaxObjectSize > 0 {
		switch {
		case streamingMode != streamingNone && hasDecoded && decodedLen > h.MaxObjectSize:
			writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
			return
		case hasLength && contentLength > h.MaxObjectSize:
			writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
			return
		}
	}
	if !hasLength {
		maxLimit := maxPartSize
		if h.MaxObjectSize > 0 && h.MaxObjectSize < maxLimit {
			maxLimit = h.MaxObjectSize
		}
		reader = newSizeLimitReader(reader, maxLimit)
	}
	expectedMD5, err := parseContentMD5(r.Header.Get("Content-MD5"))
	if err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid content-md5", requestID, r.URL.Path)
		return
	}
	if h.RequireContentMD5 && len(expectedMD5) == 0 {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "content-md5 required", requestID, r.URL.Path)
		return
	}
	payloadHash := ""
	verifyPayload := false
	if streamingMode == streamingNone {
		if hashHeader := r.Header.Get("X-Amz-Content-Sha256"); hashHeader != "" {
			expected, verify, err := parsePayloadHash(hashHeader)
			if err != nil {
				writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid payload hash", requestID, r.URL.Path)
				return
			}
			payloadHash = expected
			verifyPayload = verify
		}
	}
	if verifyPayload || len(expectedMD5) > 0 {
		reader = newValidatingReader(reader, payloadHash, verifyPayload, expectedMD5)
	}
	_, result, err := h.Engine.PutObjectWithCommit(ctx, "", "", "", reader, func(tx *sql.Tx, result *engine.PutResult, manifestPath string) error {
		if h.Meta == nil {
			return errors.New("meta store not configured")
		}
		return h.Meta.PutMultipartPartTx(ctx, tx, uploadID, partNumber, result.VersionID, result.ETag, result.Size)
	})
	if err != nil {
		switch {
		case errors.Is(err, errPayloadHashMismatch):
			writeErrorWithResource(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload hash mismatch", requestID, r.URL.Path)
			return
		case errors.Is(err, errInvalidDigest):
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid payload hash", requestID, r.URL.Path)
			return
		case errors.Is(err, errBadDigest):
			writeErrorWithResource(w, http.StatusBadRequest, "BadDigest", "content-md5 mismatch", requestID, r.URL.Path)
			return
		case errors.Is(err, errInvalidContentLength):
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid content length", requestID, r.URL.Path)
			return
		case errors.Is(err, errEntityTooLarge):
			writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.Header().Set("ETag", `"`+result.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleListParts(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	upload, err := h.Meta.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	parts, err := h.Meta.ListMultipartParts(ctx, uploadID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	out := make([]listPartContent, 0, len(parts))
	for _, part := range parts {
		out = append(out, listPartContent{
			PartNumber:   part.PartNumber,
			ETag:         `"` + part.ETag + `"`,
			Size:         part.Size,
			LastModified: formatLastModified(part.LastModified),
		})
	}
	resp := listPartsResult{
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadID: uploadID,
		Parts:    out,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleCompleteMultipart(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, requestID string) {
	if h.MPUCompleteLimiter != nil {
		if !h.MPUCompleteLimiter.Acquire() {
			writeErrorWithResource(w, http.StatusServiceUnavailable, "SlowDown", "too many inflight multipart completes", requestID, r.URL.Path)
			return
		}
		defer h.MPUCompleteLimiter.Release()
	}
	upload, err := h.Meta.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchUpload", "upload not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	var req completeMultipartRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid xml", requestID, r.URL.Path)
		return
	}
	if len(req.Parts) == 0 {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "no parts", requestID, r.URL.Path)
		return
	}
	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	parts, err := h.Meta.ListMultipartParts(ctx, uploadID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	partMap := make(map[int]meta.MultipartPart, len(parts))
	for _, p := range parts {
		partMap[p.PartNumber] = p
	}

	ordered := make([]meta.MultipartPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		part, ok := partMap[p.PartNumber]
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", "missing part", requestID, r.URL.Path)
			return
		}
		if normalizeETag(p.ETag) != part.ETag {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", "etag mismatch", requestID, r.URL.Path)
			return
		}
		ordered = append(ordered, part)
	}
	if err := validatePartSizes(ordered); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidPart", err.Error(), requestID, r.URL.Path)
		return
	}

	chunks := make([]manifest.ChunkRef, 0, len(ordered))
	totalSize := int64(0)
	chunkIndex := 0
	for _, part := range ordered {
		partManifest, err := h.Engine.GetManifest(ctx, part.VersionID)
		if err != nil {
			writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
			return
		}
		if partManifest.Size != 0 && part.Size != 0 && partManifest.Size != part.Size {
			writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "part size mismatch", requestID, r.URL.Path)
			return
		}
		for _, ch := range partManifest.Chunks {
			ch.Index = chunkIndex
			chunkIndex++
			chunks = append(chunks, ch)
		}
		if partManifest.Size > 0 {
			totalSize += partManifest.Size
		} else {
			totalSize += part.Size
		}
	}

	multiETag := multipartETag(ordered)
	_, result, err := h.Engine.PutManifestWithCommit(ctx, upload.Bucket, upload.Key, upload.ContentType, totalSize, multiETag, chunks, func(tx *sql.Tx, result *engine.PutResult, manifestPath string) error {
		if h.Meta == nil {
			return errors.New("meta store not configured")
		}
		if err := h.Meta.CompleteMultipartUploadTx(ctx, tx, uploadID); err != nil {
			return err
		}
		return h.Meta.RecordMPUCompleteTx(ctx, tx, upload.Bucket, upload.Key, result.VersionID, multiETag, result.Size)
	})
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	resp := completeMultipartResult{
		Bucket: upload.Bucket,
		Key:    upload.Key,
		ETag:   `"` + multiETag + `"`,
	}
	w.Header().Set("ETag", `"`+multiETag+`"`)
	if result != nil {
		_ = result
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleAbortMultipart(ctx context.Context, w http.ResponseWriter, uploadID string, requestID, resource string) {
	if err := h.Engine.CommitMeta(ctx, func(tx *sql.Tx) error {
		if h.Meta == nil {
			return errors.New("meta store not configured")
		}
		return h.Meta.AbortMultipartUploadTx(ctx, tx, uploadID)
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parsePartNumber(raw string) (int, bool) {
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 || v > maxPartNumber {
		return 0, false
	}
	return v, true
}

func normalizeETag(etag string) string {
	etag = strings.TrimSpace(etag)
	etag = strings.TrimPrefix(etag, "\"")
	etag = strings.TrimSuffix(etag, "\"")
	return etag
}

func validatePartSizes(parts []meta.MultipartPart) error {
	if len(parts) == 0 {
		return errors.New("no parts")
	}
	for i := 0; i < len(parts)-1; i++ {
		if parts[i].Size < minPartSize {
			return errors.New("part too small")
		}
	}
	for i := 0; i < len(parts); i++ {
		if parts[i].Size > maxPartSize {
			return errors.New("part too large")
		}
	}
	return nil
}

func multipartETag(parts []meta.MultipartPart) string {
	h := md5.New()
	for _, part := range parts {
		sum, err := hex.DecodeString(part.ETag)
		if err != nil {
			continue
		}
		h.Write(sum)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}
