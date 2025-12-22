package s3

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
)

// Handler implements a minimal path-style S3 API (PUT/GET/HEAD).
type Handler struct {
	Engine *engine.Engine
	Meta   *meta.Store
	Auth   *AuthConfig
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID, ok := h.prepareRequest(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		h.handleListBuckets(r.Context(), w, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		bucket, ok := parseBucket(r.URL.Path)
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID, r.URL.Path)
			return
		}
		h.handleListV2(r.Context(), w, r, bucket, requestID)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/meta/stats") {
		h.handleStats(r.Context(), w, requestID, r.URL.Path)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("location") {
		bucket, ok := parseBucket(r.URL.Path)
		if !ok {
			writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID)
			return
		}
		_ = bucket
		h.handleLocation(w, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("uploads") {
		bucket, ok := parseBucket(r.URL.Path)
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID, r.URL.Path)
			return
		}
		h.handleListMultipartUploads(r.Context(), w, r, bucket, requestID)
		return
	}
	bucket, key, ok := parsePath(r.URL.Path)
	if !ok {
		if r.Method == http.MethodGet {
			if bucketOnly, ok := parseBucket(r.URL.Path); ok {
				h.handleListV1(r.Context(), w, r, bucketOnly, requestID)
				return
			}
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket/key", requestID, r.URL.Path)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			h.handleUploadPart(r.Context(), w, r, bucket, key, uploadID, requestID)
			return
		}
		h.handlePut(r.Context(), w, r, bucket, key, requestID)
	case http.MethodGet:
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			h.handleListParts(r.Context(), w, r, bucket, key, uploadID, requestID)
			return
		}
		h.handleGet(r.Context(), w, r, bucket, key, requestID, false)
	case http.MethodHead:
		h.handleGet(r.Context(), w, r, bucket, key, requestID, true)
	default:
		if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
			h.handleInitiateMultipart(r.Context(), w, bucket, key, requestID)
			return
		}
		if r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "" {
			h.handleCompleteMultipart(r.Context(), w, r, bucket, key, r.URL.Query().Get("uploadId"), requestID)
			return
		}
		if r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") != "" {
			h.handleAbortMultipart(r.Context(), w, r.URL.Query().Get("uploadId"), requestID)
			return
		}
		writeErrorWithResource(w, http.StatusMethodNotAllowed, "InvalidRequest", "unsupported method", requestID, r.URL.Path)
	}
}

func (h *Handler) prepareRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	requestID := newRequestID()
	w.Header().Set("x-amz-request-id", requestID)
	w.Header().Set("x-amz-id-2", hostID())
	if bucket, ok := parseBucket(r.URL.Path); ok {
		region := "us-east-1"
		if h.Auth != nil && h.Auth.Region != "" {
			region = h.Auth.Region
		}
		w.Header().Set("x-amz-bucket-region", region)
		_ = bucket
	}
	if h.Engine == nil || h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "storage not initialized", requestID, r.URL.Path)
		return requestID, false
	}
	if h.Auth == nil {
		return requestID, true
	}
	if err := h.Auth.VerifyRequest(r); err != nil {
		if isSigV2ListRequest(r) {
			return requestID, true
		}
		switch err {
		case errAccessDenied:
			writeErrorWithResource(w, http.StatusForbidden, "AccessDenied", "access denied", requestID, r.URL.Path)
		case errTimeSkew:
			writeErrorWithResource(w, http.StatusForbidden, "RequestTimeTooSkewed", "request time too skewed", requestID, r.URL.Path)
		default:
			writeErrorWithResource(w, http.StatusForbidden, "SignatureDoesNotMatch", "signature mismatch", requestID, r.URL.Path)
		}
		return requestID, false
	}
	return requestID, true
}

func isSigV2ListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS ") {
		return false
	}
	if r.URL.Path == "/" {
		return true
	}
	if _, _, ok := parsePath(r.URL.Path); ok {
		return false
	}
	if _, ok := parseBucket(r.URL.Path); ok {
		return true
	}
	return false
}

func (h *Handler) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	defer func() { _ = r.Body.Close() }()
	reader := io.Reader(r.Body)
	if hashHeader := r.Header.Get("X-Amz-Content-Sha256"); hashHeader != "" {
		expected, verify, err := parsePayloadHash(hashHeader)
		if err != nil {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid payload hash", requestID, r.URL.Path)
			return
		}
		if verify {
			reader = newPayloadHashReader(reader, expected)
		}
	}
	_, result, err := h.Engine.PutObject(ctx, bucket, key, reader)
	if err != nil {
		if errors.Is(err, errPayloadHashMismatch) {
			writeErrorWithResource(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload hash mismatch", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if result.ETag != "" {
		w.Header().Set("ETag", `"`+result.ETag+`"`)
	}
	w.Header().Set("Last-Modified", formatHTTPTime(result.CommittedAt))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string, headOnly bool) {
	meta, err := h.Meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchKey", "key not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if strings.EqualFold(meta.State, "DAMAGED") {
		w.Header().Set("X-Error", "DamagedObject")
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "object damaged", requestID, r.URL.Path)
		return
	}
	if meta.ETag != "" {
		w.Header().Set("ETag", `"`+meta.ETag+`"`)
	}
	if meta.LastModified != "" {
		if t, err := time.Parse(time.RFC3339Nano, meta.LastModified); err == nil {
			w.Header().Set("Last-Modified", formatHTTPTime(t))
		}
	}
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		ranges, ok := parseRanges(rangeHeader, meta.Size)
		if !ok || len(ranges) == 0 {
			w.Header().Set("Content-Range", "bytes */"+intToString(meta.Size))
			writeErrorWithResource(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "invalid range", requestID, r.URL.Path)
			return
		}
		if len(ranges) == 1 {
			start, length := ranges[0].start, ranges[0].length
			if headOnly {
				w.Header().Set("Content-Length", intToString(length))
				w.Header().Set("Content-Range", formatContentRange(start, length, meta.Size))
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			reader, _, err := h.Engine.GetRange(ctx, meta.VersionID, start, length)
			if err != nil {
				writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
				return
			}
			defer func() { _ = reader.Close() }()
			w.Header().Set("Content-Length", intToString(length))
			w.Header().Set("Content-Range", formatContentRange(start, length, meta.Size))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = ioCopy(w, reader)
			return
		}
		boundary := "seglake-" + requestID
		w.Header().Set("Content-Type", "multipart/byteranges; boundary="+boundary)
		w.WriteHeader(http.StatusPartialContent)
		if headOnly {
			return
		}
		for _, br := range ranges {
			start, length := br.start, br.length
			_, _ = io.WriteString(w, "--"+boundary+"\r\n")
			_, _ = io.WriteString(w, "Content-Type: application/octet-stream\r\n")
			_, _ = io.WriteString(w, "Content-Range: "+formatContentRange(start, length, meta.Size)+"\r\n\r\n")
			reader, _, err := h.Engine.GetRange(ctx, meta.VersionID, start, length)
			if err != nil {
				return
			}
			_, _ = ioCopy(w, reader)
			_ = reader.Close()
			_, _ = io.WriteString(w, "\r\n")
		}
		_, _ = io.WriteString(w, "--"+boundary+"--\r\n")
		return
	}
	if headOnly {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", intToString(meta.Size))
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	reader, _, err := h.Engine.Get(ctx, meta.VersionID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	defer func() { _ = reader.Close() }()
	if meta.Size >= 0 {
		w.Header().Set("Content-Length", intToString(meta.Size))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = ioCopy(w, reader)
}

func parsePath(path string) (bucket string, key string, ok bool) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", "", false
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func parseBucket(path string) (string, bool) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", false
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", false
	}
	return parts[0], true
}
