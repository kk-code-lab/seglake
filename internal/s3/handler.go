package s3

import (
	"context"
	"database/sql"
	"errors"
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
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		bucket, ok := parseBucket(r.URL.Path)
		if !ok {
			writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID)
			return
		}
		h.handleListV2(r.Context(), w, r, bucket, requestID)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/meta/stats") {
		h.handleStats(r.Context(), w, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("uploads") {
		bucket, ok := parseBucket(r.URL.Path)
		if !ok {
			writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID)
			return
		}
		h.handleListMultipartUploads(r.Context(), w, r, bucket, requestID)
		return
	}
	bucket, key, ok := parsePath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket/key", requestID)
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
			h.handleListParts(r.Context(), w, bucket, key, uploadID, requestID)
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
		writeError(w, http.StatusMethodNotAllowed, "InvalidRequest", "unsupported method", requestID)
	}
}

func (h *Handler) prepareRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	requestID := newRequestID()
	w.Header().Set("x-amz-request-id", requestID)
	w.Header().Set("x-amz-id-2", hostID())
	if h.Engine == nil || h.Meta == nil {
		writeError(w, http.StatusInternalServerError, "InternalError", "storage not initialized", requestID)
		return requestID, false
	}
	if h.Auth == nil {
		return requestID, true
	}
	if err := h.Auth.VerifyRequest(r); err != nil {
		switch err {
		case errAccessDenied:
			writeError(w, http.StatusForbidden, "AccessDenied", "access denied", requestID)
		case errTimeSkew:
			writeError(w, http.StatusForbidden, "RequestTimeTooSkewed", "request time too skewed", requestID)
		default:
			writeError(w, http.StatusForbidden, "SignatureDoesNotMatch", "signature mismatch", requestID)
		}
		return requestID, false
	}
	return requestID, true
}

func (h *Handler) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	defer func() { _ = r.Body.Close() }()
	_, result, err := h.Engine.PutObject(ctx, bucket, key, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	if result.ETag != "" {
		w.Header().Set("ETag", `"`+result.ETag+`"`)
	}
	w.Header().Set("Last-Modified", result.CommittedAt.Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string, headOnly bool) {
	meta, err := h.Meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NoSuchKey", "key not found", requestID)
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	if strings.EqualFold(meta.State, "DAMAGED") {
		w.Header().Set("X-Error", "DamagedObject")
		writeError(w, http.StatusInternalServerError, "InternalError", "object damaged", requestID)
		return
	}
	if meta.ETag != "" {
		w.Header().Set("ETag", `"`+meta.ETag+`"`)
	}
	if meta.LastModified != "" {
		if t, err := time.Parse(time.RFC3339Nano, meta.LastModified); err == nil {
			w.Header().Set("Last-Modified", t.UTC().Format(time.RFC1123))
		}
	}
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		start, length, ok := parseRange(rangeHeader, meta.Size)
		if !ok {
			w.Header().Set("Content-Range", "bytes */"+intToString(meta.Size))
			writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "invalid range", requestID)
			return
		}
		w.Header().Set("Content-Length", intToString(length))
		w.Header().Set("Content-Range", formatContentRange(start, length, meta.Size))
		if headOnly {
			w.WriteHeader(http.StatusPartialContent)
			return
		}
		reader, _, err := h.Engine.GetRange(ctx, meta.VersionID, start, length)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
			return
		}
		defer func() { _ = reader.Close() }()
		w.WriteHeader(http.StatusPartialContent)
		_, _ = ioCopy(w, reader)
		return
	}
	if meta.Size >= 0 {
		w.Header().Set("Content-Length", intToString(meta.Size))
	}
	if headOnly {
		w.WriteHeader(http.StatusOK)
		return
	}
	reader, _, err := h.Engine.Get(ctx, meta.VersionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	defer func() { _ = reader.Close() }()
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
