package s3

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
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
	// VirtualHosted enables bucket resolution from Host header (e.g. bucket.localhost).
	VirtualHosted bool
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID, ok := h.prepareRequest(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/meta/stats") {
		h.handleStats(r.Context(), w, requestID, r.URL.Path)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/" && h.hostBucket(r) == "" && r.URL.Query().Get("list-type") == "" {
		h.handleListBuckets(r.Context(), w, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		bucket, ok := h.parseBucketOnly(r)
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID, r.URL.Path)
			return
		}
		h.handleListV2(r.Context(), w, r, bucket, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("location") {
		bucket, ok := h.parseBucketOnly(r)
		if !ok {
			writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID)
			return
		}
		_ = bucket
		h.handleLocation(w, requestID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("uploads") {
		bucket, ok := h.parseBucketOnly(r)
		if !ok {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket", requestID, r.URL.Path)
			return
		}
		h.handleListMultipartUploads(r.Context(), w, r, bucket, requestID)
		return
	}
	bucket, key, ok := h.parseBucketKey(r)
	if !ok {
		if r.Method == http.MethodGet {
			if bucketOnly, ok := h.parseBucketOnly(r); ok {
				h.handleListV1(r.Context(), w, r, bucketOnly, requestID)
				return
			}
		}
		if r.Method == http.MethodDelete {
			if bucketOnly, ok := h.parseBucketOnly(r); ok {
				h.handleDeleteBucket(r.Context(), w, bucketOnly, requestID, r.URL.Path)
				return
			}
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid bucket/key", requestID, r.URL.Path)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if copySource := r.Header.Get("X-Amz-Copy-Source"); copySource != "" {
			h.handleCopyObject(r.Context(), w, r, bucket, key, copySource, requestID)
			return
		}
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
	case http.MethodDelete:
		h.handleDeleteObject(r.Context(), w, bucket, key, requestID, r.URL.Path)
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
	if bucket, ok := h.bucketFromRequest(r); ok {
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
		if h.isSigV2ListRequest(r) {
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

func (h *Handler) isSigV2ListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS ") {
		return false
	}
	if r.URL.Path == "/" {
		return h.hostBucket(r) == ""
	}
	if _, _, ok := h.parseBucketKey(r); ok {
		return false
	}
	if _, ok := h.parseBucketOnly(r); ok {
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
	if h.checkPreconditions(w, r, meta, requestID, r.URL.Path) {
		return
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

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

func (h *Handler) handleCopyObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, copySource, requestID string) {
	srcBucket, srcKey, ok := parseCopySource(copySource)
	if !ok {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "invalid copy source", requestID, r.URL.Path)
		return
	}
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, srcBucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	srcMeta, err := h.Meta.GetObjectMeta(ctx, srcBucket, srcKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchKey", "key not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if strings.EqualFold(srcMeta.State, "DAMAGED") {
		w.Header().Set("X-Error", "DamagedObject")
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "object damaged", requestID, r.URL.Path)
		return
	}
	reader, _, err := h.Engine.Get(ctx, srcMeta.VersionID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	defer func() { _ = reader.Close() }()

	_, result, err := h.Engine.PutObject(ctx, bucket, key, reader)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if result == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "copy result missing", requestID, r.URL.Path)
		return
	}
	if result != nil && result.ETag != "" {
		w.Header().Set("ETag", `"`+result.ETag+`"`)
	}
	resp := copyObjectResult{
		ETag:         `"` + result.ETag + `"`,
		LastModified: result.CommittedAt.UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleDeleteObject(ctx context.Context, w http.ResponseWriter, bucket, key, requestID, resource string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, resource)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, resource)
		return
	}
	_, err = h.Meta.DeleteObject(ctx, bucket, key)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDeleteBucket(ctx context.Context, w http.ResponseWriter, bucket, requestID, resource string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, resource)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, resource)
		return
	}
	hasObjects, err := h.Meta.BucketHasObjects(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	if hasObjects {
		writeErrorWithResource(w, http.StatusConflict, "BucketNotEmpty", "bucket not empty", requestID, resource)
		return
	}
	if err := h.Meta.DeleteBucket(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) hostBucket(r *http.Request) string {
	if h == nil || !h.VirtualHosted {
		return ""
	}
	host := r.Host
	if host == "" {
		return ""
	}
	if hst, _, err := net.SplitHostPort(host); err == nil {
		host = hst
	}
	host = strings.TrimSuffix(host, ".")
	host = strings.ToLower(host)
	if strings.Count(host, ".") == 0 {
		return ""
	}
	parts := strings.Split(host, ".")
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}
	return parts[0]
}

func (h *Handler) parseBucketKey(r *http.Request) (bucket string, key string, ok bool) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return "", "", false
	}
	hostBucket := h.hostBucket(r)
	if strings.Contains(path, "/") {
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			return "", "", false
		}
		if hostBucket != "" && parts[0] != hostBucket {
			return hostBucket, path, true
		}
		return parts[0], parts[1], true
	}
	if hostBucket != "" {
		return hostBucket, path, true
	}
	return "", "", false
}

func (h *Handler) parseBucketOnly(r *http.Request) (string, bool) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	hostBucket := h.hostBucket(r)
	if path == "" {
		if hostBucket != "" {
			return hostBucket, true
		}
		return "", false
	}
	if strings.Contains(path, "/") {
		return "", false
	}
	if hostBucket != "" && path != hostBucket {
		return "", false
	}
	return path, true
}

func (h *Handler) bucketFromRequest(r *http.Request) (string, bool) {
	if bucket, _, ok := h.parseBucketKey(r); ok {
		return bucket, true
	}
	if bucket, ok := h.parseBucketOnly(r); ok {
		return bucket, true
	}
	return "", false
}

func parseCopySource(raw string) (bucket, key string, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	raw = strings.TrimPrefix(raw, "/")
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", "", false
	}
	decoded = strings.TrimPrefix(decoded, "/")
	parts := strings.SplitN(decoded, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (h *Handler) checkPreconditions(w http.ResponseWriter, r *http.Request, meta *meta.ObjectMeta, requestID, resource string) bool {
	if meta == nil {
		return false
	}
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" {
		if !etagMatch(ifMatch, meta.ETag) {
			writeErrorWithResource(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag mismatch", requestID, resource)
			return true
		}
	}
	ifNone := r.Header.Get("If-None-Match")
	if ifNone != "" {
		if etagMatch(ifNone, meta.ETag) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}
	return false
}

func etagMatch(header, etag string) bool {
	if etag == "" {
		return false
	}
	header = strings.TrimSpace(header)
	if header == "*" {
		return true
	}
	etag = strings.ToLower(strings.Trim(etag, "\""))
	parts := strings.Split(header, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "W/")
		part = strings.Trim(part, "\"")
		if strings.ToLower(part) == etag {
			return true
		}
	}
	return false
}
