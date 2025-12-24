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
	Engine  *engine.Engine
	Meta    *meta.Store
	Auth    *AuthConfig
	Metrics *Metrics
	// AuthLimiter rate-limits failed auth attempts.
	AuthLimiter *AuthLimiter
	// InflightLimiter limits concurrent requests per access key.
	InflightLimiter *InflightLimiter
	// VirtualHosted enables bucket resolution from Host header (e.g. bucket.localhost).
	VirtualHosted bool
	// TrustedProxies contains CIDR ranges for trusted proxy IPs; used for X-Forwarded-For.
	TrustedProxies []string
	// MaxObjectSize enforces an optional max object size (0 = unlimited).
	MaxObjectSize int64
	// CORSAllowOrigins contains allowed origins for CORS (empty = "*").
	CORSAllowOrigins []string
	// CORSAllowMethods contains allowed methods for CORS (empty = default set).
	CORSAllowMethods []string
	// CORSAllowHeaders contains allowed headers for CORS (empty = default set).
	CORSAllowHeaders []string
	// CORSMaxAge is the Access-Control-Max-Age value in seconds (0 = default).
	CORSMaxAge int
	// RequireContentMD5 enforces Content-MD5 on PUT and UploadPart.
	RequireContentMD5 bool
	// ReplayCacheTTL enables replay protection within the TTL window (0 disables).
	ReplayCacheTTL time.Duration
	replayCache    *replayCache
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := h.opForRequest(r)
	bytesIn := int64(0)
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &countingReadCloser{reader: r.Body, counter: &bytesIn}
	}
	bucket, key, hasBucketKey := h.parseBucketKey(r)
	bucketOnly, hasBucketOnly := h.parseBucketOnly(r)
	bucketName := ""
	keyName := ""
	if hasBucketKey {
		bucketName = bucket
		keyName = key
	} else if hasBucketOnly {
		bucketName = bucketOnly
	}
	mw := &metricsWriter{ResponseWriter: w, status: http.StatusOK}
	h.applyCORSHeaders(mw, r)
	start := time.Now()
	accessKey := extractAccessKey(r)
	if r.Method == http.MethodOptions {
		requestID := newRequestID()
		h.handleOptions(mw, r, requestID)
		return
	}
	requestID, ok := h.prepareRequest(mw, r)
	if !ok {
		return
	}
	if h.InflightLimiter != nil && accessKey != "" {
		limit := int64(0)
		if h.Meta != nil {
			if key, err := h.Meta.GetAPIKey(r.Context(), accessKey); err == nil && key.InflightLimit > 0 {
				limit = key.InflightLimit
			}
		}
		if !h.InflightLimiter.AcquireWithLimit(accessKey, limit) {
			writeErrorWithResource(mw, http.StatusServiceUnavailable, "SlowDown", "too many inflight requests", requestID, r.URL.Path)
			return
		}
		defer h.InflightLimiter.Release(accessKey)
	}
	if h.Metrics != nil {
		h.Metrics.InflightInc(op)
		defer h.Metrics.InflightDec(op)
	}
	defer func() {
		if h.Metrics != nil {
			h.Metrics.AddBytesIn(bytesIn)
			h.Metrics.AddBytesOut(mw.bytes)
			h.Metrics.Record(op, mw.status, time.Since(start), bucketName, keyName)
		}
	}()
	if h.handleMetaAndReplication(r.Context(), mw, r, requestID) {
		return
	}
	hostBucket := h.hostBucket(r)
	if h.handleBucketLevelRequests(r.Context(), mw, r, requestID, bucketOnly, hasBucketOnly, hostBucket) {
		return
	}
	if !hasBucketKey {
		if r.Method == http.MethodPut {
			if hasBucketOnly {
				h.handleCreateBucket(r.Context(), mw, bucketOnly, requestID, r.URL.Path)
				return
			}
		}
		if r.Method == http.MethodGet {
			if isListV1Request(r, hasBucketOnly) {
				h.handleListV1(r.Context(), mw, r, bucketOnly, requestID)
				return
			}
		}
		if r.Method == http.MethodDelete {
			if hasBucketOnly {
				h.handleDeleteBucket(r.Context(), mw, bucketOnly, requestID, r.URL.Path)
				return
			}
		}
		if hasBucketOnly {
			writeErrorWithResource(mw, http.StatusMethodNotAllowed, "MethodNotAllowed", "", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(mw, http.StatusBadRequest, "InvalidURI", "", requestID, r.URL.Path)
		return
	}
	h.handleObjectRequests(r.Context(), mw, r, requestID, bucket, key)
}

func (h *Handler) handleMetaAndReplication(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) bool {
	type route struct {
		method  string
		prefix  string
		handler func(context.Context, http.ResponseWriter, *http.Request, string)
	}
	routes := []route{
		{
			method: http.MethodGet,
			prefix: "/v1/meta/stats",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleStats(ctx, w, requestID, r.URL.Path)
			},
		},
		{
			method: http.MethodGet,
			prefix: "/v1/replication/oplog",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleOplog(ctx, w, r, requestID)
			},
		},
		{
			method: http.MethodGet,
			prefix: "/v1/replication/snapshot",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleReplicationSnapshot(ctx, w, r, requestID)
			},
		},
		{
			method: http.MethodGet,
			prefix: "/v1/replication/manifest",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleReplicationManifest(ctx, w, r, requestID)
			},
		},
		{
			method: http.MethodGet,
			prefix: "/v1/replication/chunk",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleReplicationChunk(ctx, w, r, requestID)
			},
		},
		{
			method: http.MethodPost,
			prefix: "/v1/replication/oplog",
			handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
				h.handleOplogApply(ctx, w, r, requestID)
			},
		},
	}
	for _, route := range routes {
		if r.Method != route.method {
			continue
		}
		if strings.HasPrefix(r.URL.Path, route.prefix) {
			route.handler(ctx, w, r, requestID)
			return true
		}
	}
	return false
}

type bucketListKind int

const (
	bucketListNone bucketListKind = iota
	bucketListBuckets
	bucketListV2
	bucketListLocation
	bucketListUploads
)

func bucketListKindForRequest(r *http.Request, hostBucket string, hasBucketOnly bool) bucketListKind {
	if r.Method != http.MethodGet {
		return bucketListNone
	}
	if r.URL.Path == "/" && hostBucket == "" && r.URL.Query().Get("list-type") == "" {
		return bucketListBuckets
	}
	if r.URL.Query().Get("list-type") == "2" {
		if !hasBucketOnly {
			return bucketListNone
		}
		return bucketListV2
	}
	if r.URL.Query().Has("location") {
		if !hasBucketOnly {
			return bucketListNone
		}
		return bucketListLocation
	}
	if r.URL.Query().Has("uploads") {
		if !hasBucketOnly {
			return bucketListNone
		}
		return bucketListUploads
	}
	return bucketListNone
}

func isListV1Request(r *http.Request, hasBucketOnly bool) bool {
	if r.Method != http.MethodGet || !hasBucketOnly {
		return false
	}
	if r.URL.Query().Get("list-type") == "2" {
		return false
	}
	if r.URL.Query().Has("location") {
		return false
	}
	if r.URL.Query().Has("uploads") {
		return false
	}
	return true
}

func (h *Handler) handleBucketLevelRequests(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID, bucketOnly string, hasBucketOnly bool, hostBucket string) bool {
	switch bucketListKindForRequest(r, hostBucket, hasBucketOnly) {
	case bucketListBuckets:
		h.handleListBuckets(ctx, w, requestID)
		return true
	case bucketListV2:
		h.handleListV2(ctx, w, r, bucketOnly, requestID)
		return true
	case bucketListLocation:
		_ = bucketOnly
		h.handleLocation(w, requestID)
		return true
	case bucketListUploads:
		h.handleListMultipartUploads(ctx, w, r, bucketOnly, requestID)
		return true
	}
	return false
}

func (h *Handler) handleObjectRequests(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	type objectRoute struct {
		method  string
		match   func(*http.Request) bool
		handler func()
	}
	routes := []objectRoute{
		{
			method: http.MethodPut,
			match: func(r *http.Request) bool {
				return r.Header.Get("X-Amz-Copy-Source") != ""
			},
			handler: func() {
				h.handleCopyObject(ctx, w, r, bucket, key, r.Header.Get("X-Amz-Copy-Source"), requestID)
			},
		},
		{
			method: http.MethodPut,
			match: func(r *http.Request) bool {
				return r.URL.Query().Get("uploadId") != ""
			},
			handler: func() {
				h.handleUploadPart(ctx, w, r, bucket, key, r.URL.Query().Get("uploadId"), requestID)
			},
		},
		{
			method: http.MethodPut,
			match:  func(*http.Request) bool { return true },
			handler: func() {
				h.handlePut(ctx, w, r, bucket, key, requestID)
			},
		},
		{
			method: http.MethodGet,
			match: func(r *http.Request) bool {
				return r.URL.Query().Get("uploadId") != ""
			},
			handler: func() {
				h.handleListParts(ctx, w, r, bucket, key, r.URL.Query().Get("uploadId"), requestID)
			},
		},
		{
			method: http.MethodGet,
			match:  func(*http.Request) bool { return true },
			handler: func() {
				h.handleGet(ctx, w, r, bucket, key, requestID, false)
			},
		},
		{
			method: http.MethodHead,
			match:  func(*http.Request) bool { return true },
			handler: func() {
				h.handleGet(ctx, w, r, bucket, key, requestID, true)
			},
		},
		{
			method: http.MethodDelete,
			match: func(r *http.Request) bool {
				return r.URL.Query().Get("uploadId") != ""
			},
			handler: func() {
				h.handleAbortMultipart(ctx, w, r.URL.Query().Get("uploadId"), requestID, r.URL.Path)
			},
		},
		{
			method: http.MethodDelete,
			match:  func(*http.Request) bool { return true },
			handler: func() {
				h.handleDeleteObject(ctx, w, r, bucket, key, requestID, r.URL.Path)
			},
		},
		{
			method: http.MethodPost,
			match: func(r *http.Request) bool {
				return r.URL.Query().Has("uploads")
			},
			handler: func() {
				h.handleInitiateMultipart(ctx, w, r, bucket, key, requestID, r.URL.Path)
			},
		},
		{
			method: http.MethodPost,
			match: func(r *http.Request) bool {
				return r.URL.Query().Get("uploadId") != ""
			},
			handler: func() {
				h.handleCompleteMultipart(ctx, w, r, bucket, key, r.URL.Query().Get("uploadId"), requestID)
			},
		},
	}
	for _, route := range routes {
		if r.Method != route.method {
			continue
		}
		if route.match(r) {
			route.handler()
			return
		}
	}
	writeErrorWithResource(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "", requestID, r.URL.Path)
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
		if h.AuthLimiter != nil {
			ip := clientIP(r.RemoteAddr)
			key := extractAccessKey(r)
			if !h.AuthLimiter.Allow(ip, key) {
				writeErrorWithResource(w, http.StatusServiceUnavailable, "SlowDown", "too many auth failures", requestID, r.URL.Path)
				return requestID, false
			}
			h.AuthLimiter.ObserveFailure(ip, key)
		}
		switch err {
		case errAccessDenied:
			writeErrorWithResource(w, http.StatusForbidden, "AccessDenied", "access denied", requestID, r.URL.Path)
		case errTimeSkew:
			writeErrorWithResource(w, http.StatusForbidden, "RequestTimeTooSkewed", "request time too skewed", requestID, r.URL.Path)
		case errAuthMalformed:
			writeErrorWithResource(w, http.StatusBadRequest, "AuthorizationHeaderMalformed", "authorization header malformed", requestID, r.URL.Path)
		default:
			writeErrorWithResource(w, http.StatusForbidden, "SignatureDoesNotMatch", "signature mismatch", requestID, r.URL.Path)
		}
		return requestID, false
	}
	if h.ReplayCacheTTL > 0 {
		if h.replayCache == nil {
			h.replayCache = newReplayCache(h.ReplayCacheTTL)
		}
		key := replayKey(r)
		if !h.replayCache.allow(key, time.Now().UTC()) {
			writeErrorWithResource(w, http.StatusForbidden, "AccessDenied", "replay detected", requestID, r.URL.Path)
			return requestID, false
		}
	}
	if err := h.authorizeRequest(r.Context(), r); err != nil {
		writeErrorWithResource(w, http.StatusForbidden, "AccessDenied", "access denied", requestID, r.URL.Path)
		return requestID, false
	}
	return requestID, true
}

func (h *Handler) authorizeRequest(ctx context.Context, r *http.Request) error {
	if h == nil || h.Meta == nil || r == nil {
		return nil
	}
	accessKey := extractAccessKey(r)
	if accessKey == "" {
		return nil
	}
	hasKeys, err := h.Meta.HasAPIKeys(ctx)
	if err != nil {
		return err
	}
	key, err := h.Meta.GetAPIKey(ctx, accessKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if hasKeys {
				return errAccessDenied
			}
			return nil
		}
		return err
	}
	if !key.Enabled {
		return errAccessDenied
	}
	policy := strings.TrimSpace(key.Policy)
	action := policyActionForRequest(h.opForRequest(r))
	bucket := ""
	keyName := ""
	if bkt, ok := h.bucketFromRequest(r); ok {
		bucket = bkt
	}
	if _, keyParsed, ok := h.parseBucketKey(r); ok {
		keyName = keyParsed
	}
	if bucket != "" {
		allowed, err := h.Meta.IsBucketAllowed(ctx, accessKey, bucket)
		if err != nil {
			return err
		}
		if !allowed {
			return errAccessDenied
		}
	}
	if action != "" {
		pol, err := ParsePolicy(policy)
		if err != nil {
			return errAccessDenied
		}
		targetBucket := bucket
		if targetBucket == "" {
			targetBucket = "*"
		}
		reqCtx := h.policyContextFromRequest(r)
		identityAllowed, identityDenied := pol.DecisionWithContext(action, targetBucket, keyName, reqCtx)
		if identityDenied {
			return errAccessDenied
		}
		bucketAllowed := false
		bucketDenied := false
		if bucket != "" {
			if bucketPolicy, err := h.Meta.GetBucketPolicy(ctx, bucket); err == nil && bucketPolicy != "" {
				if bpol, err := ParsePolicy(bucketPolicy); err == nil {
					bucketAllowed, bucketDenied = bpol.DecisionWithContext(action, bucket, keyName, reqCtx)
				} else {
					return errAccessDenied
				}
			}
		}
		if bucketDenied {
			return errAccessDenied
		}
		if !identityAllowed && !bucketAllowed {
			return errAccessDenied
		}
	}
	_ = h.Meta.RecordAPIKeyUse(ctx, accessKey)
	return nil
}

func (h *Handler) policyContextFromRequest(r *http.Request) *PolicyContext {
	if r == nil {
		return &PolicyContext{Now: time.Now().UTC()}
	}
	headers := make(map[string]string)
	for k, values := range r.Header {
		if len(values) == 0 {
			continue
		}
		headers[strings.ToLower(k)] = values[0]
	}
	ip := clientIP(r.RemoteAddr)
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" && h.isTrustedProxy(r.RemoteAddr) {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			ip = strings.TrimSpace(parts[0])
		}
	}
	return &PolicyContext{
		Now:      time.Now().UTC(),
		SourceIP: ip,
		Headers:  headers,
	}
}

func (h *Handler) isTrustedProxy(remoteAddr string) bool {
	if h == nil {
		return false
	}
	if len(h.TrustedProxies) == 0 {
		return false
	}
	ip := net.ParseIP(clientIP(remoteAddr))
	if ip == nil {
		return false
	}
	for _, cidr := range h.TrustedProxies {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (h *Handler) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	defer func() { _ = r.Body.Close() }()
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
	if h.MaxObjectSize > 0 && hasLength && contentLength > h.MaxObjectSize {
		writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
		return
	}
	reader := io.Reader(r.Body)
	if h.MaxObjectSize > 0 && !hasLength {
		reader = newSizeLimitReader(reader, h.MaxObjectSize)
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
	if hashHeader := r.Header.Get("X-Amz-Content-Sha256"); hashHeader != "" {
		expected, verify, err := parsePayloadHash(hashHeader)
		if err != nil {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidDigest", "invalid payload hash", requestID, r.URL.Path)
			return
		}
		payloadHash = expected
		verifyPayload = verify
	}
	if verifyPayload || len(expectedMD5) > 0 {
		reader = newValidatingReader(reader, payloadHash, verifyPayload, expectedMD5)
	}
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	_, result, err := h.Engine.PutObject(ctx, bucket, key, contentType, reader)
	if err != nil {
		switch {
		case errors.Is(err, errPayloadHashMismatch):
			writeErrorWithResource(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload hash mismatch", requestID, r.URL.Path)
			return
		case errors.Is(err, errBadDigest):
			writeErrorWithResource(w, http.StatusBadRequest, "BadDigest", "content-md5 mismatch", requestID, r.URL.Path)
			return
		case errors.Is(err, errEntityTooLarge):
			writeErrorWithResource(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "entity too large", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if result.ETag != "" {
		w.Header().Set("ETag", `"`+result.ETag+`"`)
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	w.Header().Set("Last-Modified", formatHTTPTime(result.CommittedAt))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string, headOnly bool) {
	versionID := r.URL.Query().Get("versionId")
	var (
		meta *meta.ObjectMeta
		err  error
	)
	if versionID != "" {
		meta, err = h.Meta.GetObjectVersion(ctx, bucket, key, versionID)
	} else {
		meta, err = h.Meta.GetObjectMeta(ctx, bucket, key)
	}
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
	if meta.VersionID != "" {
		w.Header().Set("x-amz-version-id", meta.VersionID)
	}
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
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

	contentType := srcMeta.ContentType
	_, result, err := h.Engine.PutObject(ctx, bucket, key, contentType, reader)
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
	if result != nil && result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	resp := copyObjectResult{
		ETag:         `"` + result.ETag + `"`,
		LastModified: result.CommittedAt.UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleDeleteObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID, resource string) {
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
	versionID := r.URL.Query().Get("versionId")
	if versionID != "" {
		deleted, err := h.Meta.DeleteObjectVersion(ctx, bucket, key, versionID)
		if err != nil {
			writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
			return
		}
		if !deleted {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchVersion", "version not found", requestID, resource)
			return
		}
		w.Header().Set("x-amz-version-id", versionID)
	} else {
		if _, err := h.Meta.DeleteObject(ctx, bucket, key); err != nil {
			writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
			return
		}
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

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request, requestID string) {
	if w.Header().Get("x-amz-request-id") == "" {
		w.Header().Set("x-amz-request-id", requestID)
	}
	if w.Header().Get("x-amz-id-2") == "" {
		w.Header().Set("x-amz-id-2", hostID())
	}
	h.applyCORSPreflightHeaders(w, r)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) applyCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	allowOrigin := h.corsAllowOrigin(origin)
	if allowOrigin == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
}

func (h *Handler) applyCORSPreflightHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin != "" {
		allowOrigin := h.corsAllowOrigin(origin)
		if allowOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
		}
	}
	w.Header().Set("Access-Control-Allow-Methods", h.corsAllowMethods())
	w.Header().Set("Access-Control-Allow-Headers", h.corsAllowHeaders())
	w.Header().Set("Access-Control-Max-Age", intToString(int64(h.corsMaxAge())))
}

func (h *Handler) corsAllowOrigin(origin string) string {
	origins := h.CORSAllowOrigins
	if len(origins) == 0 {
		return "*"
	}
	for _, allowed := range origins {
		if allowed == "*" {
			return "*"
		}
		if strings.EqualFold(allowed, origin) {
			return origin
		}
	}
	return ""
}

func (h *Handler) corsAllowMethods() string {
	if len(h.CORSAllowMethods) > 0 {
		return strings.Join(h.CORSAllowMethods, ", ")
	}
	return "GET, PUT, HEAD, DELETE"
}

func (h *Handler) corsAllowHeaders() string {
	if len(h.CORSAllowHeaders) > 0 {
		return strings.Join(h.CORSAllowHeaders, ", ")
	}
	return "authorization, content-md5, content-type, x-amz-date, x-amz-content-sha256"
}

func (h *Handler) corsMaxAge() int {
	if h.CORSMaxAge > 0 {
		return h.CORSMaxAge
	}
	return 86400
}

func (h *Handler) handleCreateBucket(ctx context.Context, w http.ResponseWriter, bucket, requestID, resource string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, resource)
		return
	}
	if err := h.Meta.CreateBucket(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	w.WriteHeader(http.StatusOK)
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
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	}
	host = strings.TrimSuffix(host, ".")
	host = strings.ToLower(host)
	if host == "localhost" || net.ParseIP(host) != nil {
		return ""
	}
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
	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/")
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

func (h *Handler) opForRequest(r *http.Request) string {
	if r == nil {
		return "unknown"
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/meta/stats") {
		return "meta_stats"
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/replication/oplog") {
		return "repl_oplog"
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/replication/snapshot") {
		return "repl_snapshot"
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/replication/manifest") {
		return "repl_manifest"
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/replication/chunk") {
		return "repl_chunk"
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/replication/oplog") {
		return "repl_oplog_apply"
	}
	if r.Method == http.MethodGet && r.URL.Path == "/" && h.hostBucket(r) == "" && r.URL.Query().Get("list-type") == "" {
		return "list_buckets"
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		return "list_v2"
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("uploads") {
		return "mpu_list_uploads"
	}
	if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
		return "mpu_initiate"
	}
	if r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "" {
		return "mpu_upload_part"
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("uploadId") != "" {
		return "mpu_list_parts"
	}
	if r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "" {
		return "mpu_complete"
	}
	if r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") != "" {
		return "mpu_abort"
	}
	if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
		return "copy"
	}
	if r.Method == http.MethodGet {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" && !strings.Contains(path, "/") {
			return "list_v1"
		}
		return "get"
	}
	if r.Method == http.MethodHead {
		return "head"
	}
	if r.Method == http.MethodPut {
		return "put"
	}
	if r.Method == http.MethodDelete {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" && !strings.Contains(path, "/") {
			return "delete_bucket"
		}
		return "delete"
	}
	return "other"
}

type metricsWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *metricsWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

type countingReadCloser struct {
	reader  io.ReadCloser
	counter *int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 {
		*c.counter += int64(n)
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
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
	var lastModified time.Time
	if meta.LastModified != "" {
		if t, err := time.Parse(time.RFC3339Nano, meta.LastModified); err == nil {
			lastModified = t
		}
	}
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" {
		if !etagMatch(ifMatch, meta.ETag) {
			writeErrorWithResource(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag mismatch", requestID, resource)
			return true
		}
	}
	ifUnmodified := r.Header.Get("If-Unmodified-Since")
	if ifUnmodified != "" && !lastModified.IsZero() {
		if since, err := parseHTTPTime(ifUnmodified); err == nil {
			if lastModified.After(since) {
				writeErrorWithResource(w, http.StatusPreconditionFailed, "PreconditionFailed", "precondition failed", requestID, resource)
				return true
			}
		}
	}
	ifNone := r.Header.Get("If-None-Match")
	if ifNone != "" {
		if etagMatch(ifNone, meta.ETag) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
		return false
	}
	ifModified := r.Header.Get("If-Modified-Since")
	if ifModified != "" && !lastModified.IsZero() {
		if since, err := parseHTTPTime(ifModified); err == nil {
			if !lastModified.After(since) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
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
