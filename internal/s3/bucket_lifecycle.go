package s3

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/kk-code-lab/seglake/internal/lifecycle"
	"github.com/kk-code-lab/seglake/internal/meta"
)

func (h *Handler) handleGetBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	cfg, err := h.Meta.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchLifecycleConfiguration", "bucket lifecycle configuration not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(cfg.XML))
}

func (h *Handler) handlePutBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	parsed, err := lifecycle.ParseXML(r.Body)
	if err != nil {
		if errors.Is(err, lifecycle.ErrUnsupportedFeature) {
			writeErrorWithResource(w, http.StatusNotImplemented, "NotImplemented", err.Error(), requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", err.Error(), requestID, r.URL.Path)
		return
	}
	if err := h.Meta.SetBucketLifecycle(ctx, meta.BucketLifecycleConfig{
		Bucket:            bucket,
		XML:               parsed.XMLText,
		NormalizedJSON:    parsed.NormalizedJSON,
		ConfigFingerprint: parsed.Fingerprint,
		RuleIDs:           parsed.RuleIDsJSON,
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	if err := h.Meta.DeleteBucketLifecycle(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ensureBucketLifecycleTarget(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) bool {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return false
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return false
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return false
	}
	return true
}
