package s3

import (
	"context"
	"database/sql"
	"net/http"
)

func (h *Handler) handleGetBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	if bucket == "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidBucketName", "bucket required", requestID, r.URL.Path)
		return
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	policy, err := h.Meta.GetBucketPolicy(ctx, bucket)
	if err != nil {
		if err == sql.ErrNoRows {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucketPolicy", "bucket policy not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if policy == "" {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucketPolicy", "bucket policy not found", requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(policy))
}
