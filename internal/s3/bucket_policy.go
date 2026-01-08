package s3

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"strings"
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
		if errors.Is(err, sql.ErrNoRows) {
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

func (h *Handler) handlePutBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid policy body", requestID, r.URL.Path)
		return
	}
	policy := strings.TrimSpace(string(body))
	if policy == "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "policy required", requestID, r.URL.Path)
		return
	}
	if _, err := ParsePolicy(policy); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid policy", requestID, r.URL.Path)
		return
	}
	if err := h.Meta.SetBucketPolicy(ctx, bucket, policy); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDeleteBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
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
	if err := h.Meta.DeleteBucketPolicy(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
