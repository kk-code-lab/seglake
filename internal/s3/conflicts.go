package s3

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type conflictItem struct {
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	VersionID    string `json:"version_id"`
	ETag         string `json:"etag,omitempty"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified_utc"`
}

type conflictsResponse struct {
	Items       []conflictItem `json:"items"`
	NextBucket  string         `json:"next_bucket,omitempty"`
	NextKey     string         `json:"next_key,omitempty"`
	NextVersion string         `json:"next_version,omitempty"`
}

func (h *Handler) handleConflicts(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	query := r.URL.Query()
	bucket := strings.TrimSpace(query.Get("bucket"))
	prefix := strings.TrimSpace(query.Get("prefix"))
	afterBucket := strings.TrimSpace(query.Get("after_bucket"))
	afterKey := strings.TrimSpace(query.Get("after_key"))
	afterVersion := strings.TrimSpace(query.Get("after_version"))
	limit := 0
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		v, err := parseInt(rawLimit)
		if err != nil || v <= 0 || v > 10000 {
			writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid limit", requestID, r.URL.Path)
			return
		}
		limit = int(v)
	}
	items, err := h.Meta.ListConflicts(ctx, bucket, prefix, afterBucket, afterKey, afterVersion, limit)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	resp := conflictsResponse{
		Items: make([]conflictItem, 0, len(items)),
	}
	for _, item := range items {
		resp.Items = append(resp.Items, conflictItem{
			Bucket:       item.Bucket,
			Key:          item.Key,
			VersionID:    item.VersionID,
			ETag:         item.ETag,
			Size:         item.Size,
			LastModified: item.LastModified,
		})
	}
	if limit > 0 && len(items) == limit {
		last := items[len(items)-1]
		resp.NextBucket = last.Bucket
		resp.NextKey = last.Key
		resp.NextVersion = last.VersionID
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
