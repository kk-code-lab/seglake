package s3

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type oplogResponse struct {
	Entries []meta.OplogEntry `json:"entries"`
	LastHLC string            `json:"last_hlc,omitempty"`
}

func (h *Handler) handleOplog(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h == nil || h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	limit := 1000
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 10000 {
		limit = 10000
	}
	since := r.URL.Query().Get("since")
	entries, err := h.Meta.ListOplogSince(ctx, since, limit)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "oplog read failed", requestID, r.URL.Path)
		return
	}
	resp := oplogResponse{Entries: entries}
	if n := len(entries); n > 0 {
		resp.LastHLC = entries[n-1].HLCTS
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
