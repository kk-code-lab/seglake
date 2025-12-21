package s3

import (
	"context"
	"encoding/json"
	"net/http"
)

func (h *Handler) handleStats(ctx context.Context, w http.ResponseWriter, requestID string) {
	if h.Meta == nil {
		writeError(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID)
		return
	}
	stats, err := h.Meta.GetStats(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}
