package s3

import (
	"context"
	"encoding/json"
	"net/http"
)

func (h *Handler) handleStats(ctx context.Context, w http.ResponseWriter, requestID string, resource string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, resource)
		return
	}
	stats, err := h.Meta.GetStats(ctx)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}
