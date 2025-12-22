package s3

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type statsResponse struct {
	Objects           int64                       `json:"objects"`
	Segments          int64                       `json:"segments"`
	BytesLive         int64                       `json:"bytes_live"`
	LastFsckAt        string                      `json:"last_fsck_at,omitempty"`
	LastFsckErrors    int                         `json:"last_fsck_errors,omitempty"`
	LastScrubAt       string                      `json:"last_scrub_at,omitempty"`
	LastScrubErrors   int                         `json:"last_scrub_errors,omitempty"`
	LastGCAt          string                      `json:"last_gc_at,omitempty"`
	LastGCErrors      int                         `json:"last_gc_errors,omitempty"`
	LastGCReclaimed   int64                       `json:"last_gc_reclaimed_bytes,omitempty"`
	LastGCRewritten   int64                       `json:"last_gc_rewritten_bytes,omitempty"`
	LastGCNewSegments int                         `json:"last_gc_new_segments,omitempty"`
	RequestsTotal     map[string]map[string]int64 `json:"requests_total,omitempty"`
	Inflight          map[string]int64            `json:"inflight,omitempty"`
	BytesInTotal      int64                       `json:"bytes_in_total,omitempty"`
	BytesOutTotal     int64                       `json:"bytes_out_total,omitempty"`
	LatencyMs         map[string]LatencyStats     `json:"latency_ms,omitempty"`
	RequestsByBucket  map[string]map[string]int64 `json:"requests_total_by_bucket,omitempty"`
	LatencyByBucketMs map[string]LatencyStats     `json:"latency_ms_by_bucket,omitempty"`
	RequestsByKey     map[string]map[string]int64 `json:"requests_total_by_key,omitempty"`
	LatencyByKeyMs    map[string]LatencyStats     `json:"latency_ms_by_key,omitempty"`
	GCTrends          []meta.GCTrend              `json:"gc_trends,omitempty"`
}

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
	gcTrends, err := h.Meta.ListGCTrends(ctx, 30)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, resource)
		return
	}
	resp := statsResponse{
		Objects:           stats.Objects,
		Segments:          stats.Segments,
		BytesLive:         stats.BytesLive,
		LastFsckAt:        stats.LastFsckAt,
		LastFsckErrors:    stats.LastFsckErrors,
		LastScrubAt:       stats.LastScrubAt,
		LastScrubErrors:   stats.LastScrubErrors,
		LastGCAt:          stats.LastGCAt,
		LastGCErrors:      stats.LastGCErrors,
		LastGCReclaimed:   stats.LastGCReclaimed,
		LastGCRewritten:   stats.LastGCRewritten,
		LastGCNewSegments: stats.LastGCNewSegments,
		GCTrends:          gcTrends,
	}
	if h.Metrics != nil {
		reqs, inflight, bytesIn, bytesOut, latency, bucketReqs, bucketLatency, keyReqs, keyLatency := h.Metrics.Snapshot()
		resp.RequestsTotal = reqs
		resp.Inflight = inflight
		resp.BytesInTotal = bytesIn
		resp.BytesOutTotal = bytesOut
		resp.LatencyMs = latency
		resp.RequestsByBucket = bucketReqs
		resp.LatencyByBucketMs = bucketLatency
		resp.RequestsByKey = keyReqs
		resp.LatencyByKeyMs = keyLatency
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
