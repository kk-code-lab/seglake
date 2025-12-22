package s3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type oplogResponse struct {
	Entries []meta.OplogEntry `json:"entries"`
	LastHLC string            `json:"last_hlc,omitempty"`
}

type oplogApplyRequest struct {
	Entries []meta.OplogEntry `json:"entries"`
}

type oplogApplyResponse struct {
	Applied          int            `json:"applied"`
	MissingManifests []string       `json:"missing_manifests,omitempty"`
	MissingChunks    []missingChunk `json:"missing_chunks,omitempty"`
}

type missingChunk struct {
	SegmentID string `json:"segment_id"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"len"`
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

func (h *Handler) handleOplogApply(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h == nil || h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	var req oplogApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "invalid json body", requestID, r.URL.Path)
		return
	}
	applied, err := h.Meta.ApplyOplogEntries(ctx, req.Entries)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "oplog apply failed", requestID, r.URL.Path)
		return
	}
	resp := oplogApplyResponse{Applied: applied}
	if h.Engine != nil {
		missingManifests := make(map[string]struct{})
		missingChunks := make(map[string]missingChunk)
		for _, entry := range req.Entries {
			if entry.OpType != "put" || entry.VersionID == "" {
				continue
			}
			man, err := h.Engine.GetManifest(ctx, entry.VersionID)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					missingManifests[entry.VersionID] = struct{}{}
					continue
				}
				writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "manifest read failed", requestID, r.URL.Path)
				return
			}
			chunks, err := h.Engine.MissingChunks(man)
			if err != nil {
				writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "segment check failed", requestID, r.URL.Path)
				return
			}
			for _, ch := range chunks {
				key := ch.SegmentID + ":" + strconv.FormatInt(ch.Offset, 10) + ":" + strconv.FormatInt(ch.Length, 10)
				missingChunks[key] = missingChunk{
					SegmentID: ch.SegmentID,
					Offset:    ch.Offset,
					Length:    ch.Length,
				}
			}
		}
		if len(missingManifests) > 0 {
			resp.MissingManifests = make([]string, 0, len(missingManifests))
			for versionID := range missingManifests {
				resp.MissingManifests = append(resp.MissingManifests, versionID)
			}
		}
		if len(missingChunks) > 0 {
			resp.MissingChunks = make([]missingChunk, 0, len(missingChunks))
			for _, ch := range missingChunks {
				resp.MissingChunks = append(resp.MissingChunks, ch)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleReplicationManifest(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h == nil || h.Engine == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "storage not initialized", requestID, r.URL.Path)
		return
	}
	versionID := strings.TrimSpace(r.URL.Query().Get("versionId"))
	if versionID == "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "versionId required", requestID, r.URL.Path)
		return
	}
	data, err := h.Engine.ManifestBytes(ctx, versionID)
	if err != nil {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchKey", "manifest not found", requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("x-amz-version-id", versionID)
	_, _ = w.Write(data)
}

func (h *Handler) handleReplicationChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h == nil || h.Engine == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "storage not initialized", requestID, r.URL.Path)
		return
	}
	segmentID := strings.TrimSpace(r.URL.Query().Get("segmentId"))
	rawOffset := strings.TrimSpace(r.URL.Query().Get("offset"))
	rawLen := strings.TrimSpace(r.URL.Query().Get("len"))
	if segmentID == "" || rawOffset == "" || rawLen == "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "segmentId/offset/len required", requestID, r.URL.Path)
		return
	}
	offset, err := strconv.ParseInt(rawOffset, 10, 64)
	if err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "invalid offset", requestID, r.URL.Path)
		return
	}
	length, err := strconv.ParseInt(rawLen, 10, 64)
	if err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "invalid len", requestID, r.URL.Path)
		return
	}
	data, err := h.Engine.ReadSegmentRange(segmentID, offset, length)
	if err != nil {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchKey", "segment data not found", requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}
