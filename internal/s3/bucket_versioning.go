package s3

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
)

const versioningXMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type bucketVersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status,omitempty"`
}

func (h *Handler) handleGetBucketVersioning(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	state, err := h.Meta.GetBucketVersioningState(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	resp := bucketVersioningConfiguration{
		Xmlns: versioningXMLNamespace,
	}
	switch state {
	case meta.BucketVersioningEnabled:
		resp.Status = "Enabled"
	case meta.BucketVersioningSuspended:
		resp.Status = "Suspended"
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handlePutBucketVersioning(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return
	}
	var req bucketVersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid xml", requestID, r.URL.Path)
		return
	}
	status := strings.TrimSpace(req.Status)
	if status == "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "missing versioning status", requestID, r.URL.Path)
		return
	}
	target, ok := normalizeVersioningStatus(status)
	if !ok {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid versioning status", requestID, r.URL.Path)
		return
	}
	if err := h.Meta.SetBucketVersioningState(ctx, bucket, target); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func normalizeVersioningStatus(status string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "enabled":
		return meta.BucketVersioningEnabled, true
	case "suspended":
		return meta.BucketVersioningSuspended, true
	default:
		return "", false
	}
}

func parseCreateBucketVersioningHeader(raw string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "enabled":
		return meta.BucketVersioningEnabled, true
	case "unversioned", "disabled":
		return meta.BucketVersioningDisabled, true
	default:
		return "", false
	}
}
