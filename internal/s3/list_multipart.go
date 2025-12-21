package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strconv"
)

type listMultipartResult struct {
	XMLName            xml.Name             `xml:"ListMultipartUploadsResult"`
	Bucket             string               `xml:"Bucket"`
	Prefix             string               `xml:"Prefix"`
	KeyMarker          string               `xml:"KeyMarker"`
	UploadIDMarker     string               `xml:"UploadIdMarker"`
	MaxUploads         int                  `xml:"MaxUploads"`
	IsTruncated        bool                 `xml:"IsTruncated"`
	NextKeyMarker      string               `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string               `xml:"NextUploadIdMarker,omitempty"`
	Uploads            []multipartUploadOut `xml:"Upload"`
}

type multipartUploadOut struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

func (h *Handler) handleListMultipartUploads(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	maxUploads := parseMaxUploads(q.Get("max-uploads"))

	uploads, err := h.Meta.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID)
		return
	}
	out := make([]multipartUploadOut, 0, len(uploads))
	for _, up := range uploads {
		out = append(out, multipartUploadOut{
			Key:       up.Key,
			UploadID:  up.UploadID,
			Initiated: formatLastModified(up.CreatedAt),
		})
	}
	resp := listMultipartResult{
		Bucket:      bucket,
		Prefix:      prefix,
		MaxUploads:  maxUploads,
		IsTruncated: false,
		Uploads:     out,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func parseMaxUploads(raw string) int {
	if raw == "" {
		return 1000
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 1000
	}
	if v > 1000 {
		return 1000
	}
	return v
}
