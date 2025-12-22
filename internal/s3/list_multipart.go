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
	keyMarker := q.Get("key-marker")
	uploadIDMarker := q.Get("upload-id-marker")
	maxUploads := parseMaxUploads(q.Get("max-uploads"))

	uploads, err := h.Meta.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
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
		Bucket:         bucket,
		Prefix:         prefix,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		IsTruncated:    len(uploads) == maxUploads,
		Uploads:        out,
	}
	if resp.IsTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		resp.NextKeyMarker = last.Key
		resp.NextUploadIDMarker = last.UploadID
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
