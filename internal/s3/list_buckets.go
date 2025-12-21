package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"time"
)

type listBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucket `xml:"Bucket"`
}

type bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func (h *Handler) handleListBuckets(ctx context.Context, w http.ResponseWriter, requestID string) {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, "/")
		return
	}
	names, err := h.Meta.ListBuckets(ctx)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, "/")
		return
	}
	out := listBucketsResult{
		Owner: owner{ID: "seglake", DisplayName: "seglake"},
		Buckets: buckets{
			Bucket: make([]bucket, 0, len(names)),
		},
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, name := range names {
		out.Buckets.Bucket = append(out.Buckets.Bucket, bucket{
			Name:         name,
			CreationDate: now,
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(out)
}
