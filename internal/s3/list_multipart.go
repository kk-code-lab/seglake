package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
)

type listMultipartResult struct {
	XMLName            xml.Name             `xml:"ListMultipartUploadsResult"`
	Bucket             string               `xml:"Bucket"`
	Prefix             string               `xml:"Prefix"`
	Delimiter          string               `xml:"Delimiter,omitempty"`
	KeyMarker          string               `xml:"KeyMarker"`
	UploadIDMarker     string               `xml:"UploadIdMarker"`
	MaxUploads         int                  `xml:"MaxUploads"`
	IsTruncated        bool                 `xml:"IsTruncated"`
	NextKeyMarker      string               `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string               `xml:"NextUploadIdMarker,omitempty"`
	Uploads            []multipartUploadOut `xml:"Upload"`
	CommonPrefixes     []commonPrefix       `xml:"CommonPrefixes,omitempty"`
}

type multipartUploadOut struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

func (h *Handler) handleListMultipartUploads(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	keyMarker := q.Get("key-marker")
	uploadIDMarker := q.Get("upload-id-marker")
	maxUploads := parseMaxUploads(q.Get("max-uploads"))

	if keyMarker == "" && uploadIDMarker != "" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "upload-id-marker requires key-marker", requestID, r.URL.Path)
		return
	}

	out, common, truncated, nextKey, nextUpload, err := h.listMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIDMarker, maxUploads)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	resp := listMultipartResult{
		Bucket:         bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		IsTruncated:    truncated,
		Uploads:        out,
		CommonPrefixes: common,
	}
	if resp.IsTruncated && nextKey != "" && nextUpload != "" {
		resp.NextKeyMarker = nextKey
		resp.NextUploadIDMarker = nextUpload
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) listMultipartUploads(ctx context.Context, bucket, prefix, delimiter, keyMarker, uploadIDMarker string, maxUploads int) ([]multipartUploadOut, []commonPrefix, bool, string, string, error) {
	pageLimit := maxUploads
	if pageLimit <= 0 {
		pageLimit = 1000
	}

	uploadsOut := make([]multipartUploadOut, 0)
	common := make([]commonPrefix, 0)
	commonSet := make(map[string]struct{})
	count := 0
	truncated := false
	afterKey := keyMarker
	afterUpload := uploadIDMarker
	var lastKey string
	var lastUpload string

	for {
		uploads, err := h.Meta.ListMultipartUploads(ctx, bucket, prefix, afterKey, afterUpload, pageLimit)
		if err != nil {
			return nil, nil, false, "", "", err
		}
		if len(uploads) == 0 {
			break
		}
		for _, up := range uploads {
			lastKey = up.Key
			lastUpload = up.UploadID
			if delimiter != "" && strings.HasPrefix(up.Key, prefix) {
				rest := strings.TrimPrefix(up.Key, prefix)
				if idx := strings.Index(rest, delimiter); idx >= 0 {
					cp := prefix + rest[:idx+len(delimiter)]
					if _, ok := commonSet[cp]; !ok {
						commonSet[cp] = struct{}{}
						common = append(common, commonPrefix{Prefix: cp})
						count++
					}
					if count >= maxUploads {
						truncated = true
						break
					}
					continue
				}
			}
			uploadsOut = append(uploadsOut, multipartUploadOut{
				Key:       up.Key,
				UploadID:  up.UploadID,
				Initiated: formatLastModified(up.CreatedAt),
			})
			count++
			if count >= maxUploads {
				truncated = true
				break
			}
		}
		if truncated {
			break
		}
		if len(uploads) < pageLimit {
			break
		}
		afterKey = lastKey
		afterUpload = lastUpload
	}

	return uploadsOut, common, truncated, lastKey, lastUpload, nil
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
