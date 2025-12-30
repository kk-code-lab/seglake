package s3

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []listContents `xml:"Contents,omitempty"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes,omitempty"`
}

type listBucketResultV1 struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker,omitempty"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []listContents `xml:"Contents,omitempty"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes,omitempty"`
}

type listContents struct {
	Key          string `xml:"Key"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type locationResult struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Value   string   `xml:",chardata"`
}

func (h *Handler) handleListV2(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := parseMaxKeys(q.Get("max-keys"))
	afterKey, afterVersion := decodeContinuation(q.Get("continuation-token"))
	if afterKey == "" {
		afterKey = q.Get("start-after")
	}

	contents, common, count, truncated, lastKey, lastVersion, err := h.listObjects(ctx, bucket, prefix, delimiter, afterKey, afterVersion, maxKeys)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	resp := listBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		StartAfter:     q.Get("start-after"),
		KeyCount:       count,
		MaxKeys:        maxKeys,
		IsTruncated:    truncated,
		Contents:       contents,
		CommonPrefixes: common,
	}
	if truncated && lastKey != "" {
		resp.NextContinuationToken = encodeContinuation(lastKey, lastVersion)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleListV1(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := parseMaxKeys(q.Get("max-keys"))
	marker := q.Get("marker")

	contents, common, _, truncated, lastKey, _, err := h.listObjects(ctx, bucket, prefix, delimiter, marker, "", maxKeys)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	resp := listBucketResultV1{
		Name:           bucket,
		Prefix:         prefix,
		Marker:         marker,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    truncated,
		Contents:       contents,
		CommonPrefixes: common,
	}
	if truncated && lastKey != "" {
		resp.NextMarker = lastKey
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleLocation(w http.ResponseWriter, requestID string) {
	region := ""
	if h.Auth != nil && h.Auth.Region != "" && h.Auth.Region != "us-east-1" {
		region = h.Auth.Region
	}
	resp := locationResult{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Value: region,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) listObjects(ctx context.Context, bucket, prefix, delimiter, afterKey, afterVersion string, maxKeys int) ([]listContents, []commonPrefix, int, bool, string, string, error) {
	pageLimit := maxKeys
	if pageLimit <= 0 {
		pageLimit = 1000
	}

	contents := make([]listContents, 0)
	common := make([]commonPrefix, 0)
	commonSet := make(map[string]struct{})
	count := 0
	truncated := false
	var lastKey string
	var lastVersion string

	for {
		objs, err := h.Meta.ListObjects(ctx, bucket, prefix, afterKey, afterVersion, pageLimit)
		if err != nil {
			return nil, nil, 0, false, "", "", err
		}
		if len(objs) == 0 {
			break
		}
		for _, obj := range objs {
			lastKey = obj.Key
			lastVersion = obj.VersionID
			if delimiter != "" {
				rest := strings.TrimPrefix(obj.Key, prefix)
				if rest != obj.Key {
					if idx := strings.Index(rest, delimiter); idx >= 0 {
						cp := prefix + rest[:idx+len(delimiter)]
						if _, ok := commonSet[cp]; !ok {
							commonSet[cp] = struct{}{}
							common = append(common, commonPrefix{Prefix: cp})
							count++
						}
						if count >= maxKeys {
							truncated = true
							break
						}
						continue
					}
				}
			}
			contents = append(contents, listContents{
				Key:          obj.Key,
				ETag:         `"` + obj.ETag + `"`,
				Size:         obj.Size,
				LastModified: formatLastModified(obj.LastModified),
				StorageClass: "STANDARD",
			})
			count++
			if count >= maxKeys {
				truncated = true
				break
			}
		}
		if truncated {
			break
		}
		if len(objs) < pageLimit {
			break
		}
		afterKey = lastKey
		afterVersion = lastVersion
	}
	return contents, common, count, truncated, lastKey, lastVersion, nil
}

func parseMaxKeys(raw string) int {
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

func encodeContinuation(key, versionID string) string {
	if versionID == "" {
		return base64.RawURLEncoding.EncodeToString([]byte(key))
	}
	return base64.RawURLEncoding.EncodeToString([]byte(key + "\n" + versionID))
}

func decodeContinuation(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		// Fallback: treat token as raw key for compatibility.
		return raw, ""
	}
	parts := strings.SplitN(string(data), "\n", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func formatLastModified(raw string) string {
	if raw == "" {
		return ""
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC().Format(time.RFC3339)
	}
	return raw
}
