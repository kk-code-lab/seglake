package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type listObjectVersionsResult struct {
	XMLName             xml.Name            `xml:"ListVersionsResult"`
	Name                string              `xml:"Name"`
	Prefix              string              `xml:"Prefix"`
	KeyMarker           string              `xml:"KeyMarker,omitempty"`
	VersionIDMarker     string              `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string              `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string              `xml:"NextVersionIdMarker,omitempty"`
	Delimiter           string              `xml:"Delimiter,omitempty"`
	MaxKeys             int                 `xml:"MaxKeys"`
	IsTruncated         bool                `xml:"IsTruncated"`
	EncodingType        string              `xml:"EncodingType,omitempty"`
	Versions            []listObjectVersion `xml:"Version,omitempty"`
	DeleteMarkers       []listDeleteMarker  `xml:"DeleteMarker,omitempty"`
	CommonPrefixes      []commonPrefix      `xml:"CommonPrefixes,omitempty"`
}

type listObjectVersion struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size,omitempty"`
	StorageClass string `xml:"StorageClass,omitempty"`
}

type listDeleteMarker struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

func (h *Handler) handleListVersions(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
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
	keyMarker := q.Get("key-marker")
	versionIDMarker := q.Get("version-id-marker")
	encodingType := strings.ToLower(strings.TrimSpace(q.Get("encoding-type")))
	if encodingType != "" && encodingType != "url" {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "invalid encoding-type", requestID, r.URL.Path)
		return
	}
	maxKeys := parseMaxKeys(q.Get("max-keys"))

	versioningState, err := h.bucketVersioningState(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if versioningState == meta.BucketVersioningDisabled {
		resp := listObjectVersionsResult{
			Name:        bucket,
			Prefix:      prefix,
			KeyMarker:   keyMarker,
			Delimiter:   delimiter,
			MaxKeys:     maxKeys,
			IsTruncated: false,
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_ = xml.NewEncoder(w).Encode(resp)
		return
	}

	markerVersionID := versionIDMarker
	if versionIDMarker == "null" && keyMarker != "" {
		if nullMeta, err := h.Meta.GetNullObjectVersion(ctx, bucket, keyMarker); err == nil && nullMeta != nil {
			markerVersionID = nullMeta.VersionID
		}
	}

	versions, deletes, common, _, truncated, lastKey, lastVersion, lastIsNull, err := h.listObjectVersions(ctx, bucket, prefix, delimiter, keyMarker, markerVersionID, maxKeys)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}

	encode := func(value string) string {
		if encodingType != "url" || value == "" {
			return value
		}
		return url.PathEscape(value)
	}

	if encodingType == "url" {
		for i := range versions {
			versions[i].Key = encode(versions[i].Key)
			if versions[i].VersionID != "null" {
				versions[i].VersionID = encode(versions[i].VersionID)
			}
		}
		for i := range deletes {
			deletes[i].Key = encode(deletes[i].Key)
			if deletes[i].VersionID != "null" {
				deletes[i].VersionID = encode(deletes[i].VersionID)
			}
		}
		for i := range common {
			common[i].Prefix = encode(common[i].Prefix)
		}
	}

	resp := listObjectVersionsResult{
		Name:            bucket,
		Prefix:          encode(prefix),
		KeyMarker:       encode(keyMarker),
		VersionIDMarker: encode(versionIDMarker),
		Delimiter:       encode(delimiter),
		MaxKeys:         maxKeys,
		IsTruncated:     truncated,
		EncodingType:    encodingType,
		Versions:        versions,
		DeleteMarkers:   deletes,
		CommonPrefixes:  common,
	}
	if truncated && lastKey != "" {
		resp.NextKeyMarker = encode(lastKey)
		if lastVersion != "" {
			if lastIsNull {
				resp.NextVersionIDMarker = "null"
			} else {
				resp.NextVersionIDMarker = encode(lastVersion)
			}
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) listObjectVersions(ctx context.Context, bucket, prefix, delimiter, afterKey, afterVersion string, maxKeys int) ([]listObjectVersion, []listDeleteMarker, []commonPrefix, int, bool, string, string, bool, error) {
	pageLimit := maxKeys
	if pageLimit <= 0 {
		pageLimit = 1000
	}

	versions := make([]listObjectVersion, 0)
	deletes := make([]listDeleteMarker, 0)
	common := make([]commonPrefix, 0)
	commonSet := make(map[string]struct{})
	count := 0
	truncated := false
	var lastKey string
	var lastVersion string
	var lastIsNull bool
	var lastListedKey string

	for {
		objs, err := h.Meta.ListObjectVersions(ctx, bucket, prefix, afterKey, afterVersion, pageLimit)
		if err != nil {
			return nil, nil, nil, 0, false, "", "", false, err
		}
		if len(objs) == 0 {
			break
		}
		for _, obj := range objs {
			lastKey = obj.Key
			lastVersion = obj.VersionID
			lastIsNull = obj.IsNull
			if delimiter != "" && strings.HasPrefix(obj.Key, prefix) {
				rest := strings.TrimPrefix(obj.Key, prefix)
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

			isLatest := false
			if obj.Key != lastListedKey {
				lastListedKey = obj.Key
				isLatest = true
			}
			versionID := obj.VersionID
			if obj.IsNull {
				versionID = "null"
			}
			if obj.State == meta.VersionStateDeleteMarker {
				deletes = append(deletes, listDeleteMarker{
					Key:          obj.Key,
					VersionID:    versionID,
					IsLatest:     isLatest,
					LastModified: formatLastModified(obj.LastModified),
				})
			} else {
				versions = append(versions, listObjectVersion{
					Key:          obj.Key,
					VersionID:    versionID,
					IsLatest:     isLatest,
					LastModified: formatLastModified(obj.LastModified),
					ETag:         `"` + obj.ETag + `"`,
					Size:         obj.Size,
					StorageClass: "STANDARD",
				})
			}
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
	return versions, deletes, common, count, truncated, lastKey, lastVersion, lastIsNull, nil
}
