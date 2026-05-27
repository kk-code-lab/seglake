package s3

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"unicode/utf16"

	"github.com/kk-code-lab/seglake/internal/meta"
)

const objectTaggingXMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type objectTaggingXML struct {
	XMLName xml.Name        `xml:"Tagging"`
	Xmlns   string          `xml:"xmlns,attr,omitempty"`
	TagSet  objectTagSetXML `xml:"TagSet"`
}

type objectTagSetXML struct {
	Tags []objectTagXML `xml:"Tag"`
}

type objectTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

func parseObjectTaggingXML(r *http.Request) ([]meta.ObjectTag, error) {
	var body objectTaggingXML
	if err := xml.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	tags := make([]meta.ObjectTag, 0, len(body.TagSet.Tags))
	for _, tag := range body.TagSet.Tags {
		tags = append(tags, meta.ObjectTag{Key: tag.Key, Value: tag.Value})
	}
	if err := validateObjectTags(tags); err != nil {
		return nil, err
	}
	sortObjectTags(tags)
	return tags, nil
}

func writeObjectTaggingXML(w http.ResponseWriter, tags []meta.ObjectTag) {
	sortObjectTags(tags)
	out := objectTaggingXML{Xmlns: objectTaggingXMLNamespace}
	out.TagSet.Tags = make([]objectTagXML, 0, len(tags))
	for _, tag := range tags {
		out.TagSet.Tags = append(out.TagSet.Tags, objectTagXML{Key: tag.Key, Value: tag.Value})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(out)
}

func parseTaggingHeader(raw string) ([]meta.ObjectTag, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return nil, err
	}
	tags := make([]meta.ObjectTag, 0, len(values))
	for key, vals := range values {
		if len(vals) != 1 {
			return nil, fmt.Errorf("duplicate tag key")
		}
		tags = append(tags, meta.ObjectTag{Key: key, Value: vals[0]})
	}
	if err := validateObjectTags(tags); err != nil {
		return nil, err
	}
	sortObjectTags(tags)
	return tags, nil
}

func validateObjectTags(tags []meta.ObjectTag) error {
	if len(tags) > 10 {
		return fmt.Errorf("too many tags")
	}
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if tag.Key == "" {
			return fmt.Errorf("tag key required")
		}
		if utf16Len(tag.Key) > 128 {
			return fmt.Errorf("tag key too long")
		}
		if utf16Len(tag.Value) > 256 {
			return fmt.Errorf("tag value too long")
		}
		if _, ok := seen[tag.Key]; ok {
			return fmt.Errorf("duplicate tag key")
		}
		seen[tag.Key] = struct{}{}
	}
	return nil
}

func utf16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

func sortObjectTags(tags []meta.ObjectTag) {
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})
}

func (h *Handler) handlePutObjectTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	defer func() { _ = r.Body.Close() }()
	objMeta, ok := h.resolveTaggingTarget(ctx, w, r, bucket, key, requestID)
	if !ok {
		return
	}
	tags, err := parseObjectTaggingXML(r)
	if err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidTag", "invalid object tags", requestID, r.URL.Path)
		return
	}
	if err := h.Engine.CommitMeta(ctx, func(tx *sql.Tx) error {
		return h.Meta.SetObjectTagsTx(ctx, tx, bucket, key, objMeta.VersionID, tags)
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if versionID, ok := h.versionIDHeaderForTagging(ctx, bucket, objMeta); ok {
		w.Header().Set("x-amz-version-id", versionID)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetObjectTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	objMeta, ok := h.resolveTaggingTarget(ctx, w, r, bucket, key, requestID)
	if !ok {
		return
	}
	tags, err := h.Meta.GetObjectTags(ctx, objMeta.VersionID)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if versionID, ok := h.versionIDHeaderForTagging(ctx, bucket, objMeta); ok {
		w.Header().Set("x-amz-version-id", versionID)
	}
	writeObjectTaggingXML(w, tags)
}

func (h *Handler) handleDeleteObjectTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	objMeta, ok := h.resolveTaggingTarget(ctx, w, r, bucket, key, requestID)
	if !ok {
		return
	}
	if err := h.Engine.CommitMeta(ctx, func(tx *sql.Tx) error {
		return h.Meta.DeleteObjectTagsTx(ctx, tx, bucket, key, objMeta.VersionID)
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	if versionID, ok := h.versionIDHeaderForTagging(ctx, bucket, objMeta); ok {
		w.Header().Set("x-amz-version-id", versionID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) resolveTaggingTarget(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, requestID string) (*meta.ObjectMeta, bool) {
	if h == nil || h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return nil, false
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return nil, false
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return nil, false
	}
	versioningState, err := h.bucketVersioningState(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return nil, false
	}
	versionID := r.URL.Query().Get("versionId")
	var objMeta *meta.ObjectMeta
	if versionID == "" {
		objMeta, err = h.Meta.GetObjectMeta(ctx, bucket, key)
	} else if versionID == "null" && isNullVersioningState(versioningState) {
		objMeta, err = h.Meta.GetNullObjectVersion(ctx, bucket, key)
	} else {
		objMeta, err = h.Meta.GetObjectVersion(ctx, bucket, key, versionID)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			code := "NoSuchKey"
			if versionID != "" {
				code = "NoSuchVersion"
			}
			writeErrorWithResource(w, http.StatusNotFound, code, "", requestID, r.URL.Path)
			return nil, false
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return nil, false
	}
	if strings.EqualFold(objMeta.State, meta.VersionStateDeleteMarker) || strings.EqualFold(objMeta.State, meta.VersionStateDeleted) {
		code := "NoSuchKey"
		if versionID != "" {
			code = "NoSuchVersion"
		}
		writeErrorWithResource(w, http.StatusNotFound, code, "", requestID, r.URL.Path)
		return nil, false
	}
	return objMeta, true
}

func (h *Handler) versionIDHeaderForTagging(ctx context.Context, bucket string, objMeta *meta.ObjectMeta) (string, bool) {
	if objMeta == nil {
		return "", false
	}
	state, err := h.bucketVersioningState(ctx, bucket)
	if err != nil {
		return "", false
	}
	return versionIDHeaderForMeta(state, objMeta)
}

func copyObjectTagsForDirective(ctx context.Context, store *meta.Store, srcVersionID string, directive, rawHeader string) ([]meta.ObjectTag, bool, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" || strings.EqualFold(directive, "COPY") {
		tags, err := store.GetObjectTags(ctx, srcVersionID)
		return tags, true, err
	}
	if strings.EqualFold(directive, "REPLACE") {
		tags, err := parseTaggingHeader(rawHeader)
		return tags, true, err
	}
	return nil, false, fmt.Errorf("unsupported tagging directive")
}
