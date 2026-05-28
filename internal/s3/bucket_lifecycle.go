package s3

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type lifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration" json:"-"`
	Xmlns   string          `xml:"xmlns,attr,omitempty" json:"-"`
	Rules   []lifecycleRule `xml:"Rule" json:"rules"`
}

type lifecycleRule struct {
	ID                             string                             `xml:"ID,omitempty" json:"id,omitempty"`
	Status                         string                             `xml:"Status" json:"status"`
	Prefix                         *string                            `xml:"Prefix" json:"prefix,omitempty"`
	Filter                         *lifecycleFilter                   `xml:"Filter" json:"filter,omitempty"`
	Expiration                     *lifecycleExpiration               `xml:"Expiration" json:"expiration,omitempty"`
	NoncurrentVersionExpiration    *lifecycleNoncurrentExpiration     `xml:"NoncurrentVersionExpiration" json:"noncurrent_version_expiration,omitempty"`
	AbortIncompleteMultipartUpload *lifecycleAbortIncompleteMultipart `xml:"AbortIncompleteMultipartUpload" json:"abort_incomplete_multipart_upload,omitempty"`
	Transition                     []struct{}                         `xml:"Transition" json:"-"`
	NoncurrentVersionTransition    []struct{}                         `xml:"NoncurrentVersionTransition" json:"-"`
}

type lifecycleFilter struct {
	Prefix                *string             `xml:"Prefix" json:"prefix,omitempty"`
	Tag                   *lifecycleTag       `xml:"Tag" json:"tag,omitempty"`
	And                   *lifecycleAndFilter `xml:"And" json:"and,omitempty"`
	ObjectSizeGreaterThan *int64              `xml:"ObjectSizeGreaterThan" json:"-"`
	ObjectSizeLessThan    *int64              `xml:"ObjectSizeLessThan" json:"-"`
}

type lifecycleAndFilter struct {
	Prefix                *string        `xml:"Prefix" json:"prefix,omitempty"`
	Tags                  []lifecycleTag `xml:"Tag" json:"tags,omitempty"`
	ObjectSizeGreaterThan *int64         `xml:"ObjectSizeGreaterThan" json:"-"`
	ObjectSizeLessThan    *int64         `xml:"ObjectSizeLessThan" json:"-"`
}

type lifecycleTag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
}

type lifecycleExpiration struct {
	Days                      *int   `xml:"Days" json:"days,omitempty"`
	Date                      string `xml:"Date" json:"date,omitempty"`
	ExpiredObjectDeleteMarker *bool  `xml:"ExpiredObjectDeleteMarker" json:"-"`
}

type lifecycleNoncurrentExpiration struct {
	NoncurrentDays *int `xml:"NoncurrentDays" json:"noncurrent_days,omitempty"`
}

type lifecycleAbortIncompleteMultipart struct {
	DaysAfterInitiation *int `xml:"DaysAfterInitiation" json:"days_after_initiation,omitempty"`
}

var errLifecycleUnsupportedFeature = errors.New("unsupported lifecycle feature")

func parseBucketLifecycleXML(r io.Reader) (xmlText, normalizedJSON, fingerprint, ruleIDs string, err error) {
	body, err := io.ReadAll(io.LimitReader(r, 1<<20))
	if err != nil {
		return "", "", "", "", fmt.Errorf("read lifecycle xml: %w", err)
	}
	xmlText = strings.TrimSpace(string(body))
	if xmlText == "" {
		return "", "", "", "", fmt.Errorf("lifecycle configuration required")
	}
	var cfg lifecycleConfiguration
	if err := xml.Unmarshal([]byte(xmlText), &cfg); err != nil {
		return "", "", "", "", fmt.Errorf("invalid xml")
	}
	if cfg.XMLName.Local != "LifecycleConfiguration" {
		return "", "", "", "", fmt.Errorf("invalid lifecycle configuration root")
	}
	normalized, ids, err := normalizeLifecycleConfig(cfg)
	if err != nil {
		return "", "", "", "", err
	}
	normalizedBytes, err := json.Marshal(normalized)
	if err != nil {
		return "", "", "", "", err
	}
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return "", "", "", "", err
	}
	sum := sha256.Sum256(normalizedBytes)
	return xmlText, string(normalizedBytes), hex.EncodeToString(sum[:]), string(idsBytes), nil
}

func normalizeLifecycleConfig(cfg lifecycleConfiguration) (lifecycleConfiguration, []string, error) {
	if len(cfg.Rules) == 0 || len(cfg.Rules) > 1000 {
		return lifecycleConfiguration{}, nil, fmt.Errorf("lifecycle configuration requires 1 to 1000 rules")
	}
	idsSeen := map[string]struct{}{}
	ruleIDs := make([]string, 0, len(cfg.Rules))
	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		rule.ID = strings.TrimSpace(rule.ID)
		rule.Status = strings.TrimSpace(rule.Status)
		if len([]byte(rule.ID)) > 255 {
			return lifecycleConfiguration{}, nil, fmt.Errorf("lifecycle rule id too long")
		}
		if rule.ID != "" {
			if _, ok := idsSeen[rule.ID]; ok {
				return lifecycleConfiguration{}, nil, fmt.Errorf("duplicate lifecycle rule id")
			}
			idsSeen[rule.ID] = struct{}{}
			ruleIDs = append(ruleIDs, rule.ID)
		}
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return lifecycleConfiguration{}, nil, fmt.Errorf("invalid lifecycle rule status")
		}
		if len(rule.Transition) > 0 || len(rule.NoncurrentVersionTransition) > 0 {
			return lifecycleConfiguration{}, nil, fmt.Errorf("%w: lifecycle transitions are not implemented", errLifecycleUnsupportedFeature)
		}
		if rule.Prefix != nil && rule.Filter != nil {
			return lifecycleConfiguration{}, nil, fmt.Errorf("lifecycle rule cannot use both Prefix and Filter")
		}
		if err := normalizeLifecycleFilter(rule.Filter); err != nil {
			return lifecycleConfiguration{}, nil, err
		}
		actionCount := 0
		if rule.Expiration != nil {
			actionCount++
			if err := normalizeLifecycleExpiration(rule.Expiration); err != nil {
				return lifecycleConfiguration{}, nil, err
			}
		}
		if rule.NoncurrentVersionExpiration != nil {
			actionCount++
			if rule.NoncurrentVersionExpiration.NoncurrentDays == nil || *rule.NoncurrentVersionExpiration.NoncurrentDays <= 0 {
				return lifecycleConfiguration{}, nil, fmt.Errorf("NoncurrentVersionExpiration requires positive NoncurrentDays")
			}
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			actionCount++
			if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation == nil || *rule.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0 {
				return lifecycleConfiguration{}, nil, fmt.Errorf("AbortIncompleteMultipartUpload requires positive DaysAfterInitiation")
			}
			if lifecycleFilterHasTags(rule.Filter) {
				return lifecycleConfiguration{}, nil, fmt.Errorf("AbortIncompleteMultipartUpload does not support tag filters")
			}
		}
		if actionCount == 0 {
			return lifecycleConfiguration{}, nil, fmt.Errorf("lifecycle rule requires an action")
		}
	}
	sort.Strings(ruleIDs)
	cfg.Xmlns = ""
	sort.SliceStable(cfg.Rules, func(i, j int) bool {
		if cfg.Rules[i].ID == cfg.Rules[j].ID {
			return i < j
		}
		return cfg.Rules[i].ID < cfg.Rules[j].ID
	})
	return cfg, ruleIDs, nil
}

func normalizeLifecycleExpiration(exp *lifecycleExpiration) error {
	if exp.ExpiredObjectDeleteMarker != nil {
		return fmt.Errorf("%w: ExpiredObjectDeleteMarker is not implemented", errLifecycleUnsupportedFeature)
	}
	hasDays := exp.Days != nil
	hasDate := strings.TrimSpace(exp.Date) != ""
	if hasDays == hasDate {
		return fmt.Errorf("expiration requires exactly one of Days or Date")
	}
	if hasDays && *exp.Days <= 0 {
		return fmt.Errorf("expiration Days must be positive")
	}
	exp.Date = strings.TrimSpace(exp.Date)
	if hasDate {
		if _, err := time.Parse(time.RFC3339, exp.Date); err != nil {
			if _, dateErr := time.Parse("2006-01-02", exp.Date); dateErr != nil {
				return fmt.Errorf("invalid expiration Date")
			}
		}
	}
	return nil
}

func normalizeLifecycleFilter(filter *lifecycleFilter) error {
	if filter == nil {
		return nil
	}
	if filter.ObjectSizeGreaterThan != nil || filter.ObjectSizeLessThan != nil {
		return fmt.Errorf("%w: lifecycle object size filters are not implemented", errLifecycleUnsupportedFeature)
	}
	kinds := 0
	if filter.Prefix != nil {
		kinds++
	}
	if filter.Tag != nil {
		kinds++
		if err := validateObjectTags([]meta.ObjectTag{{Key: filter.Tag.Key, Value: filter.Tag.Value}}); err != nil {
			return err
		}
	}
	if filter.And != nil {
		kinds++
		if filter.And.ObjectSizeGreaterThan != nil || filter.And.ObjectSizeLessThan != nil {
			return fmt.Errorf("%w: lifecycle object size filters are not implemented", errLifecycleUnsupportedFeature)
		}
		if len(filter.And.Tags) == 0 {
			return fmt.Errorf("lifecycle And filter requires at least one tag")
		}
		tags := make([]meta.ObjectTag, 0, len(filter.And.Tags))
		seen := map[string]struct{}{}
		for _, tag := range filter.And.Tags {
			if _, ok := seen[tag.Key]; ok {
				return fmt.Errorf("duplicate lifecycle tag filter key")
			}
			seen[tag.Key] = struct{}{}
			tags = append(tags, meta.ObjectTag{Key: tag.Key, Value: tag.Value})
		}
		if err := validateObjectTags(tags); err != nil {
			return err
		}
		sort.Slice(filter.And.Tags, func(i, j int) bool {
			return filter.And.Tags[i].Key < filter.And.Tags[j].Key
		})
	}
	if kinds > 1 {
		return fmt.Errorf("lifecycle Filter must contain only one of Prefix, Tag, or And")
	}
	return nil
}

func lifecycleFilterHasTags(filter *lifecycleFilter) bool {
	if filter == nil {
		return false
	}
	if filter.Tag != nil {
		return true
	}
	return filter.And != nil && len(filter.And.Tags) > 0
}

func (h *Handler) handleGetBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	cfg, err := h.Meta.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErrorWithResource(w, http.StatusNotFound, "NoSuchLifecycleConfiguration", "bucket lifecycle configuration not found", requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(cfg.XML))
}

func (h *Handler) handlePutBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	xmlText, normalizedJSON, fingerprint, ruleIDs, err := parseBucketLifecycleXML(r.Body)
	if err != nil {
		if errors.Is(err, errLifecycleUnsupportedFeature) {
			writeErrorWithResource(w, http.StatusNotImplemented, "NotImplemented", err.Error(), requestID, r.URL.Path)
			return
		}
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", err.Error(), requestID, r.URL.Path)
		return
	}
	if err := h.Meta.SetBucketLifecycle(ctx, meta.BucketLifecycleConfig{
		Bucket:            bucket,
		XML:               xmlText,
		NormalizedJSON:    normalizedJSON,
		ConfigFingerprint: fingerprint,
		RuleIDs:           ruleIDs,
	}); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucketLifecycle(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	if !h.ensureBucketLifecycleTarget(ctx, w, r, bucket, requestID) {
		return
	}
	if err := h.Meta.DeleteBucketLifecycle(ctx, bucket); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ensureBucketLifecycleTarget(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, requestID string) bool {
	if h.Meta == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "meta not initialized", requestID, r.URL.Path)
		return false
	}
	exists, err := h.Meta.BucketExists(ctx, bucket)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return false
	}
	if !exists {
		writeErrorWithResource(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", requestID, r.URL.Path)
		return false
	}
	return true
}
