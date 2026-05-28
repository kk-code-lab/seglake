package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf16"
)

type Configuration struct {
	XMLName xml.Name `xml:"LifecycleConfiguration" json:"-"`
	Xmlns   string   `xml:"xmlns,attr,omitempty" json:"-"`
	Rules   []Rule   `xml:"Rule" json:"rules"`
}

type Rule struct {
	ID                             string                    `xml:"ID,omitempty" json:"id,omitempty"`
	Status                         string                    `xml:"Status" json:"status"`
	Prefix                         *string                   `xml:"Prefix" json:"prefix,omitempty"`
	Filter                         *Filter                   `xml:"Filter" json:"filter,omitempty"`
	Expiration                     *Expiration               `xml:"Expiration" json:"expiration,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentExpiration     `xml:"NoncurrentVersionExpiration" json:"noncurrent_version_expiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipart `xml:"AbortIncompleteMultipartUpload" json:"abort_incomplete_multipart_upload,omitempty"`
	Transition                     []struct{}                `xml:"Transition" json:"-"`
	NoncurrentVersionTransition    []struct{}                `xml:"NoncurrentVersionTransition" json:"-"`
}

type Filter struct {
	Prefix                *string    `xml:"Prefix" json:"prefix,omitempty"`
	Tag                   *Tag       `xml:"Tag" json:"tag,omitempty"`
	And                   *AndFilter `xml:"And" json:"and,omitempty"`
	ObjectSizeGreaterThan *int64     `xml:"ObjectSizeGreaterThan" json:"-"`
	ObjectSizeLessThan    *int64     `xml:"ObjectSizeLessThan" json:"-"`
}

type AndFilter struct {
	Prefix                *string `xml:"Prefix" json:"prefix,omitempty"`
	Tags                  []Tag   `xml:"Tag" json:"tags,omitempty"`
	ObjectSizeGreaterThan *int64  `xml:"ObjectSizeGreaterThan" json:"-"`
	ObjectSizeLessThan    *int64  `xml:"ObjectSizeLessThan" json:"-"`
}

type Tag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
}

type Expiration struct {
	Days                      *int   `xml:"Days" json:"days,omitempty"`
	Date                      string `xml:"Date" json:"date,omitempty"`
	ExpiredObjectDeleteMarker *bool  `xml:"ExpiredObjectDeleteMarker" json:"-"`
}

type NoncurrentExpiration struct {
	NoncurrentDays *int `xml:"NoncurrentDays" json:"noncurrent_days,omitempty"`
}

type AbortIncompleteMultipart struct {
	DaysAfterInitiation *int `xml:"DaysAfterInitiation" json:"days_after_initiation,omitempty"`
}

type ParseResult struct {
	XMLText          string
	NormalizedJSON   string
	Fingerprint      string
	RuleIDsJSON      string
	RuleIDs          []string
	NormalizedConfig Configuration
}

type ObjectTags []Tag

var ErrUnsupportedFeature = errors.New("unsupported lifecycle feature")

func ParseXML(r io.Reader) (ParseResult, error) {
	body, err := io.ReadAll(io.LimitReader(r, 1<<20))
	if err != nil {
		return ParseResult{}, fmt.Errorf("read lifecycle xml: %w", err)
	}
	xmlText := strings.TrimSpace(string(body))
	if xmlText == "" {
		return ParseResult{}, fmt.Errorf("lifecycle configuration required")
	}
	var cfg Configuration
	if err := xml.Unmarshal([]byte(xmlText), &cfg); err != nil {
		return ParseResult{}, fmt.Errorf("invalid xml")
	}
	if cfg.XMLName.Local != "LifecycleConfiguration" {
		return ParseResult{}, fmt.Errorf("invalid lifecycle configuration root")
	}
	normalized, ids, err := Normalize(cfg)
	if err != nil {
		return ParseResult{}, err
	}
	normalizedBytes, err := json.Marshal(normalized)
	if err != nil {
		return ParseResult{}, err
	}
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return ParseResult{}, err
	}
	sum := sha256.Sum256(normalizedBytes)
	return ParseResult{
		XMLText:          xmlText,
		NormalizedJSON:   string(normalizedBytes),
		Fingerprint:      hex.EncodeToString(sum[:]),
		RuleIDsJSON:      string(idsBytes),
		RuleIDs:          ids,
		NormalizedConfig: normalized,
	}, nil
}

func DecodeNormalized(raw string) (Configuration, error) {
	var cfg Configuration
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return Configuration{}, err
	}
	if len(cfg.Rules) == 0 {
		return Configuration{}, fmt.Errorf("lifecycle configuration has no rules")
	}
	return cfg, nil
}

func Normalize(cfg Configuration) (Configuration, []string, error) {
	if len(cfg.Rules) == 0 || len(cfg.Rules) > 1000 {
		return Configuration{}, nil, fmt.Errorf("lifecycle configuration requires 1 to 1000 rules")
	}
	idsSeen := map[string]struct{}{}
	ruleIDs := make([]string, 0, len(cfg.Rules))
	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		rule.ID = strings.TrimSpace(rule.ID)
		rule.Status = strings.TrimSpace(rule.Status)
		if len([]byte(rule.ID)) > 255 {
			return Configuration{}, nil, fmt.Errorf("lifecycle rule id too long")
		}
		if rule.ID != "" {
			if _, ok := idsSeen[rule.ID]; ok {
				return Configuration{}, nil, fmt.Errorf("duplicate lifecycle rule id")
			}
			idsSeen[rule.ID] = struct{}{}
			ruleIDs = append(ruleIDs, rule.ID)
		}
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return Configuration{}, nil, fmt.Errorf("invalid lifecycle rule status")
		}
		if len(rule.Transition) > 0 || len(rule.NoncurrentVersionTransition) > 0 {
			return Configuration{}, nil, fmt.Errorf("%w: lifecycle transitions are not implemented", ErrUnsupportedFeature)
		}
		if rule.Prefix != nil && rule.Filter != nil {
			return Configuration{}, nil, fmt.Errorf("lifecycle rule cannot use both Prefix and Filter")
		}
		if err := normalizeFilter(rule.Filter); err != nil {
			return Configuration{}, nil, err
		}
		actionCount := 0
		if rule.Expiration != nil {
			actionCount++
			if err := normalizeExpiration(rule.Expiration); err != nil {
				return Configuration{}, nil, err
			}
		}
		if rule.NoncurrentVersionExpiration != nil {
			actionCount++
			if rule.NoncurrentVersionExpiration.NoncurrentDays == nil || *rule.NoncurrentVersionExpiration.NoncurrentDays <= 0 {
				return Configuration{}, nil, fmt.Errorf("noncurrentVersionExpiration requires positive NoncurrentDays")
			}
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			actionCount++
			if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation == nil || *rule.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0 {
				return Configuration{}, nil, fmt.Errorf("abortIncompleteMultipartUpload requires positive DaysAfterInitiation")
			}
			if FilterHasTags(rule.Filter) {
				return Configuration{}, nil, fmt.Errorf("AbortIncompleteMultipartUpload does not support tag filters")
			}
		}
		if actionCount == 0 {
			return Configuration{}, nil, fmt.Errorf("lifecycle rule requires an action")
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

func RuleEnabled(rule Rule) bool {
	return rule.Status == "Enabled"
}

func RuleMatches(rule Rule, key string, tags ObjectTags) bool {
	if rule.Prefix != nil {
		return strings.HasPrefix(key, *rule.Prefix)
	}
	if rule.Filter == nil {
		return true
	}
	if rule.Filter.Prefix != nil {
		return strings.HasPrefix(key, *rule.Filter.Prefix)
	}
	if rule.Filter.Tag != nil {
		return tagSetContains(tags, *rule.Filter.Tag)
	}
	if rule.Filter.And != nil {
		if rule.Filter.And.Prefix != nil && !strings.HasPrefix(key, *rule.Filter.And.Prefix) {
			return false
		}
		for _, tag := range rule.Filter.And.Tags {
			if !tagSetContains(tags, tag) {
				return false
			}
		}
		return true
	}
	return true
}

func ExpirationEligible(exp Expiration, objectTime, asOf time.Time) bool {
	if exp.Days != nil {
		return !objectTime.UTC().AddDate(0, 0, *exp.Days).After(asOf.UTC())
	}
	if strings.TrimSpace(exp.Date) != "" {
		date, err := parseExpirationDate(exp.Date)
		if err != nil {
			return false
		}
		return !date.After(asOf.UTC())
	}
	return false
}

func NoncurrentEligible(exp NoncurrentExpiration, versionTime, asOf time.Time) bool {
	if exp.NoncurrentDays == nil {
		return false
	}
	return !versionTime.UTC().AddDate(0, 0, *exp.NoncurrentDays).After(asOf.UTC())
}

func MPUAbortEligible(abort AbortIncompleteMultipart, initiatedAt, asOf time.Time) bool {
	if abort.DaysAfterInitiation == nil {
		return false
	}
	return !initiatedAt.UTC().AddDate(0, 0, *abort.DaysAfterInitiation).After(asOf.UTC())
}

func FilterHasTags(filter *Filter) bool {
	if filter == nil {
		return false
	}
	if filter.Tag != nil {
		return true
	}
	return filter.And != nil && len(filter.And.Tags) > 0
}

func normalizeExpiration(exp *Expiration) error {
	if exp.ExpiredObjectDeleteMarker != nil {
		return fmt.Errorf("%w: ExpiredObjectDeleteMarker is not implemented", ErrUnsupportedFeature)
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
		if _, err := parseExpirationDate(exp.Date); err != nil {
			return fmt.Errorf("invalid expiration Date")
		}
	}
	return nil
}

func normalizeFilter(filter *Filter) error {
	if filter == nil {
		return nil
	}
	if filter.ObjectSizeGreaterThan != nil || filter.ObjectSizeLessThan != nil {
		return fmt.Errorf("%w: lifecycle object size filters are not implemented", ErrUnsupportedFeature)
	}
	kinds := 0
	if filter.Prefix != nil {
		kinds++
	}
	if filter.Tag != nil {
		kinds++
		if err := validateTags([]Tag{*filter.Tag}); err != nil {
			return err
		}
	}
	if filter.And != nil {
		kinds++
		if filter.And.ObjectSizeGreaterThan != nil || filter.And.ObjectSizeLessThan != nil {
			return fmt.Errorf("%w: lifecycle object size filters are not implemented", ErrUnsupportedFeature)
		}
		if len(filter.And.Tags) == 0 {
			return fmt.Errorf("lifecycle And filter requires at least one tag")
		}
		seen := map[string]struct{}{}
		for _, tag := range filter.And.Tags {
			if _, ok := seen[tag.Key]; ok {
				return fmt.Errorf("duplicate lifecycle tag filter key")
			}
			seen[tag.Key] = struct{}{}
		}
		if err := validateTags(filter.And.Tags); err != nil {
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

func validateTags(tags []Tag) error {
	if len(tags) > 10 {
		return fmt.Errorf("too many tags")
	}
	seen := map[string]struct{}{}
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

func tagSetContains(tags ObjectTags, want Tag) bool {
	for _, tag := range tags {
		if tag.Key == want.Key && tag.Value == want.Value {
			return true
		}
	}
	return false
}

func utf16Len(value string) int {
	return len(utf16.Encode([]rune(value)))
}

func parseExpirationDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}
