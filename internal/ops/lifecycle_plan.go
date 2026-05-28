package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/lifecycle"
	"github.com/kk-code-lab/seglake/internal/meta"
)

const lifecyclePlanSchemaVersion = 1

const (
	LifecycleActionExpireCurrent    = "expire_current"
	LifecycleActionExpireNoncurrent = "expire_noncurrent"
	LifecycleActionAbortMPU         = "abort_mpu"
)

type LifecyclePlan struct {
	SchemaVersion      int                      `json:"schema_version"`
	GeneratedAt        time.Time                `json:"generated_at"`
	AsOf               time.Time                `json:"as_of"`
	Bucket             string                   `json:"bucket,omitempty"`
	ConfigFingerprints map[string]string        `json:"config_fingerprints"`
	Candidates         []LifecyclePlanCandidate `json:"candidates"`
	Counts             LifecyclePlanCounts      `json:"counts"`
}

type LifecyclePlanCounts struct {
	BucketsScanned        int   `json:"buckets_scanned"`
	RulesScanned          int   `json:"rules_scanned"`
	SkippedRules          int   `json:"skipped_rules"`
	Candidates            int   `json:"candidates"`
	CurrentExpirations    int   `json:"current_expirations"`
	NoncurrentExpirations int   `json:"noncurrent_expirations"`
	MPUAborts             int   `json:"mpu_aborts"`
	EstimatedBytes        int64 `json:"estimated_bytes"`
}

type LifecyclePlanCandidate struct {
	Action            string    `json:"action"`
	Bucket            string    `json:"bucket"`
	Key               string    `json:"key"`
	VersionID         string    `json:"version_id,omitempty"`
	UploadID          string    `json:"upload_id,omitempty"`
	CurrentVersionID  string    `json:"current_version_id,omitempty"`
	State             string    `json:"state"`
	RuleID            string    `json:"rule_id,omitempty"`
	ConfigFingerprint string    `json:"config_fingerprint"`
	Timestamp         time.Time `json:"timestamp"`
	Size              int64     `json:"size,omitempty"`
}

type LifecyclePlanOptions struct {
	Bucket string
	AsOf   time.Time
	Limit  int
}

func LifecyclePlanBuild(metaPath string, opts LifecyclePlanOptions) (*LifecyclePlan, *Report, error) {
	if opts.AsOf.IsZero() {
		opts.AsOf = now().UTC()
	} else {
		opts.AsOf = opts.AsOf.UTC()
	}
	if opts.Limit <= 0 {
		opts.Limit = 10000
	}
	report := newReport("lifecycle-plan")
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = store.Close() }()

	configs, err := lifecycleConfigsForPlan(context.Background(), store, opts.Bucket)
	if err != nil {
		return nil, nil, err
	}
	fingerprints := make(map[string]string, len(configs))
	candidates := make([]LifecyclePlanCandidate, 0)
	limited := false
	for _, cfg := range configs {
		fingerprints[cfg.Bucket] = cfg.ConfigFingerprint
		normalized, err := lifecycle.DecodeNormalized(cfg.NormalizedJSON)
		if err != nil {
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, fmt.Sprintf("%s: %v", cfg.Bucket, err))
			}
			continue
		}
		report.BucketsScanned++
		report.RulesScanned += len(normalized.Rules)
		for _, rule := range normalized.Rules {
			if !lifecycle.RuleEnabled(rule) {
				report.SkippedRules++
			}
		}
		bucketCandidates, err := planBucketLifecycle(context.Background(), store, cfg, normalized, opts.AsOf, opts.Limit-len(candidates))
		if err != nil {
			return nil, nil, err
		}
		for _, cand := range bucketCandidates {
			addLifecycleCandidate(report, cand)
			candidates = append(candidates, cand)
			if len(candidates) >= opts.Limit {
				limited = true
				break
			}
		}
		if limited {
			break
		}
	}
	if limited {
		report.addWarning(fmt.Sprintf("lifecycle-plan: candidate limit %d reached", opts.Limit))
	}
	report.Candidates = len(candidates)
	report.FinishedAt = now().UTC()
	_ = store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
	plan := &LifecyclePlan{
		SchemaVersion:      lifecyclePlanSchemaVersion,
		GeneratedAt:        now().UTC(),
		AsOf:               opts.AsOf,
		Bucket:             opts.Bucket,
		ConfigFingerprints: fingerprints,
		Candidates:         candidates,
		Counts: LifecyclePlanCounts{
			BucketsScanned:        report.BucketsScanned,
			RulesScanned:          report.RulesScanned,
			SkippedRules:          report.SkippedRules,
			Candidates:            report.Candidates,
			CurrentExpirations:    report.CurrentExpirations,
			NoncurrentExpirations: report.NoncurrentExpirations,
			MPUAborts:             report.MPUAborts,
			EstimatedBytes:        report.CandidateBytes,
		},
	}
	return plan, report, nil
}

func WriteLifecyclePlan(path string, plan *LifecyclePlan) error {
	if path == "" || plan == nil {
		return fmt.Errorf("lifecycle-plan: plan and path required")
	}
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ReadLifecyclePlan(path string) (*LifecyclePlan, error) {
	if path == "" {
		return nil, fmt.Errorf("lifecycle-plan: plan path required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan LifecyclePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func lifecycleConfigsForPlan(ctx context.Context, store *meta.Store, bucket string) ([]meta.BucketLifecycleConfig, error) {
	if bucket != "" {
		cfg, err := store.GetBucketLifecycle(ctx, bucket)
		if err != nil {
			return nil, err
		}
		return []meta.BucketLifecycleConfig{cfg}, nil
	}
	configMap, err := store.ListBucketLifecycle(ctx)
	if err != nil {
		return nil, err
	}
	buckets := make([]string, 0, len(configMap))
	for bucket := range configMap {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	out := make([]meta.BucketLifecycleConfig, 0, len(buckets))
	for _, bucket := range buckets {
		out = append(out, configMap[bucket])
	}
	return out, nil
}

func planBucketLifecycle(ctx context.Context, store *meta.Store, cfg meta.BucketLifecycleConfig, normalized lifecycle.Configuration, asOf time.Time, remaining int) ([]LifecyclePlanCandidate, error) {
	if remaining <= 0 {
		return nil, nil
	}
	candidates := make([]LifecyclePlanCandidate, 0)
	versionCandidates, err := planLifecycleVersions(ctx, store, cfg, normalized, asOf, remaining)
	if err != nil {
		return nil, err
	}
	candidates = append(candidates, versionCandidates...)
	remaining -= len(candidates)
	if remaining <= 0 {
		return candidates, nil
	}
	mpuCandidates, err := planLifecycleMPUs(ctx, store, cfg, normalized, asOf, remaining)
	if err != nil {
		return nil, err
	}
	candidates = append(candidates, mpuCandidates...)
	return candidates, nil
}

func planLifecycleVersions(ctx context.Context, store *meta.Store, cfg meta.BucketLifecycleConfig, normalized lifecycle.Configuration, asOf time.Time, remaining int) ([]LifecyclePlanCandidate, error) {
	versions, err := store.ListLifecycleObjectVersions(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	var out []LifecyclePlanCandidate
	for i := 0; i < len(versions) && remaining > 0; {
		key := versions[i].Key
		current := versions[i]
		j := i + 1
		for j < len(versions) && versions[j].Key == key {
			j++
		}
		cands, err := planLifecycleVersionGroup(ctx, store, cfg, normalized, current, versions[i:j], asOf, remaining)
		if err != nil {
			return nil, err
		}
		out = append(out, cands...)
		remaining -= len(cands)
		i = j
	}
	return out, nil
}

func planLifecycleVersionGroup(ctx context.Context, store *meta.Store, cfg meta.BucketLifecycleConfig, normalized lifecycle.Configuration, current meta.ObjectMeta, versions []meta.ObjectMeta, asOf time.Time, limit int) ([]LifecyclePlanCandidate, error) {
	var out []LifecyclePlanCandidate
	tagsByVersion := map[string]lifecycle.ObjectTags{}
	for idx, version := range versions {
		if len(out) >= limit {
			break
		}
		if version.State != meta.VersionStateActive {
			continue
		}
		tags, err := lifecycleTagsForVersion(ctx, store, version.VersionID, tagsByVersion)
		if err != nil {
			return nil, err
		}
		isCurrent := idx == 0 && current.VersionID == version.VersionID
		for _, rule := range normalized.Rules {
			if !lifecycle.RuleEnabled(rule) || !lifecycle.RuleMatches(rule, version.Key, tags) {
				continue
			}
			if isCurrent && rule.Expiration != nil {
				ts, ok := parseMetaTime(version.LastModified)
				if ok && lifecycle.ExpirationEligible(*rule.Expiration, ts, asOf) {
					out = append(out, LifecyclePlanCandidate{
						Action:            LifecycleActionExpireCurrent,
						Bucket:            cfg.Bucket,
						Key:               version.Key,
						VersionID:         version.VersionID,
						CurrentVersionID:  current.VersionID,
						State:             version.State,
						RuleID:            rule.ID,
						ConfigFingerprint: cfg.ConfigFingerprint,
						Timestamp:         ts,
						Size:              version.Size,
					})
					break
				}
			}
			if !isCurrent && rule.NoncurrentVersionExpiration != nil {
				ts, ok := parseMetaTime(version.LastModified)
				if ok && lifecycle.NoncurrentEligible(*rule.NoncurrentVersionExpiration, ts, asOf) {
					out = append(out, LifecyclePlanCandidate{
						Action:            LifecycleActionExpireNoncurrent,
						Bucket:            cfg.Bucket,
						Key:               version.Key,
						VersionID:         version.VersionID,
						CurrentVersionID:  current.VersionID,
						State:             version.State,
						RuleID:            rule.ID,
						ConfigFingerprint: cfg.ConfigFingerprint,
						Timestamp:         ts,
						Size:              version.Size,
					})
					break
				}
			}
		}
	}
	return out, nil
}

func planLifecycleMPUs(ctx context.Context, store *meta.Store, cfg meta.BucketLifecycleConfig, normalized lifecycle.Configuration, asOf time.Time, remaining int) ([]LifecyclePlanCandidate, error) {
	if remaining <= 0 {
		return nil, nil
	}
	var out []LifecyclePlanCandidate
	uploads, err := store.ListLifecycleMultipartUploads(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	for _, up := range uploads {
		if remaining <= 0 {
			break
		}
		ts, ok := parseMetaTime(up.CreatedAt)
		if !ok {
			continue
		}
		for _, rule := range normalized.Rules {
			if !lifecycle.RuleEnabled(rule) || rule.AbortIncompleteMultipartUpload == nil || !lifecycle.RuleMatches(rule, up.Key, nil) {
				continue
			}
			if lifecycle.MPUAbortEligible(*rule.AbortIncompleteMultipartUpload, ts, asOf) {
				_, bytes, _ := store.MultipartUploadStats(ctx, up.UploadID)
				out = append(out, LifecyclePlanCandidate{
					Action:            LifecycleActionAbortMPU,
					Bucket:            cfg.Bucket,
					Key:               up.Key,
					UploadID:          up.UploadID,
					State:             up.State,
					RuleID:            rule.ID,
					ConfigFingerprint: cfg.ConfigFingerprint,
					Timestamp:         ts,
					Size:              bytes,
				})
				remaining--
				break
			}
		}
	}
	return out, nil
}

func lifecycleTagsForVersion(ctx context.Context, store *meta.Store, versionID string, cache map[string]lifecycle.ObjectTags) (lifecycle.ObjectTags, error) {
	if tags, ok := cache[versionID]; ok {
		return tags, nil
	}
	metaTags, err := store.GetObjectTags(ctx, versionID)
	if err != nil {
		return nil, err
	}
	tags := make(lifecycle.ObjectTags, 0, len(metaTags))
	for _, tag := range metaTags {
		tags = append(tags, lifecycle.Tag{Key: tag.Key, Value: tag.Value})
	}
	cache[versionID] = tags
	return tags, nil
}

func addLifecycleCandidate(report *Report, cand LifecyclePlanCandidate) {
	if report == nil {
		return
	}
	report.CandidateBytes += cand.Size
	report.CandidateIDs = append(report.CandidateIDs, lifecycleCandidateID(cand))
	switch cand.Action {
	case LifecycleActionExpireCurrent:
		report.CurrentExpirations++
	case LifecycleActionExpireNoncurrent:
		report.NoncurrentExpirations++
	case LifecycleActionAbortMPU:
		report.MPUAborts++
	}
}

func lifecycleCandidateID(cand LifecyclePlanCandidate) string {
	switch cand.Action {
	case LifecycleActionAbortMPU:
		return strings.Join([]string{cand.Action, cand.Bucket, cand.Key, cand.UploadID}, ":")
	default:
		return strings.Join([]string{cand.Action, cand.Bucket, cand.Key, cand.VersionID}, ":")
	}
}

func parseMetaTime(raw string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, raw)
	}
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}
