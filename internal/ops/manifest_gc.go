package ops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

const manifestGCPlanSchemaVersion = 1

type ManifestGCPlan struct {
	SchemaVersion  int                   `json:"schema_version"`
	GeneratedAt    time.Time             `json:"generated_at"`
	TTL            time.Duration         `json:"ttl"`
	Candidates     []ManifestGCCandidate `json:"candidates"`
	CandidateBytes int64                 `json:"candidate_bytes"`
}

type ManifestGCCandidate struct {
	Path              string    `json:"path"`
	Size              int64     `json:"size"`
	ModTime           time.Time `json:"mod_time"`
	FingerprintSHA256 string    `json:"fingerprint_sha256"`
}

func ManifestGCPlanBuild(layout fs.Layout, metaPath string, ttl time.Duration) (*ManifestGCPlan, *Report, error) {
	if ttl < 0 {
		return nil, nil, fmt.Errorf("manifest-gc: ttl must be non-negative")
	}
	report := newReport("manifest-gc-plan")
	allPaths, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, nil, err
	}
	report.Manifests = len(allPaths)
	liveSet, liveCount, err := manifestGCLiveSet(metaPath)
	if err != nil {
		return nil, nil, err
	}
	report.LiveManifests = liveCount
	cutoffNow := now()
	candidates := make([]ManifestGCCandidate, 0)
	for _, path := range allPaths {
		if _, ok := liveSet[path]; ok {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, err.Error())
			}
			continue
		}
		if info.IsDir() || cutoffNow.Sub(info.ModTime()) < ttl {
			continue
		}
		fp, err := fileSHA256(path)
		if err != nil {
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, err.Error())
			}
			continue
		}
		cand := ManifestGCCandidate{
			Path:              path,
			Size:              info.Size(),
			ModTime:           info.ModTime().UTC(),
			FingerprintSHA256: fp,
		}
		candidates = append(candidates, cand)
		report.CandidateBytes += cand.Size
	}
	report.Candidates = len(candidates)
	report.FinishedAt = now().UTC()
	_ = recordOpsRun(metaPath, report)
	return &ManifestGCPlan{
		SchemaVersion:  manifestGCPlanSchemaVersion,
		GeneratedAt:    now().UTC(),
		TTL:            ttl,
		Candidates:     candidates,
		CandidateBytes: report.CandidateBytes,
	}, report, nil
}

func ManifestGCRun(layout fs.Layout, metaPath string, plan *ManifestGCPlan, force bool) (*Report, error) {
	if !force {
		return nil, fmt.Errorf("manifest-gc: refuse to run without --force")
	}
	if plan == nil {
		return nil, fmt.Errorf("manifest-gc: plan required")
	}
	if plan.SchemaVersion != manifestGCPlanSchemaVersion {
		return nil, fmt.Errorf("manifest-gc: unsupported plan schema %d", plan.SchemaVersion)
	}
	report := newReport("manifest-gc-run")
	report.Candidates = len(plan.Candidates)
	report.CandidateBytes = plan.CandidateBytes
	liveSet, liveCount, err := manifestGCLiveSet(metaPath)
	if err != nil {
		return nil, err
	}
	report.LiveManifests = liveCount
	for _, cand := range plan.Candidates {
		if _, ok := liveSet[cand.Path]; ok {
			report.SkippedManifests++
			continue
		}
		info, err := os.Stat(cand.Path)
		if err != nil {
			if os.IsNotExist(err) {
				report.SkippedManifests++
				continue
			}
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, err.Error())
			}
			continue
		}
		if info.IsDir() || info.Size() != cand.Size || !info.ModTime().UTC().Equal(cand.ModTime.UTC()) {
			report.SkippedManifests++
			continue
		}
		fp, err := fileSHA256(cand.Path)
		if err != nil {
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, err.Error())
			}
			continue
		}
		if fp != cand.FingerprintSHA256 {
			report.SkippedManifests++
			continue
		}
		if err := os.Remove(cand.Path); err != nil {
			report.Errors++
			if len(report.ErrorSample) < 5 {
				report.ErrorSample = append(report.ErrorSample, err.Error())
			}
			continue
		}
		report.Deleted++
		report.Reclaimed += cand.Size
	}
	report.FinishedAt = now().UTC()
	_ = recordOpsRun(metaPath, report)
	return report, nil
}

func WriteManifestGCPlan(path string, plan *ManifestGCPlan) error {
	if path == "" || plan == nil {
		return fmt.Errorf("manifest-gc: plan and path required")
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func ReadManifestGCPlan(path string) (*ManifestGCPlan, error) {
	if path == "" {
		return nil, fmt.Errorf("manifest-gc: plan path required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan ManifestGCPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func manifestGCLiveSet(metaPath string) (map[string]struct{}, int, error) {
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = store.Close() }()
	livePaths, err := store.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, 0, err
	}
	mpuPaths, err := store.ListMultipartPartManifestPaths(context.Background())
	if err != nil {
		return nil, 0, err
	}
	livePaths = mergeUniquePaths(livePaths, mpuPaths)
	out := make(map[string]struct{}, len(livePaths))
	for _, path := range livePaths {
		out[path] = struct{}{}
	}
	return out, len(livePaths), nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func recordOpsRun(metaPath string, report *Report) error {
	store, err := meta.Open(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	return store.RecordOpsRun(context.Background(), report.Mode, reportOpsFrom(report))
}
