package admin

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func isOpsMode(mode string) bool {
	switch mode {
	case "status", "fsck", "scrub", "snapshot", "rebuild-index", "gc-plan", "gc-run", "gc-rewrite", "gc-rewrite-plan", "gc-rewrite-run", "mpu-gc-plan", "mpu-gc-run", "support-bundle", "repl-validate", "db-integrity-check", "db-reindex":
		return true
	default:
		return false
	}
}

func requiresQuiescedOps(mode string) bool {
	switch mode {
	case "rebuild-index", "gc-run", "gc-rewrite", "gc-rewrite-run", "mpu-gc-run", "db-integrity-check", "db-reindex":
		return true
	default:
		return false
	}
}

func runOpsRequest(mode string, layout fs.Layout, metaPath, snapshotDir, replCompareDir string, fsckAllManifests, scrubAllManifests bool, gcMinAge time.Duration, gcForce bool, gcLiveThreshold float64, gcRewritePlanFile, gcRewriteFromPlan string, gcRewriteBps int64, gcPauseFile string, mpuTTL time.Duration, mpuForce bool, gcGuardrails ops.GCGuardrails, mpuGuardrails ops.MPUGCGuardrails, dbReindexTable string) (*ops.Report, error) {
	var (
		report *ops.Report
		err    error
	)
	switch mode {
	case "status":
		report, err = ops.Status(layout)
	case "fsck":
		report, err = ops.Fsck(layout, metaPath, !fsckAllManifests)
	case "scrub":
		report, err = ops.Scrub(layout, metaPath, !scrubAllManifests)
	case "snapshot":
		if snapshotDir == "" {
			snapshotDir = filepath.Join(filepath.Dir(layout.Root), "snapshots", "snapshot-"+fmtTime())
		}
		report, err = ops.Snapshot(layout, metaPath, snapshotDir)
	case "rebuild-index":
		report, err = ops.Rebuild(layout, metaPath)
	case "repl-validate":
		report, err = ops.ReplValidate(layout, metaPath, replCompareDir)
	case "gc-plan":
		var candidates []meta.Segment
		report, candidates, err = ops.GCPlan(layout, metaPath, gcMinAge, gcGuardrails)
		if err == nil {
			report.Candidates = len(candidates)
			report.CandidateIDs = nil
			for _, seg := range candidates {
				report.CandidateIDs = append(report.CandidateIDs, seg.ID)
			}
		}
	case "gc-run":
		report, err = ops.GCRun(layout, metaPath, gcMinAge, gcForce, gcGuardrails)
	case "gc-rewrite":
		report, err = ops.GCRewrite(layout, metaPath, gcMinAge, gcLiveThreshold, gcForce, gcRewriteBps, gcPauseFile)
	case "gc-rewrite-plan":
		var plan *ops.GCRewritePlan
		plan, report, err = ops.GCRewritePlanBuild(layout, metaPath, gcMinAge, gcLiveThreshold)
		if err == nil && gcRewritePlanFile != "" {
			if err := ops.WriteGCRewritePlan(gcRewritePlanFile, plan); err != nil {
				return nil, err
			}
		}
	case "gc-rewrite-run":
		if gcRewriteFromPlan == "" {
			return nil, fmt.Errorf("gc-rewrite-run requires gc_rewrite_from_plan")
		}
		var plan *ops.GCRewritePlan
		plan, err = ops.ReadGCRewritePlan(gcRewriteFromPlan)
		if err == nil {
			report, err = ops.GCRewriteFromPlan(layout, metaPath, plan, gcForce, gcRewriteBps, gcPauseFile)
		}
	case "mpu-gc-plan":
		var uploads []meta.MultipartUpload
		report, uploads, err = ops.MPUGCPlan(metaPath, mpuTTL, mpuGuardrails)
		if err == nil {
			report.Candidates = len(uploads)
			report.CandidateIDs = nil
			for _, up := range uploads {
				report.CandidateIDs = append(report.CandidateIDs, up.UploadID)
			}
		}
	case "mpu-gc-run":
		report, err = ops.MPUGCRun(metaPath, mpuTTL, mpuForce, mpuGuardrails)
	case "support-bundle":
		if snapshotDir == "" {
			snapshotDir = filepath.Join(filepath.Dir(layout.Root), "support", "bundle-"+fmtTime())
		}
		report, err = ops.SupportBundle(layout, metaPath, snapshotDir)
	case "db-integrity-check":
		report, err = ops.DBIntegrityCheck(metaPath)
	case "db-reindex":
		report, err = ops.DBReindex(metaPath, dbReindexTable)
	default:
		return nil, fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		return nil, err
	}
	return report, nil
}

func fmtTime() string {
	return fmt.Sprintf("%d", time.Now().UTC().Unix())
}
