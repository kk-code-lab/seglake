package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

type opsRunRequest struct {
	Mode              string  `json:"mode"`
	SnapshotDir       string  `json:"snapshot_dir,omitempty"`
	RebuildMeta       string  `json:"rebuild_meta,omitempty"`
	ReplCompareDir    string  `json:"repl_compare_dir,omitempty"`
	DBReindexTable    string  `json:"db_reindex_table,omitempty"`
	FsckAllManifests  bool    `json:"fsck_all_manifests,omitempty"`
	ScrubAllManifests bool    `json:"scrub_all_manifests,omitempty"`
	GCMinAgeNanos     int64   `json:"gc_min_age_nanos,omitempty"`
	GCForce           bool    `json:"gc_force,omitempty"`
	GCWarnSegments    int     `json:"gc_warn_segments,omitempty"`
	GCWarnReclaim     int64   `json:"gc_warn_reclaim_bytes,omitempty"`
	GCMaxSegments     int     `json:"gc_max_segments,omitempty"`
	GCMaxReclaim      int64   `json:"gc_max_reclaim_bytes,omitempty"`
	GCLiveThreshold   float64 `json:"gc_live_threshold,omitempty"`
	GCRewritePlanFile string  `json:"gc_rewrite_plan,omitempty"`
	GCRewriteFromPlan string  `json:"gc_rewrite_from_plan,omitempty"`
	GCRewriteBps      int64   `json:"gc_rewrite_bps,omitempty"`
	GCPauseFile       string  `json:"gc_pause_file,omitempty"`
	MPUTTLNanos       int64   `json:"mpu_ttl_nanos,omitempty"`
	MPUForce          bool    `json:"mpu_force,omitempty"`
	MPUWarnUploads    int     `json:"mpu_warn_uploads,omitempty"`
	MPUWarnReclaim    int64   `json:"mpu_warn_reclaim_bytes,omitempty"`
	MPUMaxUploads     int     `json:"mpu_max_uploads,omitempty"`
	MPUMaxReclaim     int64   `json:"mpu_max_reclaim_bytes,omitempty"`
}

func (h *Handler) handleOpsRun(ctx context.Context, w http.ResponseWriter, r *http.Request, requestID string) {
	if h == nil || h.Meta == nil || h.Engine == nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "storage not initialized", requestID, r.URL.Path)
		return
	}
	if maintenanceStateFromContext(ctx) != "quiesced" {
		writeErrorWithResource(w, http.StatusServiceUnavailable, "ServiceUnavailable", "maintenance mode not quiesced", requestID, r.URL.Path)
		return
	}
	var req opsRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidRequest", "invalid json body", requestID, r.URL.Path)
		return
	}
	if !isOpsMode(req.Mode) {
		writeErrorWithResource(w, http.StatusBadRequest, "InvalidArgument", "unknown ops mode", requestID, r.URL.Path)
		return
	}
	dataDir := h.DataDir
	if dataDir == "" {
		dataDir = filepath.Dir(h.Engine.Layout().Root)
	}
	metaPath := filepath.Join(dataDir, "meta.db")
	if req.RebuildMeta != "" {
		metaPath = req.RebuildMeta
	}
	layout := fs.NewLayout(filepath.Join(dataDir, "objects"))
	gcGuard := ops.GCGuardrails{
		WarnCandidates:     req.GCWarnSegments,
		WarnReclaimedBytes: req.GCWarnReclaim,
		MaxCandidates:      req.GCMaxSegments,
		MaxReclaimedBytes:  req.GCMaxReclaim,
	}
	mpuGuard := ops.MPUGCGuardrails{
		WarnUploads:        req.MPUWarnUploads,
		WarnReclaimedBytes: req.MPUWarnReclaim,
		MaxUploads:         req.MPUMaxUploads,
		MaxReclaimedBytes:  req.MPUMaxReclaim,
	}
	gcMinAge := time.Duration(req.GCMinAgeNanos)
	mpuTTL := time.Duration(req.MPUTTLNanos)
	report, err := runOpsRequest(req.Mode, layout, metaPath, req.SnapshotDir, req.ReplCompareDir, req.FsckAllManifests, req.ScrubAllManifests, gcMinAge, req.GCForce, req.GCLiveThreshold, req.GCRewritePlanFile, req.GCRewriteFromPlan, req.GCRewriteBps, req.GCPauseFile, mpuTTL, req.MPUForce, gcGuard, mpuGuard, req.DBReindexTable)
	if err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", err.Error(), requestID, r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		writeErrorWithResource(w, http.StatusInternalServerError, "InternalError", "failed to encode report", requestID, r.URL.Path)
		return
	}
}

func isOpsMode(mode string) bool {
	switch mode {
	case "status", "fsck", "scrub", "snapshot", "rebuild-index", "gc-plan", "gc-run", "gc-rewrite", "gc-rewrite-plan", "gc-rewrite-run", "mpu-gc-plan", "mpu-gc-run", "support-bundle", "repl-validate", "db-integrity-check", "db-reindex":
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
