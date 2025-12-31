package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

type opsRunRequest struct {
	Mode              string  `json:"mode"`
	SnapshotDir       string  `json:"snapshot_dir,omitempty"`
	RebuildMeta       string  `json:"rebuild_meta,omitempty"`
	ReplCompareDir    string  `json:"repl_compare_dir,omitempty"`
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

func runOpsWithMode(mode string, opts *opsOptions) error {
	if opts == nil {
		return fmt.Errorf("ops options required")
	}
	remoteURL := ""
	if isOpsRemoteEligible(mode, opts.dataDir) {
		if opts.opsURL != "" {
			remoteURL = normalizeOpsURL(opts.opsURL)
		} else if addr, ok := heartbeatAddr(opts.dataDir); ok {
			remoteURL = normalizeOpsURL(addr)
		}
	}
	if remoteURL != "" {
		req := opsRunRequest{
			Mode:              mode,
			SnapshotDir:       opts.snapshotDir,
			RebuildMeta:       opts.rebuildMeta,
			ReplCompareDir:    opts.replCompareDir,
			GCMinAgeNanos:     int64(opts.gcMinAge),
			GCForce:           opts.gcForce,
			GCWarnSegments:    opts.gcWarnSegments,
			GCWarnReclaim:     opts.gcWarnReclaim,
			GCMaxSegments:     opts.gcMaxSegments,
			GCMaxReclaim:      opts.gcMaxReclaim,
			GCLiveThreshold:   opts.gcLiveThreshold,
			GCRewritePlanFile: opts.gcRewritePlanFile,
			GCRewriteFromPlan: opts.gcRewriteFromPlan,
			GCRewriteBps:      opts.gcRewriteBps,
			GCPauseFile:       opts.gcPauseFile,
			MPUTTLNanos:       int64(opts.mpuTTL),
			MPUForce:          opts.mpuForce,
			MPUWarnUploads:    opts.mpuWarnUploads,
			MPUWarnReclaim:    opts.mpuWarnReclaim,
			MPUMaxUploads:     opts.mpuMaxUploads,
			MPUMaxReclaim:     opts.mpuMaxReclaim,
		}
		return runOpsRemote(remoteURL, req, opts.jsonOut, opts.opsAccessKey, opts.opsSecretKey, opts.opsRegion)
	}
	metaPath := resolveMetaPath(opts.dataDir, opts.rebuildMeta)
	gcGuard := ops.GCGuardrails{
		WarnCandidates:     opts.gcWarnSegments,
		WarnReclaimedBytes: opts.gcWarnReclaim,
		MaxCandidates:      opts.gcMaxSegments,
		MaxReclaimedBytes:  opts.gcMaxReclaim,
	}
	mpuGuard := ops.MPUGCGuardrails{
		WarnUploads:        opts.mpuWarnUploads,
		WarnReclaimedBytes: opts.mpuWarnReclaim,
		MaxUploads:         opts.mpuMaxUploads,
		MaxReclaimedBytes:  opts.mpuMaxReclaim,
	}
	return runOps(mode, opts.dataDir, metaPath, opts.snapshotDir, opts.replCompareDir, opts.gcMinAge, opts.gcForce, opts.gcLiveThreshold, opts.gcRewritePlanFile, opts.gcRewriteFromPlan, opts.gcRewriteBps, opts.gcPauseFile, opts.mpuTTL, opts.mpuForce, gcGuard, mpuGuard, opts.jsonOut)
}

func runOpsRemote(url string, req opsRunRequest, jsonOut bool, accessKey, secretKey, region string) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if accessKey != "" || secretKey != "" {
		if region == "" {
			region = "us-east-1"
		}
		auth := &s3.AuthConfig{AccessKey: accessKey, SecretKey: secretKey, Region: region}
		if signed, err := auth.Presign(http.MethodPost, url, 5*time.Minute); err == nil {
			url = signed
		} else {
			return err
		}
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		if len(body) > 0 {
			return fmt.Errorf("ops remote failed: %s", strings.TrimSpace(string(body)))
		}
		return fmt.Errorf("ops remote failed: %s", resp.Status)
	}
	var report ops.Report
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return err
	}
	if jsonOut {
		return writeJSON(&report)
	}
	fmt.Printf("%s\n", formatReport(&report))
	return nil
}

func isOpsRemoteEligible(mode, dataDir string) bool {
	if dataDir == "" {
		return false
	}
	state, err := maintenanceState(dataDir)
	if err != nil || state != "quiesced" {
		return false
	}
	_, ok := heartbeatAddr(dataDir)
	return ok
}

func heartbeatAddr(dataDir string) (string, bool) {
	status, err := readHeartbeatStatus(dataDir)
	if err != nil || !status.Fresh {
		return "", false
	}
	if status.HasData && status.Data.Addr != "" {
		return status.Data.Addr, true
	}
	return "", false
}

func normalizeOpsURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		if strings.HasPrefix(addr, ":") {
			addr = "http://127.0.0.1" + addr
		} else {
			addr = "http://" + addr
		}
	}
	addr = strings.TrimSuffix(addr, "/")
	return addr + "/v1/ops/run"
}

func runOps(mode, dataDir, metaPath, snapshotDir, replCompareDir string, gcMinAge time.Duration, gcForce bool, gcLiveThreshold float64, gcRewritePlanFile, gcRewriteFromPlan string, gcRewriteBps int64, gcPauseFile string, mpuTTL time.Duration, mpuForce bool, gcGuardrails ops.GCGuardrails, mpuGuardrails ops.MPUGCGuardrails, jsonOut bool) error {
	layout := fs.NewLayout(filepath.Join(dataDir, "objects"))
	var (
		report *ops.Report
		err    error
	)
	switch mode {
	case "status":
		report, err = ops.Status(layout)
	case "fsck":
		report, err = ops.Fsck(layout)
	case "scrub":
		report, err = ops.Scrub(layout, metaPath)
	case "snapshot":
		if snapshotDir == "" {
			snapshotDir = filepath.Join(dataDir, "snapshots", "snapshot-"+fmtTime())
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
				return err
			}
		}
	case "gc-rewrite-run":
		if gcRewriteFromPlan == "" {
			return fmt.Errorf("gc-rewrite-run requires -gc-rewrite-from-plan")
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
			snapshotDir = filepath.Join(dataDir, "support", "bundle-"+fmtTime())
		}
		report, err = ops.SupportBundle(layout, metaPath, snapshotDir)
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		return err
	}
	if jsonOut {
		return writeJSONReport(report)
	}
	fmt.Printf("%s\n", formatReport(report))
	return nil
}

func fmtTime() string {
	return fmt.Sprintf("%d", time.Now().UTC().Unix())
}

func formatReport(report *ops.Report) string {
	if report == nil {
		return ""
	}
	if report.Mode == "repl-validate" {
		return fmt.Sprintf("mode=%s local_manifests=%d remote_manifests=%d local_live=%d remote_live=%d errors=%d",
			report.Mode,
			report.CompareManifestsLocal,
			report.CompareManifestsRemote,
			report.CompareLiveLocal,
			report.CompareLiveRemote,
			report.Errors,
		)
	}
	if report.Warnings > 0 {
		return fmt.Sprintf("mode=%s manifests=%d segments=%d errors=%d warnings=%d", report.Mode, report.Manifests, report.Segments, report.Errors, report.Warnings)
	}
	return fmt.Sprintf("mode=%s manifests=%d segments=%d errors=%d", report.Mode, report.Manifests, report.Segments, report.Errors)
}

func writeJSONReport(report *ops.Report) error {
	if report == nil {
		return nil
	}
	return writeJSON(report)
}

func printModeHelp(mode string, fs *flag.FlagSet) {
	switch mode {
	case "server":
		fmt.Println("Mode server: run HTTP API server.")
	case "status":
		fmt.Println("Mode status: counts manifests and segments.")
	case "fsck":
		fmt.Println("Mode fsck: validates segment headers/footers and chunk bounds.")
	case "scrub":
		fmt.Println("Mode scrub: verifies chunk hashes against stored data.")
	case "snapshot":
		fmt.Println("Mode snapshot: copies meta.db (+wal/shm) and writes snapshot.json.")
	case "rebuild-index":
		fmt.Println("Mode rebuild-index: rebuilds metadata DB from manifests.")
	case "gc-plan":
		fmt.Println("Mode gc-plan: prints segments eligible for removal.")
	case "gc-run":
		fmt.Println("Mode gc-run: deletes 100% dead segments.")
	case "gc-rewrite":
		fmt.Println("Mode gc-rewrite: rewrites partially-dead sealed segments.")
	case "gc-rewrite-plan":
		fmt.Println("Mode gc-rewrite-plan: writes rewrite plan for partially-dead segments.")
	case "gc-rewrite-run":
		fmt.Println("Mode gc-rewrite-run: executes rewrite from plan.")
	case "mpu-gc-plan":
		fmt.Println("Mode mpu-gc-plan: lists multipart uploads eligible for cleanup.")
	case "mpu-gc-run":
		fmt.Println("Mode mpu-gc-run: deletes stale multipart uploads and parts.")
	case "support-bundle":
		fmt.Println("Mode support-bundle: creates snapshot + fsck/scrub reports.")
	case "keys":
		fmt.Println("Mode keys: manage API keys and bucket allowlists.")
	case "bucket-policy":
		fmt.Println("Mode bucket-policy: manage bucket policies.")
	case "buckets":
		fmt.Println("Mode buckets: manage bucket entries (admin).")
	case "maintenance":
		fmt.Println("Mode maintenance: toggle read-only maintenance mode.")
	case "repl-pull":
		fmt.Println("Mode repl-pull: pull oplog from remote and apply locally.")
	case "repl-push":
		fmt.Println("Mode repl-push: push local oplog to remote.")
	case "repl-validate":
		fmt.Println("Mode repl-validate: compare manifests and live versions between data dirs.")
	case "repl-bootstrap":
		fmt.Println("Mode repl-bootstrap: download snapshot and catch up oplog.")
	default:
		fmt.Printf("Unknown mode %q\n", mode)
		return
	}
	if fs == nil {
		return
	}
	fmt.Println("Flags:")
	fs.SetOutput(os.Stdout)
	fs.PrintDefaults()
}
