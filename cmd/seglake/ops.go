package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func runOps(mode, dataDir, metaPath, snapshotDir string, gcMinAge time.Duration, gcForce bool, gcLiveThreshold float64, gcRewritePlanFile, gcRewriteFromPlan string, gcRewriteBps int64, gcPauseFile string, mpuTTL time.Duration, mpuForce bool, gcGuardrails ops.GCGuardrails, mpuGuardrails ops.MPUGCGuardrails, jsonOut bool) error {
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
	if report.Warnings > 0 {
		return fmt.Sprintf("mode=%s manifests=%d segments=%d errors=%d warnings=%d", report.Mode, report.Manifests, report.Segments, report.Errors, report.Warnings)
	}
	return fmt.Sprintf("mode=%s manifests=%d segments=%d errors=%d", report.Mode, report.Manifests, report.Segments, report.Errors)
}

func writeJSONReport(report *ops.Report) error {
	if report == nil {
		return nil
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func printModeHelp(mode string) {
	switch mode {
	case "server":
		fmt.Println("Mode server: run HTTP API server.")
		fmt.Println("Flags: -addr, -data-dir, -access-key, -secret-key, -region, -sync-interval, -sync-bytes")
	case "status":
		fmt.Println("Mode status: counts manifests and segments.")
	case "fsck":
		fmt.Println("Mode fsck: validates segment headers/footers and chunk bounds.")
	case "scrub":
		fmt.Println("Mode scrub: verifies chunk hashes against stored data.")
	case "snapshot":
		fmt.Println("Mode snapshot: copies meta.db (+wal/shm) and writes snapshot.json.")
		fmt.Println("Flags: -snapshot-dir (optional, default under data/snapshots).")
	case "rebuild-index":
		fmt.Println("Mode rebuild-index: rebuilds metadata DB from manifests.")
		fmt.Println("Flags: -rebuild-meta (optional path to target meta.db).")
	case "gc-plan":
		fmt.Println("Mode gc-plan: prints segments eligible for removal.")
		fmt.Println("Flags: -gc-min-age (default 24h), -gc-warn-segments, -gc-warn-reclaim-bytes, -gc-max-segments, -gc-max-reclaim-bytes.")
	case "gc-run":
		fmt.Println("Mode gc-run: deletes 100% dead segments.")
		fmt.Println("Flags: -gc-min-age, -gc-force (required), -gc-warn-segments, -gc-warn-reclaim-bytes, -gc-max-segments, -gc-max-reclaim-bytes.")
	case "gc-rewrite":
		fmt.Println("Mode gc-rewrite: rewrites partially-dead sealed segments.")
		fmt.Println("Flags: -gc-min-age, -gc-live-threshold (default 0.5), -gc-force (required), -gc-rewrite-bps, -gc-pause-file.")
	case "gc-rewrite-plan":
		fmt.Println("Mode gc-rewrite-plan: writes rewrite plan for partially-dead segments.")
		fmt.Println("Flags: -gc-min-age, -gc-live-threshold, -gc-rewrite-plan (output file).")
	case "gc-rewrite-run":
		fmt.Println("Mode gc-rewrite-run: executes rewrite from plan.")
		fmt.Println("Flags: -gc-rewrite-from-plan, -gc-force (required), -gc-rewrite-bps, -gc-pause-file.")
	case "mpu-gc-plan":
		fmt.Println("Mode mpu-gc-plan: lists multipart uploads eligible for cleanup.")
		fmt.Println("Flags: -mpu-ttl (default 7d), -mpu-warn-uploads, -mpu-warn-reclaim-bytes, -mpu-max-uploads, -mpu-max-reclaim-bytes.")
	case "mpu-gc-run":
		fmt.Println("Mode mpu-gc-run: deletes stale multipart uploads and parts.")
		fmt.Println("Flags: -mpu-ttl, -mpu-force (required), -mpu-warn-uploads, -mpu-warn-reclaim-bytes, -mpu-max-uploads, -mpu-max-reclaim-bytes.")
	case "support-bundle":
		fmt.Println("Mode support-bundle: creates snapshot + fsck/scrub reports.")
		fmt.Println("Flags: -snapshot-dir (output directory).")
	case "keys":
		fmt.Println("Mode keys: manage API keys and bucket allowlists.")
		fmt.Println("Flags: -keys-action list|create|allow-bucket|disallow-bucket|list-buckets|enable|disable|delete|set-policy, -key-access, -key-secret, -key-policy, -key-enabled, -key-inflight, -key-bucket, -json.")
	case "bucket-policy":
		fmt.Println("Mode bucket-policy: manage bucket policies.")
		fmt.Println("Flags: -bucket-policy-action get|set|delete, -bucket-policy-bucket, -bucket-policy, -bucket-policy-file, -json.")
	case "repl-pull":
		fmt.Println("Mode repl-pull: pull oplog from remote and apply locally.")
		fmt.Println("Flags: -repl-remote, -repl-since, -repl-limit, -repl-fetch-data, -repl-watch, -repl-interval, -repl-backoff-max, -repl-retry-timeout, -repl-access-key, -repl-secret-key, -repl-region.")
	case "repl-push":
		fmt.Println("Mode repl-push: push local oplog to remote.")
		fmt.Println("Flags: -repl-remote, -repl-push-since, -repl-push-limit, -repl-push-watch, -repl-push-interval, -repl-push-backoff-max, -repl-access-key, -repl-secret-key, -repl-region.")
	default:
		fmt.Printf("Unknown mode %q\n", mode)
	}
}
