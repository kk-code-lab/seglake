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

func runOps(mode, dataDir, metaPath, snapshotDir string, gcMinAge time.Duration, gcForce bool, jsonOut bool) error {
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
		report, candidates, err = ops.GCPlan(layout, metaPath, gcMinAge)
		if err == nil {
			report.Candidates = len(candidates)
			report.CandidateIDs = nil
			for _, seg := range candidates {
				report.CandidateIDs = append(report.CandidateIDs, seg.ID)
			}
		}
	case "gc-run":
		report, err = ops.GCRun(layout, metaPath, gcMinAge, gcForce)
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
		fmt.Println("Flags: -addr, -data-dir, -access-key, -secret-key, -region")
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
		fmt.Println("Flags: -gc-min-age (default 24h).")
	case "gc-run":
		fmt.Println("Mode gc-run: deletes 100% dead segments.")
		fmt.Println("Flags: -gc-min-age, -gc-force (required).")
	case "support-bundle":
		fmt.Println("Mode support-bundle: creates snapshot + fsck/scrub reports.")
		fmt.Println("Flags: -snapshot-dir (output directory).")
	default:
		fmt.Printf("Unknown mode %q\n", mode)
	}
}
