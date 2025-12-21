package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func runOps(mode, dataDir, metaPath, snapshotDir string) error {
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
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		return err
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
	default:
		fmt.Printf("Unknown mode %q\n", mode)
	}
}
