package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	showVersionShort := flag.Bool("v", false, "Print version and exit (shorthand)")
	addr := flag.String("addr", ":9000", "HTTP listen address")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	accessKey := flag.String("access-key", "", "S3 access key (enables SigV4)")
	secretKey := flag.String("secret-key", "", "S3 secret key (enables SigV4)")
	region := flag.String("region", "us-east-1", "S3 region")
	virtualHosted := flag.Bool("virtual-hosted", false, "Enable virtual-hosted-style bucket routing")
	logRequests := flag.Bool("log-requests", true, "Log HTTP requests")
	mode := flag.String("mode", "server", "Mode: server|fsck|scrub|snapshot|status|rebuild-index|gc-plan|gc-run|gc-rewrite|gc-rewrite-plan|gc-rewrite-run|mpu-gc-plan|mpu-gc-run|support-bundle")
	snapshotDir := flag.String("snapshot-dir", "", "Snapshot output directory")
	rebuildMeta := flag.String("rebuild-meta", "", "Path to meta.db for rebuild-index")
	gcMinAge := flag.Duration("gc-min-age", 24*time.Hour, "GC minimum segment age")
	gcForce := flag.Bool("gc-force", false, "GC delete segments (required for gc-run)")
	gcLiveThreshold := flag.Float64("gc-live-threshold", 0.5, "GC rewrite live-bytes ratio threshold (<= value)")
	gcRewritePlanFile := flag.String("gc-rewrite-plan", "", "GC rewrite plan output file")
	gcRewriteFromPlan := flag.String("gc-rewrite-from-plan", "", "GC rewrite plan input file")
	gcRewriteBps := flag.Int64("gc-rewrite-bps", 0, "GC rewrite max bytes per second (0 = unlimited)")
	gcPauseFile := flag.String("gc-pause-file", "", "GC pause while file exists")
	syncInterval := flag.Duration("sync-interval", 100*time.Millisecond, "Write barrier interval")
	syncBytes := flag.Int64("sync-bytes", 128<<20, "Write barrier byte threshold")
	mpuTTL := flag.Duration("mpu-ttl", 7*24*time.Hour, "Multipart upload TTL for cleanup")
	mpuForce := flag.Bool("mpu-force", false, "Multipart GC delete uploads (required for mpu-gc-run)")
	jsonOut := flag.Bool("json", false, "Output ops report as JSON")
	showModeHelp := flag.Bool("mode-help", false, "Show help for the selected mode")
	flag.Parse()

	if *showVersion || *showVersionShort {
		fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
		return
	}
	if *showModeHelp {
		printModeHelp(*mode)
		return
	}

	if flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "unknown arguments:", flag.Args())
		os.Exit(2)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "data dir error: %v\n", err)
		os.Exit(1)
	}

	metaPath := filepath.Join(*dataDir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "meta open error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:          fs.NewLayout(filepath.Join(*dataDir, "objects")),
		MetaStore:       store,
		BarrierInterval: *syncInterval,
		BarrierMaxBytes: *syncBytes,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "engine init error: %v\n", err)
		os.Exit(1)
	}

	if *mode != "server" {
		metaArg := metaPath
		if *rebuildMeta != "" {
			metaArg = *rebuildMeta
		}
		if err := runOps(*mode, *dataDir, metaArg, *snapshotDir, *gcMinAge, *gcForce, *gcLiveThreshold, *gcRewritePlanFile, *gcRewriteFromPlan, *gcRewriteBps, *gcPauseFile, *mpuTTL, *mpuForce, *jsonOut); err != nil {
			fmt.Fprintf(os.Stderr, "ops error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
	var handler http.Handler = &s3.Handler{
		Engine: eng,
		Meta:   store,
		Auth: &s3.AuthConfig{
			AccessKey: *accessKey,
			SecretKey: *secretKey,
			Region:    *region,
			MaxSkew:   5 * time.Minute,
		},
		Metrics:       s3.NewMetrics(),
		VirtualHosted: *virtualHosted,
	}
	if *logRequests {
		handler = s3.LoggingMiddleware(handler)
	}
	if err := http.ListenAndServe(*addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
		os.Exit(1)
	}
}
