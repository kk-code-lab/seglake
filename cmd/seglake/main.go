package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func splitComma(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	showVersionShort := flag.Bool("v", false, "Print version and exit (shorthand)")
	addr := flag.String("addr", ":9000", "HTTP listen address")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	accessKey := flag.String("access-key", "", "S3 access key (enables SigV4)")
	secretKey := flag.String("secret-key", "", "S3 secret key (enables SigV4)")
	region := flag.String("region", "us-east-1", "S3 region")
	virtualHosted := flag.Bool("virtual-hosted", true, "Enable virtual-hosted-style bucket routing")
	logRequests := flag.Bool("log-requests", true, "Log HTTP requests")
	mode := flag.String("mode", "server", "Mode: server|fsck|scrub|snapshot|status|rebuild-index|gc-plan|gc-run|gc-rewrite|gc-rewrite-plan|gc-rewrite-run|mpu-gc-plan|mpu-gc-run|support-bundle|keys|bucket-policy|repl-pull")
	tlsEnable := flag.Bool("tls", false, "Enable HTTPS listener with TLS")
	tlsCert := flag.String("tls-cert", "", "TLS certificate path (PEM)")
	tlsKey := flag.String("tls-key", "", "TLS private key path (PEM)")
	trustedProxies := flag.String("trusted-proxies", "", "Comma-separated CIDR ranges trusted for X-Forwarded-For")
	siteID := flag.String("site-id", "local", "Site identifier for replication (HLC/oplog)")
	snapshotDir := flag.String("snapshot-dir", "", "Snapshot output directory")
	rebuildMeta := flag.String("rebuild-meta", "", "Path to meta.db for rebuild-index")
	gcMinAge := flag.Duration("gc-min-age", 24*time.Hour, "GC minimum segment age")
	gcForce := flag.Bool("gc-force", false, "GC delete segments (required for gc-run)")
	gcWarnSegments := flag.Int("gc-warn-segments", 100, "GC warn when candidates exceed this count (0 disables)")
	gcWarnReclaim := flag.Int64("gc-warn-reclaim-bytes", 100<<30, "GC warn when candidate bytes exceed this count (0 disables)")
	gcMaxSegments := flag.Int("gc-max-segments", 0, "GC hard limit on candidates (0 disables)")
	gcMaxReclaim := flag.Int64("gc-max-reclaim-bytes", 0, "GC hard limit on candidate bytes (0 disables)")
	gcLiveThreshold := flag.Float64("gc-live-threshold", 0.5, "GC rewrite live-bytes ratio threshold (<= value)")
	gcRewritePlanFile := flag.String("gc-rewrite-plan", "", "GC rewrite plan output file")
	gcRewriteFromPlan := flag.String("gc-rewrite-from-plan", "", "GC rewrite plan input file")
	gcRewriteBps := flag.Int64("gc-rewrite-bps", 0, "GC rewrite max bytes per second (0 = unlimited)")
	gcPauseFile := flag.String("gc-pause-file", "", "GC pause while file exists")
	syncInterval := flag.Duration("sync-interval", 100*time.Millisecond, "Write barrier interval")
	syncBytes := flag.Int64("sync-bytes", 128<<20, "Write barrier byte threshold")
	mpuTTL := flag.Duration("mpu-ttl", 7*24*time.Hour, "Multipart upload TTL for cleanup")
	mpuForce := flag.Bool("mpu-force", false, "Multipart GC delete uploads (required for mpu-gc-run)")
	mpuWarnUploads := flag.Int("mpu-warn-uploads", 1000, "MPU GC warn when uploads exceed this count (0 disables)")
	mpuWarnReclaim := flag.Int64("mpu-warn-reclaim-bytes", 10<<30, "MPU GC warn when candidate bytes exceed this count (0 disables)")
	mpuMaxUploads := flag.Int("mpu-max-uploads", 0, "MPU GC hard limit on uploads (0 disables)")
	mpuMaxReclaim := flag.Int64("mpu-max-reclaim-bytes", 0, "MPU GC hard limit on candidate bytes (0 disables)")
	keysAction := flag.String("keys-action", "list", "Keys action: list|create|allow-bucket|disallow-bucket|list-buckets|enable|disable|delete|set-policy")
	keyAccess := flag.String("key-access", "", "API access key for keys-action")
	keySecret := flag.String("key-secret", "", "API secret key for keys-action")
	keyPolicy := flag.String("key-policy", "rw", "API key policy: rw|ro|read-only")
	keyEnabled := flag.Bool("key-enabled", true, "API key enabled flag")
	keyInflight := flag.Int64("key-inflight", 0, "API key inflight limit (0=default)")
	keyBucket := flag.String("key-bucket", "", "Bucket name for keys-action allow-bucket")
	bucketPolicyAction := flag.String("bucket-policy-action", "get", "Bucket policy action: get|set|delete")
	bucketPolicyBucket := flag.String("bucket-policy-bucket", "", "Bucket name for bucket-policy action")
	bucketPolicy := flag.String("bucket-policy", "", "Bucket policy JSON")
	bucketPolicyFile := flag.String("bucket-policy-file", "", "Bucket policy JSON file path")
	replRemote := flag.String("repl-remote", "", "Replication remote base URL (e.g. http://host:9000)")
	replSince := flag.String("repl-since", "", "Replication oplog HLC watermark")
	replLimit := flag.Int("repl-limit", 1000, "Replication oplog batch size")
	replFetchData := flag.Bool("repl-fetch-data", true, "Fetch missing manifests/chunks after oplog apply")
	replAccessKey := flag.String("repl-access-key", "", "Replication access key for SigV4 presign")
	replSecretKey := flag.String("repl-secret-key", "", "Replication secret key for SigV4 presign")
	replRegion := flag.String("repl-region", "us-east-1", "Replication SigV4 region")
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
	store.SetSiteID(*siteID)
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
		if *mode == "repl-pull" {
			if err := runReplPull(*replRemote, *replSince, *replLimit, *replFetchData, *replAccessKey, *replSecretKey, *replRegion, eng); err != nil {
				fmt.Fprintf(os.Stderr, "repl pull error: %v\n", err)
				os.Exit(1)
			}
			return
		}
		if *mode == "keys" {
			if err := runKeys(*keysAction, metaArg, *keyAccess, *keySecret, *keyPolicy, *keyBucket, *keyEnabled, *keyInflight, *jsonOut); err != nil {
				fmt.Fprintf(os.Stderr, "keys error: %v\n", err)
				os.Exit(1)
			}
			return
		}
		if *mode == "bucket-policy" {
			if err := runBucketPolicy(*bucketPolicyAction, metaArg, *bucketPolicyBucket, *bucketPolicy, *bucketPolicyFile, *jsonOut); err != nil {
				fmt.Fprintf(os.Stderr, "bucket policy error: %v\n", err)
				os.Exit(1)
			}
			return
		}
		gcGuard := ops.GCGuardrails{
			WarnCandidates:     *gcWarnSegments,
			WarnReclaimedBytes: *gcWarnReclaim,
			MaxCandidates:      *gcMaxSegments,
			MaxReclaimedBytes:  *gcMaxReclaim,
		}
		mpuGuard := ops.MPUGCGuardrails{
			WarnUploads:        *mpuWarnUploads,
			WarnReclaimedBytes: *mpuWarnReclaim,
			MaxUploads:         *mpuMaxUploads,
			MaxReclaimedBytes:  *mpuMaxReclaim,
		}
		if err := runOps(*mode, *dataDir, metaArg, *snapshotDir, *gcMinAge, *gcForce, *gcLiveThreshold, *gcRewritePlanFile, *gcRewriteFromPlan, *gcRewriteBps, *gcPauseFile, *mpuTTL, *mpuForce, gcGuard, mpuGuard, *jsonOut); err != nil {
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
			SecretLookup: func(ctx context.Context, accessKey string) (string, bool, error) {
				return store.LookupAPISecret(ctx, accessKey)
			},
		},
		Metrics:         s3.NewMetrics(),
		AuthLimiter:     s3.NewAuthLimiter(),
		InflightLimiter: s3.NewInflightLimiter(32),
		VirtualHosted:   *virtualHosted,
	}
	if *trustedProxies != "" {
		handler.(*s3.Handler).TrustedProxies = splitComma(*trustedProxies)
	}
	if *logRequests {
		handler = s3.LoggingMiddleware(handler)
	}
	if *tlsEnable || (*tlsCert != "" || *tlsKey != "") {
		cfg, err := newTLSConfig(*tlsCert, *tlsKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "tls config error: %v\n", err)
			os.Exit(1)
		}
		server := &http.Server{
			Addr:      *addr,
			Handler:   handler,
			TLSConfig: cfg,
		}
		ln, err := net.Listen("tcp", *addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
			os.Exit(1)
		}
		tlsLn := tls.NewListener(ln, cfg)
		if err := server.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := http.ListenAndServe(*addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
		os.Exit(1)
	}
}
