package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/meta"
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

func bucketSet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

type globalArgs struct {
	mode        string
	modeHelp    bool
	showVersion bool
	help        bool
	assumeYes   bool
}

type serverOptions struct {
	addr              string
	dataDir           string
	accessKey         string
	secretKey         string
	region            string
	publicBuckets     string
	virtualHosted     bool
	logRequests       bool
	allowUnsigned     bool
	tlsEnable         bool
	tlsCert           string
	tlsKey            string
	trustedProxies    string
	siteID            string
	syncInterval      time.Duration
	syncBytes         int64
	maxObjectSize     int64
	corsOrigins       string
	corsMethods       string
	corsHeaders       string
	corsMaxAge        int
	replayTTL         time.Duration
	replayBlock       bool
	replayMaxEntries  int
	requireIfMatch    string
	requireMD5        bool
	mpuCompleteLimit  int
	maxHeaderBytes    int
	maxURLLength      int
	readHeaderTimeout time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	shutdownTimeout   time.Duration
	opsAccessKey      string
	opsSecretKey      string
}

type opsOptions struct {
	dataDir           string
	snapshotDir       string
	rebuildMeta       string
	replCompareDir    string
	gcMinAge          time.Duration
	gcForce           bool
	gcWarnSegments    int
	gcWarnReclaim     int64
	gcMaxSegments     int
	gcMaxReclaim      int64
	gcLiveThreshold   float64
	gcRewritePlanFile string
	gcRewriteFromPlan string
	gcRewriteBps      int64
	gcPauseFile       string
	mpuTTL            time.Duration
	mpuForce          bool
	mpuWarnUploads    int
	mpuWarnReclaim    int64
	mpuMaxUploads     int
	mpuMaxReclaim     int64
	jsonOut           bool
	opsURL            string
	opsAccessKey      string
	opsSecretKey      string
	opsRegion         string
}

type keysOptions struct {
	dataDir     string
	rebuildMeta string
	action      string
	accessKey   string
	secretKey   string
	policy      string
	enabled     bool
	inflight    int64
	bucket      string
	jsonOut     bool
}

type bucketPolicyOptions struct {
	dataDir     string
	rebuildMeta string
	action      string
	bucket      string
	policy      string
	policyFile  string
	jsonOut     bool
}

type bucketsOptions struct {
	dataDir     string
	rebuildMeta string
	action      string
	bucket      string
	jsonOut     bool
}

type replPullOptions struct {
	dataDir      string
	siteID       string
	remote       string
	since        string
	limit        int
	fetchData    bool
	watch        bool
	interval     time.Duration
	backoffMax   time.Duration
	retryTimeout time.Duration
	accessKey    string
	secretKey    string
	region       string
	syncInterval time.Duration
	syncBytes    int64
}

type replPushOptions struct {
	dataDir    string
	siteID     string
	remote     string
	since      string
	limit      int
	watch      bool
	interval   time.Duration
	backoffMax time.Duration
	accessKey  string
	secretKey  string
	region     string
}

type replBootstrapOptions struct {
	dataDir   string
	remote    string
	accessKey string
	secretKey string
	region    string
	force     bool
}

const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 2 * time.Minute
	defaultMaxHeaderBytes    = 32 << 10
	defaultMaxURLLength      = 32 << 10
)

func main() {
	global, remaining, err := parseGlobalArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if global.showVersion {
		fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
		return
	}
	if global.help && global.mode == "" {
		printGlobalHelp()
		return
	}
	if global.help && global.mode != "" {
		global.modeHelp = true
	}
	if global.mode == "" {
		global.mode = "server"
	}

	switch {
	case global.mode == "server":
		fs, opts := newServerFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := runServer(opts); err != nil {
			exitError("server", err)
		}
	case global.mode == "repl-pull":
		fs, opts := newReplPullFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := confirmLiveMode(opts.dataDir, global.mode, global.assumeYes); err != nil {
			exitError("repl pull", err)
		}
		if err := runReplPullMode(opts); err != nil {
			exitError("repl pull", err)
		}
	case global.mode == "repl-push":
		fs, opts := newReplPushFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := confirmLiveMode(opts.dataDir, global.mode, global.assumeYes); err != nil {
			exitError("repl push", err)
		}
		if err := runReplPushMode(opts); err != nil {
			exitError("repl push", err)
		}
	case global.mode == "repl-bootstrap":
		fs, opts := newReplBootstrapFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := confirmLiveMode(opts.dataDir, global.mode, global.assumeYes); err != nil {
			exitError("repl bootstrap", err)
		}
		if err := runReplBootstrap(opts.remote, opts.accessKey, opts.secretKey, opts.region, opts.dataDir, opts.force); err != nil {
			exitError("repl bootstrap", err)
		}
	case global.mode == "keys":
		fs, opts := newKeysFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if opts.rebuildMeta == "" {
			if err := requireDataDir(opts.dataDir); err != nil {
				exitError("data dir", err)
			}
		}
		metaPath := resolveMetaPath(opts.dataDir, opts.rebuildMeta)
		if err := runKeys(opts.action, metaPath, opts.accessKey, opts.secretKey, opts.policy, opts.bucket, opts.enabled, opts.inflight, opts.jsonOut); err != nil {
			exitError("keys", err)
		}
	case global.mode == "bucket-policy":
		fs, opts := newBucketPolicyFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if opts.rebuildMeta == "" {
			if err := requireDataDir(opts.dataDir); err != nil {
				exitError("data dir", err)
			}
		}
		metaPath := resolveMetaPath(opts.dataDir, opts.rebuildMeta)
		if err := runBucketPolicy(opts.action, metaPath, opts.bucket, opts.policy, opts.policyFile, opts.jsonOut); err != nil {
			exitError("bucket policy", err)
		}
	case global.mode == "buckets":
		fs, opts := newBucketsFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if opts.rebuildMeta == "" {
			if err := requireDataDir(opts.dataDir); err != nil {
				exitError("data dir", err)
			}
		}
		metaPath := resolveMetaPath(opts.dataDir, opts.rebuildMeta)
		if err := runBuckets(opts.action, metaPath, opts.bucket, opts.jsonOut); err != nil {
			exitError("buckets", err)
		}
	case global.mode == "maintenance":
		fs, opts := newMaintenanceFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := runMaintenance(opts); err != nil {
			exitError("maintenance", err)
		}
	case isOpsMode(global.mode):
		fs, opts := newOpsFlagSet()
		if global.modeHelp {
			printModeHelp(global.mode, fs)
			return
		}
		if help, err := parseModeFlags(fs, remaining); err != nil {
			exitParseError(err)
		} else if help {
			printModeHelp(global.mode, fs)
			return
		}
		if err := confirmLiveMode(opts.dataDir, global.mode, global.assumeYes); err != nil {
			exitError("ops", err)
		}
		if err := requireDataDir(opts.dataDir); err != nil {
			exitError("data dir", err)
		}
		if err := runOpsWithMode(global.mode, opts); err != nil {
			exitError("ops", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "seglake: unknown mode %q\n", global.mode)
		os.Exit(2)
	}
}

func parseGlobalArgs(args []string) (globalArgs, []string, error) {
	var out globalArgs
	remaining := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case strings.HasPrefix(arg, "-mode="):
			out.mode = strings.TrimPrefix(arg, "-mode=")
			continue
		case arg == "-mode":
			if i+1 >= len(args) {
				return out, nil, errors.New("mode requires a value")
			}
			out.mode = args[i+1]
			i++
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "-mode-help"); ok {
			if err != nil {
				return out, nil, err
			}
			out.modeHelp = value
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "-yes"); ok {
			if err != nil {
				return out, nil, err
			}
			out.assumeYes = value
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "-version"); ok {
			if err != nil {
				return out, nil, err
			}
			out.showVersion = value
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "-v"); ok {
			if err != nil {
				return out, nil, err
			}
			out.showVersion = value
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "--help"); ok {
			if err != nil {
				return out, nil, err
			}
			out.help = value
			continue
		}
		if value, ok, err := parseBoolFlag(arg, "-h"); ok {
			if err != nil {
				return out, nil, err
			}
			out.help = value
			continue
		}
		remaining = append(remaining, arg)
	}
	return out, remaining, nil
}

func parseBoolFlag(arg, name string) (bool, bool, error) {
	if arg == name {
		return true, true, nil
	}
	if strings.HasPrefix(arg, name+"=") {
		value := strings.TrimPrefix(arg, name+"=")
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, true, fmt.Errorf("invalid value for %s: %w", name, err)
		}
		return parsed, true, nil
	}
	return false, false, nil
}

func parseModeFlags(fs *flag.FlagSet, args []string) (bool, error) {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true, nil
		}
	}
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return false, err
	}
	if fs.NArg() > 0 {
		return false, fmt.Errorf("unknown arguments: %s", strings.Join(fs.Args(), " "))
	}
	return false, nil
}

func exitParseError(err error) {
	fmt.Fprintf(os.Stderr, "seglake: %v\n", err)
	os.Exit(2)
}

func exitError(context string, err error) {
	if err == nil {
		return
	}
	if coded, ok := err.(interface{ ExitCode() int }); ok {
		if quiet, ok := err.(interface{ Quiet() bool }); ok && quiet.Quiet() {
			os.Exit(coded.ExitCode())
		}
		if err.Error() != "" {
			fmt.Fprintf(os.Stderr, "seglake: %s: %v\n", context, err)
		}
		os.Exit(coded.ExitCode())
	}
	fmt.Fprintf(os.Stderr, "seglake: %s: %v\n", context, err)
	os.Exit(1)
}

func newServerFlagSet() (*flag.FlagSet, *serverOptions) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	opts := &serverOptions{}
	fs.StringVar(&opts.addr, "addr", envOrDefault("SEGLAKE_ADDR", ":9000"), "HTTP listen address (env SEGLAKE_ADDR)")
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.accessKey, "access-key", envOrDefault("SEGLAKE_ACCESS_KEY", ""), "S3 access key (enables SigV4, env SEGLAKE_ACCESS_KEY)")
	fs.StringVar(&opts.secretKey, "secret-key", envOrDefault("SEGLAKE_SECRET_KEY", ""), "S3 secret key (enables SigV4, env SEGLAKE_SECRET_KEY)")
	fs.StringVar(&opts.region, "region", envOrDefault("SEGLAKE_REGION", "us-east-1"), "S3 region (env SEGLAKE_REGION)")
	fs.StringVar(&opts.publicBuckets, "public-buckets", "", "Comma-separated bucket names allowing unsigned requests (requires bucket policy)")
	fs.BoolVar(&opts.virtualHosted, "virtual-hosted", true, "Enable virtual-hosted-style bucket routing")
	fs.BoolVar(&opts.logRequests, "log-requests", true, "Log HTTP requests")
	fs.BoolVar(&opts.allowUnsigned, "allow-unsigned-payload", true, "Allow SigV4 UNSIGNED-PAYLOAD")
	fs.BoolVar(&opts.tlsEnable, "tls", envBoolOrDefault("SEGLAKE_TLS", false), "Enable HTTPS listener with TLS (env SEGLAKE_TLS)")
	fs.StringVar(&opts.tlsCert, "tls-cert", envOrDefault("SEGLAKE_TLS_CERT", ""), "TLS certificate path (PEM, env SEGLAKE_TLS_CERT)")
	fs.StringVar(&opts.tlsKey, "tls-key", envOrDefault("SEGLAKE_TLS_KEY", ""), "TLS private key path (PEM, env SEGLAKE_TLS_KEY)")
	fs.StringVar(&opts.trustedProxies, "trusted-proxies", "", "Comma-separated CIDR ranges trusted for X-Forwarded-For")
	fs.StringVar(&opts.opsAccessKey, "ops-access-key", envOrDefault("SEGLAKE_OPS_ACCESS_KEY", envOrDefault("SEGLAKE_ACCESS_KEY", "")), "Ops API access key (env SEGLAKE_OPS_ACCESS_KEY)")
	fs.StringVar(&opts.opsSecretKey, "ops-secret-key", envOrDefault("SEGLAKE_OPS_SECRET_KEY", envOrDefault("SEGLAKE_SECRET_KEY", "")), "Ops API secret key (env SEGLAKE_OPS_SECRET_KEY)")
	fs.StringVar(&opts.siteID, "site-id", "local", "Site identifier for replication (HLC/oplog)")
	fs.DurationVar(&opts.syncInterval, "sync-interval", 100*time.Millisecond, "Write barrier interval")
	fs.Int64Var(&opts.syncBytes, "sync-bytes", 128<<20, "Write barrier byte threshold")
	fs.Int64Var(&opts.maxObjectSize, "max-object-size", 5<<30, "Max object size in bytes (0 = unlimited)")
	fs.StringVar(&opts.corsOrigins, "cors-origins", "*", "Comma-separated CORS allowed origins (* for all)")
	fs.StringVar(&opts.corsMethods, "cors-methods", "GET,PUT,HEAD,DELETE", "Comma-separated CORS allowed methods")
	fs.StringVar(&opts.corsHeaders, "cors-headers", "authorization,content-md5,content-type,x-amz-date,x-amz-content-sha256", "Comma-separated CORS allowed headers")
	fs.IntVar(&opts.corsMaxAge, "cors-max-age", 86400, "CORS preflight max age in seconds")
	fs.DurationVar(&opts.replayTTL, "replay-ttl", 0, "Replay protection TTL (0 disables)")
	fs.BoolVar(&opts.replayBlock, "replay-block", false, "Block requests on replay detection (default logs only)")
	fs.IntVar(&opts.replayMaxEntries, "replay-cache-max", 0, "Replay cache max entries (0 = default)")
	fs.StringVar(&opts.requireIfMatch, "require-if-match-buckets", "", "Comma-separated buckets requiring If-Match on overwrite (* for all)")
	fs.BoolVar(&opts.requireMD5, "require-content-md5", false, "Require Content-MD5 on PUT/UploadPart")
	fs.IntVar(&opts.mpuCompleteLimit, "mpu-complete-limit", 4, "Max concurrent CompleteMultipartUpload operations (0 disables)")
	fs.IntVar(&opts.maxHeaderBytes, "max-header-bytes", defaultMaxHeaderBytes, "Max request header bytes (0 = Go default)")
	fs.IntVar(&opts.maxURLLength, "max-url-length", defaultMaxURLLength, "Max request URI length in bytes (0 disables)")
	fs.DurationVar(&opts.readHeaderTimeout, "read-header-timeout", defaultReadHeaderTimeout, "HTTP read header timeout")
	fs.DurationVar(&opts.readTimeout, "read-timeout", defaultReadTimeout, "HTTP read timeout")
	fs.DurationVar(&opts.writeTimeout, "write-timeout", defaultWriteTimeout, "HTTP write timeout")
	fs.DurationVar(&opts.idleTimeout, "idle-timeout", defaultIdleTimeout, "HTTP idle timeout")
	fs.DurationVar(&opts.shutdownTimeout, "shutdown-timeout", 10*time.Second, "Graceful shutdown timeout")
	return fs, opts
}

func newOpsFlagSet() (*flag.FlagSet, *opsOptions) {
	fs := flag.NewFlagSet("ops", flag.ContinueOnError)
	opts := &opsOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.snapshotDir, "snapshot-dir", "", "Snapshot output directory")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db for rebuild-index")
	fs.StringVar(&opts.replCompareDir, "repl-compare-dir", "", "Replication validation compare data dir")
	fs.StringVar(&opts.opsURL, "ops-url", "", "Ops API base URL (default uses running server heartbeat)")
	fs.StringVar(&opts.opsAccessKey, "ops-access-key", envOrDefault("SEGLAKE_ACCESS_KEY", ""), "Ops API access key (env SEGLAKE_ACCESS_KEY)")
	fs.StringVar(&opts.opsSecretKey, "ops-secret-key", envOrDefault("SEGLAKE_SECRET_KEY", ""), "Ops API secret key (env SEGLAKE_SECRET_KEY)")
	fs.StringVar(&opts.opsRegion, "ops-region", envOrDefault("SEGLAKE_REGION", "us-east-1"), "Ops API SigV4 region (env SEGLAKE_REGION)")
	fs.DurationVar(&opts.gcMinAge, "gc-min-age", 24*time.Hour, "GC minimum segment age")
	fs.BoolVar(&opts.gcForce, "gc-force", false, "GC delete segments (required for gc-run)")
	fs.IntVar(&opts.gcWarnSegments, "gc-warn-segments", 100, "GC warn when candidates exceed this count (0 disables)")
	fs.Int64Var(&opts.gcWarnReclaim, "gc-warn-reclaim-bytes", 100<<30, "GC warn when candidate bytes exceed this count (0 disables)")
	fs.IntVar(&opts.gcMaxSegments, "gc-max-segments", 0, "GC hard limit on candidates (0 disables)")
	fs.Int64Var(&opts.gcMaxReclaim, "gc-max-reclaim-bytes", 0, "GC hard limit on candidate bytes (0 disables)")
	fs.Float64Var(&opts.gcLiveThreshold, "gc-live-threshold", 0.5, "GC rewrite live-bytes ratio threshold (<= value)")
	fs.StringVar(&opts.gcRewritePlanFile, "gc-rewrite-plan", "", "GC rewrite plan output file")
	fs.StringVar(&opts.gcRewriteFromPlan, "gc-rewrite-from-plan", "", "GC rewrite plan input file")
	fs.Int64Var(&opts.gcRewriteBps, "gc-rewrite-bps", 0, "GC rewrite max bytes per second (0 = unlimited)")
	fs.StringVar(&opts.gcPauseFile, "gc-pause-file", "", "GC pause while file exists")
	fs.DurationVar(&opts.mpuTTL, "mpu-ttl", 7*24*time.Hour, "Multipart upload TTL for cleanup")
	fs.BoolVar(&opts.mpuForce, "mpu-force", false, "Multipart GC delete uploads (required for mpu-gc-run)")
	fs.IntVar(&opts.mpuWarnUploads, "mpu-warn-uploads", 1000, "MPU GC warn when uploads exceed this count (0 disables)")
	fs.Int64Var(&opts.mpuWarnReclaim, "mpu-warn-reclaim-bytes", 10<<30, "MPU GC warn when candidate bytes exceed this count (0 disables)")
	fs.IntVar(&opts.mpuMaxUploads, "mpu-max-uploads", 0, "MPU GC hard limit on uploads (0 disables)")
	fs.Int64Var(&opts.mpuMaxReclaim, "mpu-max-reclaim-bytes", 0, "MPU GC hard limit on candidate bytes (0 disables)")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output ops report as JSON")
	return fs, opts
}

func newKeysFlagSet() (*flag.FlagSet, *keysOptions) {
	fs := flag.NewFlagSet("keys", flag.ContinueOnError)
	opts := &keysOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db")
	fs.StringVar(&opts.action, "keys-action", "list", "Keys action: list|create|allow-bucket|disallow-bucket|list-buckets|list-buckets-all|enable|disable|delete|set-policy")
	fs.StringVar(&opts.accessKey, "key-access", "", "API access key for keys-action")
	fs.StringVar(&opts.secretKey, "key-secret", "", "API secret key for keys-action")
	fs.StringVar(&opts.policy, "key-policy", "rw", "API key policy: rw|ro|read-only")
	fs.BoolVar(&opts.enabled, "key-enabled", true, "API key enabled flag")
	fs.Int64Var(&opts.inflight, "key-inflight", 0, "API key inflight limit (0=default)")
	fs.StringVar(&opts.bucket, "key-bucket", "", "Bucket name for keys-action allow-bucket")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output ops report as JSON")
	return fs, opts
}

func newBucketPolicyFlagSet() (*flag.FlagSet, *bucketPolicyOptions) {
	fs := flag.NewFlagSet("bucket-policy", flag.ContinueOnError)
	opts := &bucketPolicyOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db")
	fs.StringVar(&opts.action, "bucket-policy-action", "get", "Bucket policy action: get|set|delete (get without bucket lists all)")
	fs.StringVar(&opts.bucket, "bucket-policy-bucket", "", "Bucket name for bucket-policy action")
	fs.StringVar(&opts.policy, "bucket-policy", "", "Bucket policy JSON")
	fs.StringVar(&opts.policyFile, "bucket-policy-file", "", "Bucket policy JSON file path")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output ops report as JSON")
	return fs, opts
}

func newBucketsFlagSet() (*flag.FlagSet, *bucketsOptions) {
	fs := flag.NewFlagSet("buckets", flag.ContinueOnError)
	opts := &bucketsOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db")
	fs.StringVar(&opts.action, "bucket-action", "list", "Bucket action: list|create|delete|exists")
	fs.StringVar(&opts.bucket, "bucket", "", "Bucket name for bucket-action")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output ops report as JSON")
	return fs, opts
}

func newReplPullFlagSet() (*flag.FlagSet, *replPullOptions) {
	fs := flag.NewFlagSet("repl-pull", flag.ContinueOnError)
	opts := &replPullOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", "./data", "Data directory")
	fs.StringVar(&opts.siteID, "site-id", "local", "Site identifier for replication (HLC/oplog)")
	fs.StringVar(&opts.remote, "repl-remote", "", "Replication remote base URL (e.g. http://host:9000)")
	fs.StringVar(&opts.since, "repl-since", "", "Replication oplog HLC watermark")
	fs.IntVar(&opts.limit, "repl-limit", 1000, "Replication oplog batch size")
	fs.BoolVar(&opts.fetchData, "repl-fetch-data", true, "Fetch missing manifests/chunks after oplog apply")
	fs.BoolVar(&opts.watch, "repl-watch", false, "Continuously poll replication oplog")
	fs.DurationVar(&opts.interval, "repl-interval", 5*time.Second, "Replication poll interval")
	fs.DurationVar(&opts.backoffMax, "repl-backoff-max", time.Minute, "Replication max backoff on errors")
	fs.DurationVar(&opts.retryTimeout, "repl-retry-timeout", 5*time.Minute, "Replication retry deadline for missing data")
	fs.StringVar(&opts.accessKey, "repl-access-key", "", "Replication access key for SigV4 presign")
	fs.StringVar(&opts.secretKey, "repl-secret-key", "", "Replication secret key for SigV4 presign")
	fs.StringVar(&opts.region, "repl-region", "us-east-1", "Replication SigV4 region")
	fs.DurationVar(&opts.syncInterval, "sync-interval", 100*time.Millisecond, "Write barrier interval")
	fs.Int64Var(&opts.syncBytes, "sync-bytes", 128<<20, "Write barrier byte threshold")
	return fs, opts
}

func newReplPushFlagSet() (*flag.FlagSet, *replPushOptions) {
	fs := flag.NewFlagSet("repl-push", flag.ContinueOnError)
	opts := &replPushOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", "./data", "Data directory")
	fs.StringVar(&opts.siteID, "site-id", "local", "Site identifier for replication (HLC/oplog)")
	fs.StringVar(&opts.remote, "repl-remote", "", "Replication remote base URL (e.g. http://host:9000)")
	fs.StringVar(&opts.since, "repl-push-since", "", "Replication push start HLC (optional)")
	fs.IntVar(&opts.limit, "repl-push-limit", 1000, "Replication push batch size")
	fs.BoolVar(&opts.watch, "repl-push-watch", false, "Continuously push local oplog")
	fs.DurationVar(&opts.interval, "repl-push-interval", 5*time.Second, "Replication push interval")
	fs.DurationVar(&opts.backoffMax, "repl-push-backoff-max", time.Minute, "Replication push max backoff on errors")
	fs.StringVar(&opts.accessKey, "repl-access-key", "", "Replication access key for SigV4 presign")
	fs.StringVar(&opts.secretKey, "repl-secret-key", "", "Replication secret key for SigV4 presign")
	fs.StringVar(&opts.region, "repl-region", "us-east-1", "Replication SigV4 region")
	return fs, opts
}

func newReplBootstrapFlagSet() (*flag.FlagSet, *replBootstrapOptions) {
	fs := flag.NewFlagSet("repl-bootstrap", flag.ContinueOnError)
	opts := &replBootstrapOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", "./data", "Data directory")
	fs.StringVar(&opts.remote, "repl-remote", "", "Replication remote base URL (e.g. http://host:9000)")
	fs.StringVar(&opts.accessKey, "repl-access-key", "", "Replication access key for SigV4 presign")
	fs.StringVar(&opts.secretKey, "repl-secret-key", "", "Replication secret key for SigV4 presign")
	fs.StringVar(&opts.region, "repl-region", "us-east-1", "Replication SigV4 region")
	fs.BoolVar(&opts.force, "repl-bootstrap-force", false, "Overwrite local meta.db during bootstrap")
	return fs, opts
}

func isOpsMode(mode string) bool {
	switch mode {
	case "status", "fsck", "scrub", "snapshot", "rebuild-index", "gc-plan", "gc-run", "gc-rewrite", "gc-rewrite-plan", "gc-rewrite-run", "mpu-gc-plan", "mpu-gc-run", "support-bundle", "repl-validate":
		return true
	default:
		return false
	}
}

func ensureDataDir(dataDir string) error {
	if dataDir == "" {
		return errors.New("data dir required")
	}
	return os.MkdirAll(dataDir, 0o755)
}

func requireDataDir(dataDir string) error {
	if dataDir == "" {
		return errors.New("data dir required")
	}
	info, err := os.Stat(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("data dir %q does not exist", dataDir)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("data dir %q is not a directory", dataDir)
	}
	return nil
}

func resolveMetaPath(dataDir, override string) string {
	if override != "" {
		return override
	}
	return filepath.Join(dataDir, "meta.db")
}

func runServer(opts *serverOptions) error {
	lock, err := acquireServerLock(opts.dataDir, opts.addr)
	if err != nil {
		return err
	}
	defer lock.Release()

	store, err := openStore(opts.dataDir, opts.siteID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	eng, err := openEngine(opts.dataDir, store, opts.syncInterval, opts.syncBytes)
	if err != nil {
		return err
	}

	fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
	authCfg := &s3.AuthConfig{
		AccessKey:            opts.accessKey,
		SecretKey:            opts.secretKey,
		OpsAccessKey:         opts.opsAccessKey,
		OpsSecretKey:         opts.opsSecretKey,
		Region:               opts.region,
		MaxSkew:              5 * time.Minute,
		AllowUnsignedPayload: opts.allowUnsigned,
		SecretLookup: func(ctx context.Context, accessKey string) (string, bool, error) {
			return store.LookupAPISecret(ctx, accessKey)
		},
	}
	h := &s3.Handler{
		Engine:                eng,
		Meta:                  store,
		Auth:                  authCfg,
		Metrics:               s3.NewMetrics(),
		AuthLimiter:           s3.NewAuthLimiter(),
		InflightLimiter:       s3.NewInflightLimiter(32),
		MPUCompleteLimiter:    s3.NewSemaphore(int64(opts.mpuCompleteLimit)),
		VirtualHosted:         opts.virtualHosted,
		PublicBuckets:         bucketSet(splitComma(opts.publicBuckets)),
		MaxObjectSize:         opts.maxObjectSize,
		CORSAllowOrigins:      splitComma(opts.corsOrigins),
		CORSAllowMethods:      splitComma(opts.corsMethods),
		CORSAllowHeaders:      splitComma(opts.corsHeaders),
		CORSMaxAge:            opts.corsMaxAge,
		ReplayCacheTTL:        opts.replayTTL,
		ReplayBlock:           opts.replayBlock,
		ReplayCacheMaxEntries: opts.replayMaxEntries,
		RequireIfMatchBuckets: bucketSet(splitComma(opts.requireIfMatch)),
		RequireContentMD5:     opts.requireMD5,
		MaxURLLength:          opts.maxURLLength,
		DataDir:               opts.dataDir,
	}
	if opts.trustedProxies != "" {
		h.TrustedProxies = splitComma(opts.trustedProxies)
	}
	var handler http.Handler = h
	if opts.logRequests {
		handler = s3.LoggingMiddleware(handler)
	}
	server := newHTTPServer(opts, handler)
	srvErr := make(chan error, 1)
	maintCtx, maintCancel := context.WithCancel(context.Background())
	defer maintCancel()
	go h.RunMaintenanceLoop(maintCtx, 250*time.Millisecond)
	if opts.tlsEnable || (opts.tlsCert != "" || opts.tlsKey != "") {
		cfg, err := newTLSConfig(opts.tlsCert, opts.tlsKey)
		if err != nil {
			return err
		}
		server.TLSConfig = cfg
		ln, err := net.Listen("tcp", opts.addr)
		if err != nil {
			return err
		}
		tlsLn := tls.NewListener(ln, cfg)
		go func() {
			srvErr <- server.Serve(tlsLn)
		}()
	} else {
		go func() {
			srvErr <- server.ListenAndServe()
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-srvErr:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case sig := <-stop:
		fmt.Printf("seglake: received %s, shutting down\n", sig)
		ctx, cancel := context.WithTimeout(context.Background(), opts.shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return err
		}
		err := <-srvErr
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

func newHTTPServer(opts *serverOptions, handler http.Handler) *http.Server {
	addr := ":9000"
	readHeaderTimeout := defaultReadHeaderTimeout
	readTimeout := defaultReadTimeout
	writeTimeout := defaultWriteTimeout
	idleTimeout := defaultIdleTimeout
	if opts != nil {
		if opts.addr != "" {
			addr = opts.addr
		}
		if opts.readHeaderTimeout > 0 {
			readHeaderTimeout = opts.readHeaderTimeout
		}
		if opts.readTimeout > 0 {
			readTimeout = opts.readTimeout
		}
		if opts.writeTimeout > 0 {
			writeTimeout = opts.writeTimeout
		}
		if opts.idleTimeout > 0 {
			idleTimeout = opts.idleTimeout
		}
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	if opts != nil && opts.maxHeaderBytes > 0 {
		server.MaxHeaderBytes = opts.maxHeaderBytes
	}
	return server
}

func runReplPullMode(opts *replPullOptions) error {
	store, err := openStore(opts.dataDir, opts.siteID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	eng, err := openEngine(opts.dataDir, store, opts.syncInterval, opts.syncBytes)
	if err != nil {
		return err
	}
	return runReplPull(opts.remote, opts.since, opts.limit, opts.fetchData, opts.watch, opts.interval, opts.backoffMax, opts.retryTimeout, opts.accessKey, opts.secretKey, opts.region, store, eng)
}

func runReplPushMode(opts *replPushOptions) error {
	store, err := openStore(opts.dataDir, opts.siteID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	return runReplPush(opts.remote, opts.since, opts.limit, opts.watch, opts.interval, opts.backoffMax, opts.accessKey, opts.secretKey, opts.region, store)
}

func openStore(dataDir, siteID string) (*meta.Store, error) {
	if err := requireDataDir(dataDir); err != nil {
		return nil, err
	}
	metaPath := filepath.Join(dataDir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	store.SetSiteID(siteID)
	return store, nil
}

func openEngine(dataDir string, store *meta.Store, syncInterval time.Duration, syncBytes int64) (*engine.Engine, error) {
	return engine.New(engine.Options{
		Layout:          fs.NewLayout(filepath.Join(dataDir, "objects")),
		MetaStore:       store,
		BarrierInterval: syncInterval,
		BarrierMaxBytes: syncBytes,
	})
}

func printGlobalHelp() {
	fmt.Println("Usage: seglake -mode <mode> [flags]")
	fmt.Println("Global flags: -mode, -mode-help, -yes, -version, -v, -h, --help")
	fmt.Println("Modes:")
	for _, mode := range []string{
		"server",
		"status",
		"fsck",
		"scrub",
		"snapshot",
		"rebuild-index",
		"gc-plan",
		"gc-run",
		"gc-rewrite",
		"gc-rewrite-plan",
		"gc-rewrite-run",
		"mpu-gc-plan",
		"mpu-gc-run",
		"support-bundle",
		"keys",
		"bucket-policy",
		"buckets",
		"maintenance",
		"repl-pull",
		"repl-push",
		"repl-validate",
		"repl-bootstrap",
	} {
		fmt.Printf("  %s\n", mode)
	}
	fmt.Println("Use: seglake -mode <mode> --help")
}
