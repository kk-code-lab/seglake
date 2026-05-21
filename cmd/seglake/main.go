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

	"github.com/kk-code-lab/seglake/internal/admin"
	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/clock"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
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
	if value, ok := secretEnv[key]; ok {
		return value
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		if secretValue, ok := secretEnv[key]; ok {
			value = secretValue
		} else {
			return fallback
		}
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok {
		if secretValue, ok := secretEnv[key]; ok {
			value = secretValue
		} else {
			return fallback
		}
	}
	parsed, err := time.ParseDuration(value)
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
	secretsFile string
}

type serverOptions struct {
	addr                string
	dataDir             string
	accessKey           string
	secretKey           string
	region              string
	publicBuckets       string
	virtualHosted       bool
	logRequests         bool
	allowUnsigned       bool
	tlsEnable           bool
	tlsCert             string
	tlsKey              string
	trustedProxies      string
	siteID              string
	syncInterval        time.Duration
	syncBytes           int64
	maxObjectSize       int64
	corsOrigins         string
	corsMethods         string
	corsHeaders         string
	corsMaxAge          int
	replayTTL           time.Duration
	replayBlock         bool
	replayMaxEntries    int
	requireIfMatch      string
	requireMD5          bool
	mpuCompleteLimit    int
	maxHeaderBytes      int
	maxURLLength        int
	readHeaderTimeout   time.Duration
	readTimeout         time.Duration
	writeTimeout        time.Duration
	idleTimeout         time.Duration
	shutdownTimeout     time.Duration
	sseS3Enabled        bool
	sseS3Provider       string
	sseS3ActiveKey      string
	sseS3KEKs           multiString
	sseS3KEKsEnv        string
	sseS3SingleKeyB64   string
	sseS3VaultAddr      string
	sseS3VaultMount     string
	sseS3VaultTokenFile string
	sseS3VaultNamespace string
	sseS3VaultTimeout   time.Duration
}

type multiString []string

func (m *multiString) String() string {
	if m == nil {
		return ""
	}
	return strings.Join(*m, ",")
}

func (m *multiString) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	*m = append(*m, value)
	return nil
}

type opsOptions struct {
	dataDir             string
	snapshotDir         string
	rebuildMeta         string
	replCompareDir      string
	fsckAllManifests    bool
	scrubAllManifests   bool
	scrubDeepEncrypted  bool
	gcMinAge            time.Duration
	gcForce             bool
	gcWarnSegments      int
	gcWarnReclaim       int64
	gcMaxSegments       int
	gcMaxReclaim        int64
	gcLiveThreshold     float64
	gcRewritePlanFile   string
	gcRewriteFromPlan   string
	gcRewriteBps        int64
	gcPauseFile         string
	manifestGCTTL       time.Duration
	manifestGCPlan      string
	manifestGCFromPlan  string
	manifestGCForce     bool
	mpuTTL              time.Duration
	mpuForce            bool
	mpuWarnUploads      int
	mpuWarnReclaim      int64
	mpuMaxUploads       int
	mpuMaxReclaim       int64
	dbReindexTable      string
	sseS3ActiveKey      string
	sseS3Provider       string
	sseS3KEKs           multiString
	sseS3KEKsEnv        string
	sseS3SingleKeyB64   string
	sseS3VaultAddr      string
	sseS3VaultMount     string
	sseS3VaultTokenFile string
	sseS3VaultNamespace string
	sseS3VaultTimeout   time.Duration
	sseRewrapTarget     string
	sseRewrapSources    multiString
	sseRewrapPlan       string
	sseRewrapFromPlan   string
	jsonOut             bool
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
	versioning  string
	force       bool
	jsonOut     bool
}

type conflictsOptions struct {
	dataDir      string
	rebuildMeta  string
	bucket       string
	prefix       string
	afterBucket  string
	afterKey     string
	afterVersion string
	limit        int
	jsonOut      bool
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
	if global.secretsFile != "" {
		if err := loadSecretsFile(global.secretsFile); err != nil {
			fmt.Fprintf(os.Stderr, "seglake: secrets file: %v\n", err)
			os.Exit(2)
		}
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
		if client, ok, err := adminClientIfRunning(opts.dataDir); err != nil {
			exitError("repl bootstrap", err)
		} else if ok {
			req := admin.ReplBootstrapRequest{
				Remote:    opts.remote,
				Force:     opts.force,
				AccessKey: opts.accessKey,
				SecretKey: opts.secretKey,
				Region:    opts.region,
			}
			var resp map[string]string
			if err := client.postJSON("/admin/repl/bootstrap", req, &resp); err != nil {
				exitError("repl bootstrap", err)
			}
		} else if err := runReplBootstrap(opts.remote, opts.accessKey, opts.secretKey, opts.region, opts.dataDir, opts.force); err != nil {
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
		if err := runBuckets(opts.action, metaPath, opts.bucket, opts.versioning, opts.force, opts.jsonOut); err != nil {
			exitError("buckets", err)
		}
	case global.mode == "conflicts":
		fs, opts := newConflictsFlagSet()
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
		if err := runConflicts(metaPath, opts.bucket, opts.prefix, opts.afterBucket, opts.afterKey, opts.afterVersion, opts.limit, opts.jsonOut); err != nil {
			exitError("conflicts", err)
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
		case strings.HasPrefix(arg, "-secrets-file="):
			out.secretsFile = strings.TrimPrefix(arg, "-secrets-file=")
			continue
		case arg == "-secrets-file":
			if i+1 >= len(args) {
				return out, nil, errors.New("secrets-file requires a value")
			}
			out.secretsFile = args[i+1]
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

var secretEnv = map[string]string{}

func loadSecretsFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid line %d", i+1)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return fmt.Errorf("invalid line %d", i+1)
		}
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		if _, ok := os.LookupEnv(key); ok {
			continue
		}
		secretEnv[key] = value
	}
	return nil
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
	fs.StringVar(&opts.publicBuckets, "public-buckets", envOrDefault("SEGLAKE_PUBLIC_BUCKETS", ""), "Comma-separated bucket names allowing unsigned requests (requires bucket policy, env SEGLAKE_PUBLIC_BUCKETS)")
	fs.BoolVar(&opts.virtualHosted, "virtual-hosted", envBoolOrDefault("SEGLAKE_VIRTUAL_HOSTED", true), "Enable virtual-hosted-style bucket routing (env SEGLAKE_VIRTUAL_HOSTED)")
	fs.BoolVar(&opts.logRequests, "log-requests", true, "Log HTTP requests")
	fs.BoolVar(&opts.allowUnsigned, "allow-unsigned-payload", true, "Allow SigV4 UNSIGNED-PAYLOAD")
	fs.BoolVar(&opts.tlsEnable, "tls", envBoolOrDefault("SEGLAKE_TLS", false), "Enable HTTPS listener with TLS (env SEGLAKE_TLS)")
	fs.StringVar(&opts.tlsCert, "tls-cert", envOrDefault("SEGLAKE_TLS_CERT", ""), "TLS certificate path (PEM, env SEGLAKE_TLS_CERT)")
	fs.StringVar(&opts.tlsKey, "tls-key", envOrDefault("SEGLAKE_TLS_KEY", ""), "TLS private key path (PEM, env SEGLAKE_TLS_KEY)")
	fs.StringVar(&opts.trustedProxies, "trusted-proxies", envOrDefault("SEGLAKE_TRUSTED_PROXIES", ""), "Comma-separated CIDR ranges trusted for X-Forwarded-For (env SEGLAKE_TRUSTED_PROXIES)")
	fs.StringVar(&opts.siteID, "site-id", "local", "Site identifier for replication (HLC/oplog)")
	fs.DurationVar(&opts.syncInterval, "sync-interval", 100*time.Millisecond, "Write barrier interval")
	fs.Int64Var(&opts.syncBytes, "sync-bytes", 128<<20, "Write barrier byte threshold")
	fs.Int64Var(&opts.maxObjectSize, "max-object-size", 5<<30, "Max object size in bytes (0 = unlimited)")
	fs.StringVar(&opts.corsOrigins, "cors-origins", "*", "Comma-separated CORS allowed origins (* for all)")
	fs.StringVar(&opts.corsMethods, "cors-methods", "GET,PUT,HEAD,DELETE", "Comma-separated CORS allowed methods")
	fs.StringVar(&opts.corsHeaders, "cors-headers", "authorization,content-md5,content-type,x-amz-date,x-amz-content-sha256,x-amz-server-side-encryption,x-amz-server-side-encryption-aws-kms-key-id", "Comma-separated CORS allowed headers")
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
	fs.BoolVar(&opts.sseS3Enabled, "sse-s3-enabled", envBoolOrDefault("SEGLAKE_SSE_S3_ENABLED", false), "Enable explicit SSE-S3 object encryption")
	fs.StringVar(&opts.sseS3Provider, "sse-s3-provider", envOrDefault("SEGLAKE_SSE_S3_PROVIDER", ssecrypto.ProviderLocal), "SSE-S3 key provider: local|vault-transit")
	fs.StringVar(&opts.sseS3ActiveKey, "sse-s3-active-key", envOrDefault("SEGLAKE_SSE_S3_ACTIVE_KEY", ""), "Active SSE-S3 key id for new encrypted writes")
	fs.Var(&opts.sseS3KEKs, "sse-s3-kek", "SSE-S3 KEK spec key-id=file:/path or key-id=env:NAME (repeatable)")
	opts.sseS3KEKsEnv = envOrDefault("SEGLAKE_SSE_S3_KEKS", "")
	opts.sseS3SingleKeyB64 = envOrDefault("SEGLAKE_SSE_S3_KEK_B64", "")
	fs.StringVar(&opts.sseS3VaultAddr, "sse-s3-vault-addr", envOrDefault("SEGLAKE_SSE_S3_VAULT_ADDR", envOrDefault("VAULT_ADDR", "")), "Vault address for SSE-S3 vault-transit provider")
	fs.StringVar(&opts.sseS3VaultMount, "sse-s3-vault-mount", envOrDefault("SEGLAKE_SSE_S3_VAULT_MOUNT", "transit"), "Vault Transit mount path for SSE-S3")
	fs.StringVar(&opts.sseS3VaultTokenFile, "sse-s3-vault-token-file", envOrDefault("SEGLAKE_SSE_S3_VAULT_TOKEN_FILE", ""), "File containing Vault token for SSE-S3")
	fs.StringVar(&opts.sseS3VaultNamespace, "sse-s3-vault-namespace", envOrDefault("SEGLAKE_SSE_S3_VAULT_NAMESPACE", ""), "Vault namespace for SSE-S3")
	fs.DurationVar(&opts.sseS3VaultTimeout, "sse-s3-vault-timeout", envDurationOrDefault("SEGLAKE_SSE_S3_VAULT_TIMEOUT", 5*time.Second), "Vault HTTP timeout for SSE-S3")
	return fs, opts
}

func newOpsFlagSet() (*flag.FlagSet, *opsOptions) {
	fs := flag.NewFlagSet("ops", flag.ContinueOnError)
	opts := &opsOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.snapshotDir, "snapshot-dir", "", "Snapshot output directory")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db for rebuild-index")
	fs.StringVar(&opts.replCompareDir, "repl-compare-dir", "", "Replication validation compare data dir")
	fs.BoolVar(&opts.fsckAllManifests, "fsck-all-manifests", false, "Fsck scan all manifests instead of live set from meta")
	fs.BoolVar(&opts.scrubAllManifests, "scrub-all-manifests", false, "Scrub scan all manifests instead of live set from meta")
	fs.BoolVar(&opts.scrubDeepEncrypted, "scrub-deep-encrypted", false, "Scrub encrypted SSE-S3 chunks by unwrapping DEKs and verifying AEAD tags")
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
	fs.DurationVar(&opts.manifestGCTTL, "manifest-gc-ttl", 7*24*time.Hour, "Manifest GC orphan minimum age")
	fs.StringVar(&opts.manifestGCPlan, "manifest-gc-plan", "", "Manifest GC plan output file")
	fs.StringVar(&opts.manifestGCFromPlan, "manifest-gc-from-plan", "", "Manifest GC plan input file")
	fs.BoolVar(&opts.manifestGCForce, "manifest-gc-force", false, "Manifest GC delete files (required for manifest-gc-run)")
	fs.DurationVar(&opts.mpuTTL, "mpu-ttl", 7*24*time.Hour, "Multipart upload TTL for cleanup")
	fs.BoolVar(&opts.mpuForce, "mpu-force", false, "Multipart GC delete uploads (required for mpu-gc-run)")
	fs.IntVar(&opts.mpuWarnUploads, "mpu-warn-uploads", 1000, "MPU GC warn when uploads exceed this count (0 disables)")
	fs.Int64Var(&opts.mpuWarnReclaim, "mpu-warn-reclaim-bytes", 10<<30, "MPU GC warn when candidate bytes exceed this count (0 disables)")
	fs.IntVar(&opts.mpuMaxUploads, "mpu-max-uploads", 0, "MPU GC hard limit on uploads (0 disables)")
	fs.Int64Var(&opts.mpuMaxReclaim, "mpu-max-reclaim-bytes", 0, "MPU GC hard limit on candidate bytes (0 disables)")
	fs.StringVar(&opts.dbReindexTable, "db-reindex-table", "", "DB reindex table/index name (optional)")
	opts.sseS3Provider = envOrDefault("SEGLAKE_SSE_S3_PROVIDER", ssecrypto.ProviderLocal)
	fs.StringVar(&opts.sseS3Provider, "sse-s3-provider", opts.sseS3Provider, "SSE-S3 key provider: local|vault-transit")
	opts.sseS3ActiveKey = envOrDefault("SEGLAKE_SSE_S3_ACTIVE_KEY", "")
	fs.StringVar(&opts.sseS3ActiveKey, "sse-s3-active-key", opts.sseS3ActiveKey, "Active SSE-S3 key id for ops")
	fs.Var(&opts.sseS3KEKs, "sse-s3-kek", "SSE-S3 KEK spec key-id=file:/path or key-id=env:NAME (repeatable)")
	opts.sseS3KEKsEnv = envOrDefault("SEGLAKE_SSE_S3_KEKS", "")
	opts.sseS3SingleKeyB64 = envOrDefault("SEGLAKE_SSE_S3_KEK_B64", "")
	fs.StringVar(&opts.sseS3VaultAddr, "sse-s3-vault-addr", envOrDefault("SEGLAKE_SSE_S3_VAULT_ADDR", envOrDefault("VAULT_ADDR", "")), "Vault address for SSE-S3 vault-transit provider")
	fs.StringVar(&opts.sseS3VaultMount, "sse-s3-vault-mount", envOrDefault("SEGLAKE_SSE_S3_VAULT_MOUNT", "transit"), "Vault Transit mount path for SSE-S3")
	fs.StringVar(&opts.sseS3VaultTokenFile, "sse-s3-vault-token-file", envOrDefault("SEGLAKE_SSE_S3_VAULT_TOKEN_FILE", ""), "File containing Vault token for SSE-S3")
	fs.StringVar(&opts.sseS3VaultNamespace, "sse-s3-vault-namespace", envOrDefault("SEGLAKE_SSE_S3_VAULT_NAMESPACE", ""), "Vault namespace for SSE-S3")
	fs.DurationVar(&opts.sseS3VaultTimeout, "sse-s3-vault-timeout", envDurationOrDefault("SEGLAKE_SSE_S3_VAULT_TIMEOUT", 5*time.Second), "Vault HTTP timeout for SSE-S3")
	fs.StringVar(&opts.sseRewrapTarget, "sse-s3-rewrap-target-key", envOrDefault("SEGLAKE_SSE_S3_REWRAP_TARGET_KEY", ""), "Target SSE-S3 KEK id for rewrap")
	fs.Var(&opts.sseRewrapSources, "sse-s3-rewrap-source-key", "Source SSE-S3 KEK id to rewrap (repeatable; default all non-target keys)")
	fs.StringVar(&opts.sseRewrapPlan, "sse-s3-rewrap-plan", "", "SSE-S3 rewrap plan output file")
	fs.StringVar(&opts.sseRewrapFromPlan, "sse-s3-rewrap-from-plan", "", "SSE-S3 rewrap plan input file")
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
	fs.StringVar(&opts.versioning, "bucket-versioning", "", "Bucket versioning for create: enabled|suspended|disabled|unversioned")
	fs.BoolVar(&opts.force, "bucket-force", false, "Force delete bucket by deleting live objects first")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output ops report as JSON")
	return fs, opts
}

func newConflictsFlagSet() (*flag.FlagSet, *conflictsOptions) {
	fs := flag.NewFlagSet("conflicts", flag.ContinueOnError)
	opts := &conflictsOptions{}
	fs.StringVar(&opts.dataDir, "data-dir", envOrDefault("SEGLAKE_DATA_DIR", "./data"), "Data directory (env SEGLAKE_DATA_DIR)")
	fs.StringVar(&opts.rebuildMeta, "rebuild-meta", "", "Path to meta.db")
	fs.StringVar(&opts.bucket, "conflicts-bucket", "", "Bucket filter")
	fs.StringVar(&opts.prefix, "conflicts-prefix", "", "Key prefix filter")
	fs.StringVar(&opts.afterBucket, "conflicts-after-bucket", "", "Pagination marker bucket")
	fs.StringVar(&opts.afterKey, "conflicts-after-key", "", "Pagination marker key")
	fs.StringVar(&opts.afterVersion, "conflicts-after-version", "", "Pagination marker version id")
	fs.IntVar(&opts.limit, "conflicts-limit", 1000, "Maximum conflicts to return (1-10000)")
	fs.BoolVar(&opts.jsonOut, "json", false, "Output conflicts as JSON")
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
	case "status", "fsck", "scrub", "snapshot", "rebuild-index", "gc-plan", "gc-run", "gc-rewrite", "gc-rewrite-plan", "gc-rewrite-run", "manifest-gc-plan", "manifest-gc-run", "mpu-gc-plan", "mpu-gc-run", "sse-rewrap-plan", "sse-rewrap-run", "support-bundle", "repl-validate", "db-integrity-check", "db-reindex":
		return true
	default:
		return false
	}
}

func ensureDataDir(dataDir string) error {
	if dataDir == "" {
		return ErrDataDirRequired
	}
	return os.MkdirAll(dataDir, 0o755)
}

func requireDataDir(dataDir string) error {
	if dataDir == "" {
		return ErrDataDirRequired
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
	adminSocketPath := defaultAdminSocketPath(opts.dataDir)
	adminTokenPath := defaultAdminTokenPath(opts.dataDir)
	lock, err := acquireServerLock(opts.dataDir, opts.addr, adminSocketPath, adminTokenPath)
	if err != nil {
		return err
	}
	defer lock.Release()

	store, err := openStore(opts.dataDir, opts.siteID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	sseProvider, err := buildSSEProvider(opts)
	if err != nil {
		return err
	}
	eng, err := openEngine(opts.dataDir, store, opts.syncInterval, opts.syncBytes, sseProvider)
	if err != nil {
		return err
	}

	fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
	clk := clock.RealClock{}
	authCfg := &s3.AuthConfig{
		AccessKey:            opts.accessKey,
		SecretKey:            opts.secretKey,
		Region:               opts.region,
		MaxSkew:              5 * time.Minute,
		AllowUnsignedPayload: opts.allowUnsigned,
		Clock:                clk,
		SecretLookup: func(ctx context.Context, accessKey string) (string, bool, error) {
			return store.LookupAPISecret(ctx, accessKey)
		},
	}
	authLimiter := s3.NewAuthLimiter()
	authLimiter.Clock = clk
	h := &s3.Handler{
		Engine:                eng,
		Meta:                  store,
		Auth:                  authCfg,
		Metrics:               s3.NewMetrics(),
		Clock:                 clk,
		AuthLimiter:           authLimiter,
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
		handler = s3.LoggingMiddleware(handler, h.Clock)
	}
	adminCtx, adminCancel := context.WithCancel(context.Background())
	defer adminCancel()
	_, _, socketPath, tokenPath, err := startAdminServer(adminCtx, opts.dataDir, opts.addr, store, eng, h.WriteInflight)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(socketPath)
		_ = os.Remove(tokenPath)
	}()
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

func buildSSEProvider(opts *serverOptions) (ssecrypto.KeyProvider, error) {
	if opts == nil || !opts.sseS3Enabled {
		return nil, nil
	}
	return buildSSEProviderFrom(sseProviderBuildOptions{
		Provider:       opts.sseS3Provider,
		ActiveKey:      opts.sseS3ActiveKey,
		KEKSpecs:       opts.sseS3KEKs,
		KEKEnv:         opts.sseS3KEKsEnv,
		SingleKeyB64:   opts.sseS3SingleKeyB64,
		VaultAddr:      opts.sseS3VaultAddr,
		VaultMount:     opts.sseS3VaultMount,
		VaultTokenFile: opts.sseS3VaultTokenFile,
		VaultNamespace: opts.sseS3VaultNamespace,
		VaultTimeout:   opts.sseS3VaultTimeout,
	})
}

type sseProviderBuildOptions struct {
	Provider       string
	ActiveKey      string
	KEKSpecs       []string
	KEKEnv         string
	SingleKeyB64   string
	VaultAddr      string
	VaultMount     string
	VaultTokenFile string
	VaultNamespace string
	VaultTimeout   time.Duration
}

func buildSSEProviderFrom(opts sseProviderBuildOptions) (ssecrypto.KeyProvider, error) {
	provider := strings.TrimSpace(opts.Provider)
	if provider == "" {
		provider = ssecrypto.ProviderLocal
	}
	switch provider {
	case ssecrypto.ProviderLocal:
		local, err := buildLocalSSEProvider(opts.ActiveKey, opts.KEKSpecs, opts.KEKEnv, opts.SingleKeyB64)
		if err != nil {
			return nil, err
		}
		return local, nil
	case ssecrypto.ProviderVaultTransit:
		vault, err := buildVaultSSEProvider(opts)
		if err != nil {
			return nil, err
		}
		var readers []ssecrypto.KeyProvider
		if hasSSEKeyConfig(opts.KEKSpecs, opts.KEKEnv, opts.SingleKeyB64) {
			localLookup, err := buildSSELookupProviderFrom(opts.ActiveKey, opts.KEKSpecs, opts.KEKEnv, opts.SingleKeyB64)
			if err != nil {
				return nil, err
			}
			readers = append(readers, localLookup)
		}
		if len(readers) == 0 {
			return vault, nil
		}
		return ssecrypto.NewRoutingProvider(vault, readers...)
	default:
		return nil, fmt.Errorf("sse-s3: unsupported provider %q", provider)
	}
}

func buildLocalSSEProvider(activeKey string, kekSpecs []string, kekEnv, singleKeyB64 string) (*ssecrypto.Provider, error) {
	activeKey = strings.TrimSpace(activeKey)
	keys, err := loadSSEKeys(activeKey, kekSpecs, kekEnv, singleKeyB64)
	if err != nil {
		return nil, err
	}
	return ssecrypto.NewProvider(activeKey, keys)
}

func buildSSELookupProviderFrom(singleKeyID string, kekSpecs []string, kekEnv, singleKeyB64 string) (ssecrypto.KeyProvider, error) {
	keys, err := loadSSEKeys(singleKeyID, kekSpecs, kekEnv, singleKeyB64)
	if err != nil {
		return nil, err
	}
	return ssecrypto.NewLookupProvider(keys)
}

func buildVaultSSEProvider(opts sseProviderBuildOptions) (*ssecrypto.VaultTransitProvider, error) {
	token, err := loadVaultToken(opts.VaultTokenFile)
	if err != nil {
		return nil, err
	}
	return ssecrypto.NewVaultTransitProvider(ssecrypto.VaultTransitConfig{
		Address:   opts.VaultAddr,
		Mount:     opts.VaultMount,
		Token:     token,
		Namespace: opts.VaultNamespace,
		ActiveKey: opts.ActiveKey,
		Timeout:   opts.VaultTimeout,
	})
}

func loadVaultToken(tokenFile string) (string, error) {
	tokenFile = strings.TrimSpace(tokenFile)
	if tokenFile != "" {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("sse-s3: read vault token file: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", fmt.Errorf("sse-s3: vault token file is empty")
		}
		return token, nil
	}
	if token := strings.TrimSpace(envOrDefault("SEGLAKE_SSE_S3_VAULT_TOKEN", "")); token != "" {
		return token, nil
	}
	if token := strings.TrimSpace(envOrDefault("VAULT_TOKEN", "")); token != "" {
		return token, nil
	}
	return "", fmt.Errorf("sse-s3: vault token required via SEGLAKE_SSE_S3_VAULT_TOKEN, VAULT_TOKEN, or -sse-s3-vault-token-file")
}

func loadSSEKeys(singleKeyID string, kekSpecs []string, kekEnv, singleKeyB64 string) ([]ssecrypto.Key, error) {
	singleKeyID = strings.TrimSpace(singleKeyID)
	specs := make([]string, 0, len(kekSpecs)+4)
	specs = append(specs, kekSpecs...)
	specs = append(specs, splitComma(kekEnv)...)
	if singleKeyB64 != "" {
		if singleKeyID == "" {
			return nil, fmt.Errorf("sse-s3: active key required with SEGLAKE_SSE_S3_KEK_B64")
		}
		specs = append(specs, singleKeyID+"=inline:"+singleKeyB64)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("sse-s3: at least one -sse-s3-kek or SEGLAKE_SSE_S3_KEKS entry required")
	}
	keys := make([]ssecrypto.Key, 0, len(specs))
	for _, spec := range specs {
		key, err := parseSSEKeySpec(spec)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func parseSSEKeySpec(spec string) (ssecrypto.Key, error) {
	spec = strings.TrimSpace(spec)
	keyID, source, ok := strings.Cut(spec, "=")
	if !ok {
		return ssecrypto.Key{}, fmt.Errorf("sse-s3: invalid kek spec %q", spec)
	}
	keyID = strings.TrimSpace(keyID)
	source = strings.TrimSpace(source)
	var b64 string
	switch {
	case strings.HasPrefix(source, "env:"):
		envName := strings.TrimSpace(strings.TrimPrefix(source, "env:"))
		if envName == "" {
			return ssecrypto.Key{}, fmt.Errorf("sse-s3: empty env source for key %q", keyID)
		}
		value, ok := os.LookupEnv(envName)
		if !ok {
			value, ok = secretEnv[envName]
		}
		if !ok {
			return ssecrypto.Key{}, fmt.Errorf("sse-s3: env source %q not set", envName)
		}
		b64 = value
	case strings.HasPrefix(source, "file:"):
		path := strings.TrimSpace(strings.TrimPrefix(source, "file:"))
		if path == "" {
			return ssecrypto.Key{}, fmt.Errorf("sse-s3: empty file source for key %q", keyID)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return ssecrypto.Key{}, fmt.Errorf("sse-s3: read key file: %w", err)
		}
		b64 = strings.TrimSpace(string(data))
	case strings.HasPrefix(source, "inline:"):
		b64 = strings.TrimSpace(strings.TrimPrefix(source, "inline:"))
	default:
		return ssecrypto.Key{}, fmt.Errorf("sse-s3: unsupported key source for %q", keyID)
	}
	return ssecrypto.DecodeKey(keyID, b64)
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
	if client, ok, err := adminClientIfRunning(opts.dataDir); err != nil {
		return err
	} else if ok {
		req := admin.ReplPullRequest{
			Remote:            opts.remote,
			Since:             opts.since,
			Limit:             opts.limit,
			FetchData:         opts.fetchData,
			Watch:             opts.watch,
			IntervalNanos:     int64(opts.interval),
			BackoffMaxNanos:   int64(opts.backoffMax),
			RetryTimeoutNanos: int64(opts.retryTimeout),
			AccessKey:         opts.accessKey,
			SecretKey:         opts.secretKey,
			Region:            opts.region,
		}
		var resp map[string]string
		return client.postJSON("/admin/repl/pull", req, &resp)
	}
	store, err := openStore(opts.dataDir, opts.siteID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	eng, err := openEngine(opts.dataDir, store, opts.syncInterval, opts.syncBytes, nil)
	if err != nil {
		return err
	}
	return runReplPull(opts.remote, opts.since, opts.limit, opts.fetchData, opts.watch, opts.interval, opts.backoffMax, opts.retryTimeout, opts.accessKey, opts.secretKey, opts.region, store, eng)
}

func runReplPushMode(opts *replPushOptions) error {
	if client, ok, err := adminClientIfRunning(opts.dataDir); err != nil {
		return err
	} else if ok {
		req := admin.ReplPushRequest{
			Remote:          opts.remote,
			Since:           opts.since,
			Limit:           opts.limit,
			Watch:           opts.watch,
			IntervalNanos:   int64(opts.interval),
			BackoffMaxNanos: int64(opts.backoffMax),
			AccessKey:       opts.accessKey,
			SecretKey:       opts.secretKey,
			Region:          opts.region,
		}
		var resp map[string]string
		return client.postJSON("/admin/repl/push", req, &resp)
	}
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

func openEngine(dataDir string, store *meta.Store, syncInterval time.Duration, syncBytes int64, sseProvider ssecrypto.KeyProvider) (*engine.Engine, error) {
	return engine.New(engine.Options{
		Layout:          fs.NewLayout(filepath.Join(dataDir, "objects")),
		MetaStore:       store,
		BarrierInterval: syncInterval,
		BarrierMaxBytes: syncBytes,
		SSE:             sseProvider,
	})
}

func printGlobalHelp() {
	fmt.Println("Usage: seglake -mode <mode> [flags]")
	fmt.Println("Global flags: -mode, -mode-help, -secrets-file, -yes, -version, -v, -h, --help")
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
		"manifest-gc-plan",
		"manifest-gc-run",
		"mpu-gc-plan",
		"mpu-gc-run",
		"sse-rewrap-plan",
		"sse-rewrap-run",
		"support-bundle",
		"keys",
		"bucket-policy",
		"buckets",
		"conflicts",
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
