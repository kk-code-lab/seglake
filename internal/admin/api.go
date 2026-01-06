package admin

type OpsRunRequest struct {
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

type KeysRequest struct {
	Action    string `json:"action"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	Policy    string `json:"policy,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Enabled   *bool  `json:"enabled,omitempty"`
	Inflight  int64  `json:"inflight,omitempty"`
}

type BucketPolicyRequest struct {
	Action string `json:"action"`
	Bucket string `json:"bucket,omitempty"`
	Policy string `json:"policy,omitempty"`
}

type BucketsRequest struct {
	Action     string `json:"action"`
	Bucket     string `json:"bucket,omitempty"`
	Versioning string `json:"versioning,omitempty"`
	Force      bool   `json:"force,omitempty"`
}

type MaintenanceRequest struct {
	Action string `json:"action"`
	NoWait bool   `json:"no_wait,omitempty"`
}

type MaintenanceResponse struct {
	Maintenance          string `json:"maintenance"`
	MaintenanceUpdated   string `json:"maintenance_updated,omitempty"`
	Running              bool   `json:"running"`
	Addr                 string `json:"addr,omitempty"`
	WriteInflight        int64  `json:"write_inflight"`
	ServerState          string `json:"server_state,omitempty"`
	ServerStateUpdatedAt string `json:"server_state_updated,omitempty"`
}

type ReplPullRequest struct {
	Remote            string `json:"remote"`
	Since             string `json:"since,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	FetchData         bool   `json:"fetch_data,omitempty"`
	Watch             bool   `json:"watch,omitempty"`
	IntervalNanos     int64  `json:"interval_nanos,omitempty"`
	BackoffMaxNanos   int64  `json:"backoff_max_nanos,omitempty"`
	RetryTimeoutNanos int64  `json:"retry_timeout_nanos,omitempty"`
	AccessKey         string `json:"access_key,omitempty"`
	SecretKey         string `json:"secret_key,omitempty"`
	Region            string `json:"region,omitempty"`
}

type ReplPushRequest struct {
	Remote          string `json:"remote"`
	Since           string `json:"since,omitempty"`
	Limit           int    `json:"limit,omitempty"`
	Watch           bool   `json:"watch,omitempty"`
	IntervalNanos   int64  `json:"interval_nanos,omitempty"`
	BackoffMaxNanos int64  `json:"backoff_max_nanos,omitempty"`
	AccessKey       string `json:"access_key,omitempty"`
	SecretKey       string `json:"secret_key,omitempty"`
	Region          string `json:"region,omitempty"`
}

type ReplBootstrapRequest struct {
	Remote    string `json:"remote"`
	Force     bool   `json:"force,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	Region    string `json:"region,omitempty"`
}
