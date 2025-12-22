package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/clock"
	_ "modernc.org/sqlite"
)

// Store wraps the SQLite metadata database.
type Store struct {
	db     *sql.DB
	hlc    *clock.HLC
	siteID string
}

// APIKey describes stored credentials and policy metadata.
type APIKey struct {
	AccessKey     string
	SecretKey     string
	Enabled       bool
	CreatedAt     string
	Label         string
	LastUsedAt    string
	Policy        string
	InflightLimit int64
}

// OplogEntry describes a single replication log entry.
type OplogEntry struct {
	ID        int64  `json:"id"`
	SiteID    string `json:"site_id"`
	HLCTS     string `json:"hlc_ts"`
	OpType    string `json:"op_type"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
	Payload   string `json:"payload,omitempty"`
	CreatedAt string `json:"created_at"`
}

type oplogPutPayload struct {
	ETag         string `json:"etag"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified_utc"`
}

type oplogDeletePayload struct {
	LastModified string `json:"last_modified_utc"`
}

// ReplRemoteState describes replication watermarks per remote.
type ReplRemoteState struct {
	Remote      string `json:"remote"`
	UpdatedAt   string `json:"updated_at"`
	LastPullHLC string `json:"last_pull_hlc,omitempty"`
	LastPushHLC string `json:"last_push_hlc,omitempty"`
}

type oplogMPUCompletePayload struct {
	ETag         string `json:"etag"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified_utc"`
}

type oplogBucketPolicyPayload struct {
	Bucket    string `json:"bucket"`
	Policy    string `json:"policy"`
	UpdatedAt string `json:"updated_at"`
}

type oplogAPIKeyPayload struct {
	AccessKey     string `json:"access_key"`
	SecretKey     string `json:"secret_key,omitempty"`
	Enabled       bool   `json:"enabled"`
	Policy        string `json:"policy"`
	InflightLimit int64  `json:"inflight_limit"`
	Deleted       bool   `json:"deleted,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}

type oplogAPIKeyBucketPayload struct {
	AccessKey string `json:"access_key"`
	Bucket    string `json:"bucket"`
	Allowed   bool   `json:"allowed"`
	UpdatedAt string `json:"updated_at"`
}

const metaOplogBucket = "_meta"

// Open opens or creates the metadata database at the given path.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("meta: db path required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db, hlc: clock.New(), siteID: "local"}
	if err := store.applyPragmas(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// SetSiteID configures the local site identifier used in oplog entries.
func (s *Store) SetSiteID(siteID string) {
	if s == nil || siteID == "" {
		return
	}
	s.siteID = siteID
}

func (s *Store) nextHLC() (string, string) {
	if s == nil {
		return "", ""
	}
	if s.hlc == nil {
		s.hlc = clock.New()
	}
	siteID := s.siteID
	if siteID == "" {
		siteID = "local"
	}
	return s.hlc.Next(), siteID
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// ExecTx executes a statement within a provided transaction.
func ExecTx(tx *sql.Tx, query string, args ...any) error {
	_, err := tx.Exec(query, args...)
	return err
}

// Begin starts a transaction.
func (s *Store) Begin() (*sql.Tx, error) {
	return s.db.Begin()
}

// FlushWith executes commits within a transaction and flushes WAL.
func (s *Store) FlushWith(commits []func(tx *sql.Tx) error) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}
	for _, commit := range commits {
		if err := commit(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.Flush()
}

// WithTx runs fn in a single transaction.
func (s *Store) WithTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Flush forces a WAL checkpoint to durably persist changes.
func (s *Store) Flush() error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

func (s *Store) applyPragmas(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return err
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version INTEGER PRIMARY KEY,
	applied_at TEXT NOT NULL
)`); err != nil {
		return err
	}

	var version int
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version); err != nil {
		return err
	}
	if version < 1 {
		if err = applyV1(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(1, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 2 {
		if err = applyV2(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(2, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 3 {
		if err = applyV3(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(3, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 4 {
		if err = applyV4(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(4, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 5 {
		if err = applyV5(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(5, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 6 {
		if err = applyV6(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(6, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 7 {
		if err = applyV7(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(7, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 8 {
		if err = applyV8(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(8, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 9 {
		if err = applyV9(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(9, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if version < 10 {
		if err = applyV10(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(10, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyV1(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS buckets (
			bucket TEXT PRIMARY KEY,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS versions (
			version_id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			etag TEXT,
			size INTEGER NOT NULL,
			last_modified_utc TEXT NOT NULL,
			hlc_ts TEXT,
			site_id TEXT,
			state TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS versions_bucket_key_idx ON versions(bucket, key)`,
		`CREATE TABLE IF NOT EXISTS objects_current (
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			version_id TEXT NOT NULL,
			PRIMARY KEY(bucket, key)
		)`,
		`CREATE TABLE IF NOT EXISTS manifests (
			version_id TEXT PRIMARY KEY,
			path TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS segments (
			segment_id TEXT PRIMARY KEY,
			path TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			sealed_at TEXT,
			size INTEGER,
			footer_checksum BLOB
		)`,
		`CREATE TABLE IF NOT EXISTS api_keys (
			access_key TEXT PRIMARY KEY,
			secret_hash TEXT NOT NULL,
			salt TEXT NOT NULL,
			enabled INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			label TEXT,
			last_used_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS api_key_bucket_allow (
			access_key TEXT NOT NULL,
			bucket TEXT NOT NULL,
			PRIMARY KEY(access_key, bucket)
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV2(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS multipart_uploads (
			upload_id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			created_at TEXT NOT NULL,
			state TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS multipart_uploads_bucket_key_idx ON multipart_uploads(bucket, key)`,
		`CREATE TABLE IF NOT EXISTS multipart_parts (
			upload_id TEXT NOT NULL,
			part_number INTEGER NOT NULL,
			version_id TEXT NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL,
			last_modified_utc TEXT NOT NULL,
			PRIMARY KEY(upload_id, part_number)
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV3(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS rebuild_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			updated_at TEXT NOT NULL,
			status TEXT NOT NULL,
			message TEXT
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV4(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS ops_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			mode TEXT NOT NULL,
			finished_at TEXT NOT NULL,
			errors INTEGER NOT NULL,
			deleted INTEGER,
			reclaimed_bytes INTEGER,
			rewritten_segments INTEGER,
			rewritten_bytes INTEGER,
			new_segments INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS ops_runs_mode_finished_idx ON ops_runs(mode, finished_at)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV5(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`ALTER TABLE ops_runs ADD COLUMN candidates INTEGER`,
		`ALTER TABLE ops_runs ADD COLUMN candidate_bytes INTEGER`,
		`ALTER TABLE ops_runs ADD COLUMN warnings INTEGER`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(err.Error(), "duplicate column") {
				continue
			}
			return err
		}
	}
	return nil
}

func applyV6(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`ALTER TABLE api_keys ADD COLUMN secret_key TEXT`,
		`ALTER TABLE api_keys ADD COLUMN policy TEXT`,
		`ALTER TABLE api_keys ADD COLUMN inflight_limit INTEGER`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(err.Error(), "duplicate column") {
				continue
			}
			return err
		}
	}
	return nil
}

func applyV7(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS bucket_policies (
			bucket TEXT PRIMARY KEY,
			policy TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV8(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS oplog (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			site_id TEXT NOT NULL,
			hlc_ts TEXT NOT NULL,
			op_type TEXT NOT NULL,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			version_id TEXT,
			payload TEXT,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS oplog_hlc_idx ON oplog(hlc_ts)`,
		`CREATE INDEX IF NOT EXISTS oplog_site_hlc_idx ON oplog(site_id, hlc_ts)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV9(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS repl_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			updated_at TEXT NOT NULL,
			last_pull_hlc TEXT NOT NULL DEFAULT '',
			last_push_hlc TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func applyV10(ctx context.Context, tx *sql.Tx) error {
	ddl := []string{
		`ALTER TABLE repl_state ADD COLUMN last_pull_hlc TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE repl_state ADD COLUMN last_push_hlc TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS repl_state_remote (
			remote TEXT PRIMARY KEY,
			updated_at TEXT NOT NULL,
			last_pull_hlc TEXT NOT NULL DEFAULT '',
			last_push_hlc TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(err.Error(), "duplicate column") {
				continue
			}
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE repl_state
SET last_pull_hlc=last_hlc
WHERE COALESCE(last_pull_hlc,'')='' AND COALESCE(last_hlc,'')<>''`); err != nil {
		if !strings.Contains(err.Error(), "no such column") {
			return err
		}
	}
	return nil
}

// UpsertAPIKey inserts or updates an API key entry.
func (s *Store) UpsertAPIKey(ctx context.Context, accessKey, secretKey, policy string, enabled bool, inflightLimit int64) (err error) {
	if accessKey == "" || secretKey == "" {
		return errors.New("meta: access key and secret required")
	}
	if policy == "" {
		policy = "rw"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	_, err = tx.ExecContext(ctx, `
INSERT INTO api_keys(access_key, secret_hash, salt, enabled, created_at, label, last_used_at, secret_key, policy, inflight_limit)
VALUES(?, ?, '', ?, ?, '', '', ?, ?, ?)
ON CONFLICT(access_key) DO UPDATE SET
	secret_hash=excluded.secret_hash,
	salt=excluded.salt,
	enabled=excluded.enabled,
	secret_key=excluded.secret_key,
	policy=excluded.policy,
	inflight_limit=excluded.inflight_limit`,
		accessKey, secretKey, enabledInt, now, secretKey, policy, inflightLimit)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(oplogAPIKeyPayload{
		AccessKey:     accessKey,
		SecretKey:     secretKey,
		Enabled:       enabled,
		Policy:        policy,
		InflightLimit: inflightLimit,
		UpdatedAt:     now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "api_key", metaOplogBucket, accessKey, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateAPIKeyPolicy updates policy for an API key.
func (s *Store) UpdateAPIKeyPolicy(ctx context.Context, accessKey, policy string) (err error) {
	if accessKey == "" {
		return errors.New("meta: access key required")
	}
	if policy == "" {
		policy = "rw"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "UPDATE api_keys SET policy=? WHERE access_key=?", policy, accessKey); err != nil {
		return err
	}
	key, err := getAPIKeyTx(tx, accessKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tx.Commit()
		}
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(oplogAPIKeyPayload{
		AccessKey:     key.AccessKey,
		SecretKey:     key.SecretKey,
		Enabled:       key.Enabled,
		Policy:        policy,
		InflightLimit: key.InflightLimit,
		UpdatedAt:     now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "api_key", metaOplogBucket, accessKey, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// AllowBucketForKey adds a bucket allow entry for the given access key.
func (s *Store) AllowBucketForKey(ctx context.Context, accessKey, bucket string) error {
	return s.updateAPIKeyBucketAccess(ctx, accessKey, bucket, true)
}

// ListAllowedBuckets returns allowed buckets for the access key.
func (s *Store) ListAllowedBuckets(ctx context.Context, accessKey string) (out []string, err error) {
	if accessKey == "" {
		return nil, errors.New("meta: access key required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT bucket
FROM api_key_bucket_allow
WHERE access_key=?
ORDER BY bucket`, accessKey)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var bucket string
		if err := scan(&bucket); err != nil {
			return err
		}
		out = append(out, bucket)
		return nil
	})
}

// DisallowBucketForKey removes a bucket allow entry for the given access key.
func (s *Store) DisallowBucketForKey(ctx context.Context, accessKey, bucket string) error {
	return s.updateAPIKeyBucketAccess(ctx, accessKey, bucket, false)
}

func (s *Store) updateAPIKeyBucketAccess(ctx context.Context, accessKey, bucket string, allowed bool) (err error) {
	if accessKey == "" || bucket == "" {
		return errors.New("meta: access key and bucket required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if allowed {
		if _, err = tx.ExecContext(ctx, `
INSERT OR IGNORE INTO api_key_bucket_allow(access_key, bucket)
VALUES(?, ?)`, accessKey, bucket); err != nil {
			return err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `
DELETE FROM api_key_bucket_allow
WHERE access_key=? AND bucket=?`, accessKey, bucket); err != nil {
			return err
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(oplogAPIKeyBucketPayload{
		AccessKey: accessKey,
		Bucket:    bucket,
		Allowed:   allowed,
		UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "api_key_bucket", metaOplogBucket, accessKey, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// GetAPIKey returns stored API key metadata.
func (s *Store) GetAPIKey(ctx context.Context, accessKey string) (*APIKey, error) {
	if accessKey == "" {
		return nil, errors.New("meta: access key required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT access_key, COALESCE(secret_key,''), secret_hash, enabled, created_at, COALESCE(label,''), COALESCE(last_used_at,''), COALESCE(policy,''), COALESCE(inflight_limit,0)
FROM api_keys
WHERE access_key=?`, accessKey)
	return scanAPIKeyRow(row)
}

func getAPIKeyTx(tx *sql.Tx, accessKey string) (*APIKey, error) {
	if tx == nil {
		return nil, errors.New("meta: transaction required")
	}
	if accessKey == "" {
		return nil, errors.New("meta: access key required")
	}
	row := tx.QueryRow(`
SELECT access_key, COALESCE(secret_key,''), secret_hash, enabled, created_at, COALESCE(label,''), COALESCE(last_used_at,''), COALESCE(policy,''), COALESCE(inflight_limit,0)
FROM api_keys
WHERE access_key=?`, accessKey)
	return scanAPIKeyRow(row)
}

func scanAPIKeyRow(row *sql.Row) (*APIKey, error) {
	var key APIKey
	var secretKey string
	var secretHash string
	var enabledInt int
	if err := row.Scan(&key.AccessKey, &secretKey, &secretHash, &enabledInt, &key.CreatedAt, &key.Label, &key.LastUsedAt, &key.Policy, &key.InflightLimit); err != nil {
		return nil, err
	}
	if secretKey == "" {
		secretKey = secretHash
	}
	key.SecretKey = secretKey
	key.Enabled = enabledInt != 0
	return &key, nil
}

// LookupAPISecret returns secret + enabled state for an access key.
func (s *Store) LookupAPISecret(ctx context.Context, accessKey string) (string, bool, error) {
	key, err := s.GetAPIKey(ctx, accessKey)
	if err != nil {
		return "", false, err
	}
	return key.SecretKey, key.Enabled, nil
}

// HasAPIKeys reports whether any api_keys rows exist.
func (s *Store) HasAPIKeys(ctx context.Context) (bool, error) {
	row := s.db.QueryRowContext(ctx, "SELECT 1 FROM api_keys LIMIT 1")
	var any int
	if err := row.Scan(&any); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsBucketAllowed checks whether the access key can access the bucket.
func (s *Store) IsBucketAllowed(ctx context.Context, accessKey, bucket string) (bool, error) {
	if accessKey == "" || bucket == "" {
		return true, nil
	}
	row := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM api_key_bucket_allow WHERE access_key=?", accessKey)
	var count int
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	if count == 0 {
		return true, nil
	}
	row = s.db.QueryRowContext(ctx, "SELECT 1 FROM api_key_bucket_allow WHERE access_key=? AND bucket=? LIMIT 1", accessKey, bucket)
	var allowed int
	if err := row.Scan(&allowed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// RecordAPIKeyUse updates last_used_at for a key.
func (s *Store) RecordAPIKeyUse(ctx context.Context, accessKey string) error {
	if accessKey == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, "UPDATE api_keys SET last_used_at=? WHERE access_key=?", now, accessKey)
	return err
}

// SetAPIKeyEnabled enables or disables an API key.
func (s *Store) SetAPIKeyEnabled(ctx context.Context, accessKey string, enabled bool) (err error) {
	if accessKey == "" {
		return errors.New("meta: access key required")
	}
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "UPDATE api_keys SET enabled=? WHERE access_key=?", enabledInt, accessKey); err != nil {
		return err
	}
	key, err := getAPIKeyTx(tx, accessKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tx.Commit()
		}
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(oplogAPIKeyPayload{
		AccessKey:     key.AccessKey,
		SecretKey:     key.SecretKey,
		Enabled:       enabled,
		Policy:        key.Policy,
		InflightLimit: key.InflightLimit,
		UpdatedAt:     now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "api_key", metaOplogBucket, accessKey, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteAPIKey removes an API key and its bucket allowlist.
func (s *Store) DeleteAPIKey(ctx context.Context, accessKey string) (err error) {
	if accessKey == "" {
		return errors.New("meta: access key required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "DELETE FROM api_key_bucket_allow WHERE access_key=?", accessKey); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM api_keys WHERE access_key=?", accessKey); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(oplogAPIKeyPayload{
		AccessKey: accessKey,
		Deleted:   true,
		UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "api_key", metaOplogBucket, accessKey, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// ListAPIKeys returns all API keys ordered by access key.
func (s *Store) ListAPIKeys(ctx context.Context) (out []APIKey, err error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT access_key, COALESCE(secret_key,''), secret_hash, enabled, created_at, COALESCE(label,''), COALESCE(last_used_at,''), COALESCE(policy,''), COALESCE(inflight_limit,0)
FROM api_keys
ORDER BY access_key`)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var key APIKey
		var secretKey string
		var secretHash string
		var enabledInt int
		if err := scan(&key.AccessKey, &secretKey, &secretHash, &enabledInt, &key.CreatedAt, &key.Label, &key.LastUsedAt, &key.Policy, &key.InflightLimit); err != nil {
			return err
		}
		if secretKey == "" {
			secretKey = secretHash
		}
		key.SecretKey = secretKey
		key.Enabled = enabledInt != 0
		out = append(out, key)
		return nil
	})
}

// SetBucketPolicy sets or replaces a bucket policy.
func (s *Store) SetBucketPolicy(ctx context.Context, bucket, policy string) (err error) {
	if bucket == "" || policy == "" {
		return errors.New("meta: bucket and policy required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `
INSERT INTO bucket_policies(bucket, policy, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(bucket) DO UPDATE SET
	policy=excluded.policy,
	updated_at=excluded.updated_at`, bucket, policy, now); err != nil {
		return err
	}
	payload, err := json.Marshal(oplogBucketPolicyPayload{
		Bucket:    bucket,
		Policy:    policy,
		UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "bucket_policy", bucket, bucket, "", string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// GetBucketPolicy returns a policy string for the bucket.
func (s *Store) GetBucketPolicy(ctx context.Context, bucket string) (string, error) {
	if bucket == "" {
		return "", errors.New("meta: bucket required")
	}
	row := s.db.QueryRowContext(ctx, "SELECT policy FROM bucket_policies WHERE bucket=?", bucket)
	var policy string
	if err := row.Scan(&policy); err != nil {
		return "", err
	}
	return policy, nil
}

// DeleteBucketPolicy removes a bucket policy.
func (s *Store) DeleteBucketPolicy(ctx context.Context, bucket string) (err error) {
	if bucket == "" {
		return errors.New("meta: bucket required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "DELETE FROM bucket_policies WHERE bucket=?", bucket); err != nil {
		return err
	}
	hlcTS, _ := s.nextHLC()
	if err := s.recordOplogTx(tx, hlcTS, "bucket_policy_delete", bucket, bucket, "", ""); err != nil {
		return err
	}
	return tx.Commit()
}

// Segment holds segment metadata.
type Segment struct {
	ID             string
	Path           string
	State          string
	CreatedAt      string
	SealedAt       string
	Size           int64
	FooterChecksum []byte
}

// RecordSegment inserts or updates segment metadata.
func (s *Store) RecordSegment(ctx context.Context, segmentID, path, state string, size int64, footerChecksum []byte) error {
	if segmentID == "" || path == "" {
		return errors.New("meta: segment id and path required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	sealedAt := ""
	if state == "SEALED" {
		sealedAt = now
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO segments(segment_id, path, state, created_at, sealed_at, size, footer_checksum)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(segment_id) DO UPDATE SET
	path=excluded.path,
	state=excluded.state,
	size=excluded.size,
	footer_checksum=excluded.footer_checksum,
	created_at=segments.created_at,
	sealed_at=CASE
		WHEN excluded.state='SEALED' THEN excluded.sealed_at
		ELSE segments.sealed_at
	END`,
		segmentID, path, state, now, sealedAt, size, footerChecksum)
	return err
}

// RecordSegmentTx inserts or updates segment metadata within a transaction.
func (s *Store) RecordSegmentTx(tx *sql.Tx, segmentID, path, state string, size int64, footerChecksum []byte) error {
	if segmentID == "" || path == "" {
		return errors.New("meta: segment id and path required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	sealedAt := ""
	if state == "SEALED" {
		sealedAt = now
	}
	_, err := tx.Exec(`
INSERT INTO segments(segment_id, path, state, created_at, sealed_at, size, footer_checksum)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(segment_id) DO UPDATE SET
	path=excluded.path,
	state=excluded.state,
	size=excluded.size,
	footer_checksum=excluded.footer_checksum,
	created_at=segments.created_at,
	sealed_at=CASE
		WHEN excluded.state='SEALED' THEN excluded.sealed_at
		ELSE segments.sealed_at
	END`,
		segmentID, path, state, now, sealedAt, size, footerChecksum)
	return err
}

// RecordPut inserts a new version and updates objects_current.
func (s *Store) RecordPut(ctx context.Context, bucket, key, versionID, etag string, size int64, manifestPath string) error {
	if bucket == "" || key == "" {
		return errors.New("meta: bucket and key required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	hlcTS, siteID := s.nextHLC()

	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO buckets(bucket, created_at) VALUES(?, ?)", bucket, now); err != nil {
		return err
	}
	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         etag,
		Size:         size,
		LastModified: now,
	})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
INSERT INTO versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`,
		versionID, bucket, key, etag, size, now, hlcTS, siteID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
INSERT INTO objects_current(bucket, key, version_id)
VALUES(?, ?, ?)
ON CONFLICT(bucket, key) DO UPDATE SET version_id=excluded.version_id`,
		bucket, key, versionID); err != nil {
		return err
	}
	if manifestPath != "" {
		if _, err = tx.ExecContext(ctx, `
INSERT INTO manifests(version_id, path) VALUES(?, ?)
ON CONFLICT(version_id) DO UPDATE SET path=excluded.path`,
			versionID, manifestPath); err != nil {
			return err
		}
	}
	if err := s.recordOplogTx(tx, hlcTS, "put", bucket, key, versionID, string(putPayload)); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordPutTx inserts a new version and updates objects_current within a transaction.
func (s *Store) RecordPutTx(tx *sql.Tx, bucket, key, versionID, etag string, size int64, manifestPath string) error {
	if bucket == "" || key == "" {
		return errors.New("meta: bucket and key required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	hlcTS, siteID := s.nextHLC()
	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         etag,
		Size:         size,
		LastModified: now,
	})
	if err != nil {
		return err
	}

	if _, err := tx.Exec("INSERT OR IGNORE INTO buckets(bucket, created_at) VALUES(?, ?)", bucket, now); err != nil {
		return err
	}
	if _, err := tx.Exec(`
INSERT INTO versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`,
		versionID, bucket, key, etag, size, now, hlcTS, siteID); err != nil {
		return err
	}
	if _, err := tx.Exec(`
INSERT INTO objects_current(bucket, key, version_id)
VALUES(?, ?, ?)
ON CONFLICT(bucket, key) DO UPDATE SET version_id=excluded.version_id`,
		bucket, key, versionID); err != nil {
		return err
	}
	if manifestPath != "" {
		if _, err := tx.Exec(`
INSERT INTO manifests(version_id, path) VALUES(?, ?)
ON CONFLICT(version_id) DO UPDATE SET path=excluded.path`,
			versionID, manifestPath); err != nil {
			return err
		}
	}
	if err := s.recordOplogTx(tx, hlcTS, "put", bucket, key, versionID, string(putPayload)); err != nil {
		return err
	}
	return nil
}

// RecordMPUComplete records an MPU completion in the oplog.
func (s *Store) RecordMPUComplete(ctx context.Context, bucket, key, versionID, etag string, size int64) error {
	if bucket == "" || key == "" || versionID == "" {
		return errors.New("meta: bucket, key, and version id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	hlcTS, _ := s.nextHLC()
	payload, err := json.Marshal(oplogMPUCompletePayload{
		ETag:         etag,
		Size:         size,
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	if err := s.recordOplogTx(tx, hlcTS, "mpu_complete", bucket, key, versionID, string(payload)); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordManifestTx records a manifest path for a version id.
func (s *Store) RecordManifestTx(tx *sql.Tx, versionID, manifestPath string) error {
	if versionID == "" || manifestPath == "" {
		return errors.New("meta: version id and manifest path required")
	}
	if tx == nil {
		return errors.New("meta: transaction required")
	}
	if _, err := tx.Exec(`
INSERT INTO manifests(version_id, path) VALUES(?, ?)
ON CONFLICT(version_id) DO UPDATE SET path=excluded.path`,
		versionID, manifestPath); err != nil {
		return err
	}
	return nil
}

// RecordManifest records a manifest path for a version id.
func (s *Store) RecordManifest(ctx context.Context, versionID, manifestPath string) error {
	if versionID == "" || manifestPath == "" {
		return errors.New("meta: version id and manifest path required")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO manifests(version_id, path) VALUES(?, ?)
ON CONFLICT(version_id) DO UPDATE SET path=excluded.path`, versionID, manifestPath)
	return err
}

func (s *Store) recordOplogTx(tx *sql.Tx, hlcTS, opType, bucket, key, versionID, payload string) error {
	if tx == nil {
		return errors.New("meta: transaction required")
	}
	if bucket == "" || key == "" {
		return errors.New("meta: bucket and key required")
	}
	if opType == "" {
		return errors.New("meta: op type required")
	}
	siteID := s.siteID
	if siteID == "" {
		siteID = "local"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := tx.Exec(`
INSERT INTO oplog(site_id, hlc_ts, op_type, bucket, key, version_id, payload, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		siteID, hlcTS, opType, bucket, key, versionID, payload, now)
	return err
}

func (s *Store) insertOplogEntryTx(tx *sql.Tx, entry OplogEntry) (bool, error) {
	if tx == nil {
		return false, errors.New("meta: transaction required")
	}
	if entry.SiteID == "" || entry.HLCTS == "" || entry.OpType == "" || entry.Bucket == "" || entry.Key == "" {
		return false, errors.New("meta: oplog entry missing required fields")
	}
	var exists int
	err := tx.QueryRow(`
SELECT 1 FROM oplog
WHERE site_id=? AND hlc_ts=? AND op_type=? AND bucket=? AND key=? AND COALESCE(version_id,'')=COALESCE(?, '')
LIMIT 1`,
		entry.SiteID, entry.HLCTS, entry.OpType, entry.Bucket, entry.Key, entry.VersionID).Scan(&exists)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	createdAt := entry.CreatedAt
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err = tx.Exec(`
INSERT INTO oplog(site_id, hlc_ts, op_type, bucket, key, version_id, payload, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.SiteID, entry.HLCTS, entry.OpType, entry.Bucket, entry.Key, entry.VersionID, entry.Payload, createdAt)
	if err != nil {
		return false, err
	}
	return true, nil
}

func compareHLC(hlcA, siteA, hlcB, siteB string) int {
	if hlcA == hlcB {
		switch {
		case siteA == siteB:
			return 0
		case siteA > siteB:
			return 1
		default:
			return -1
		}
	}
	if hlcA > hlcB {
		return 1
	}
	return -1
}

func latestVersionHLC(tx *sql.Tx, bucket, key string) (string, string, bool, error) {
	if tx == nil {
		return "", "", false, errors.New("meta: transaction required")
	}
	var hlc string
	var site string
	err := tx.QueryRow(`
SELECT COALESCE(hlc_ts,''), COALESCE(site_id,'')
FROM versions
WHERE bucket=? AND key=?
ORDER BY hlc_ts DESC, site_id DESC
LIMIT 1`, bucket, key).Scan(&hlc, &site)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return hlc, site, true, nil
}

// ApplyOplogEntries applies replication oplog entries using LWW + site_id tie-break.
func (s *Store) ApplyOplogEntries(ctx context.Context, entries []OplogEntry) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("meta: db not initialized")
	}
	if len(entries) == 0 {
		return 0, nil
	}
	applied := 0
	err := s.WithTx(func(tx *sql.Tx) error {
		for _, entry := range entries {
			if entry.SiteID == "" || entry.HLCTS == "" || entry.OpType == "" || entry.Bucket == "" || entry.Key == "" {
				return errors.New("meta: invalid oplog entry")
			}
			inserted, err := s.insertOplogEntryTx(tx, entry)
			if err != nil {
				return err
			}
			if !inserted {
				continue
			}
			switch entry.OpType {
			case "put", "mpu_complete":
				if entry.VersionID == "" {
					if entry.OpType == "mpu_complete" {
						return errors.New("meta: mpu_complete entry requires version id")
					}
					return errors.New("meta: put entry requires version id")
				}
				if _, err := tx.Exec("INSERT OR IGNORE INTO buckets(bucket, created_at) VALUES(?, ?)", entry.Bucket, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
					return err
				}
				var payload oplogPutPayload
				if entry.Payload != "" {
					if entry.OpType == "mpu_complete" {
						var mpuPayload oplogMPUCompletePayload
						if err := json.Unmarshal([]byte(entry.Payload), &mpuPayload); err != nil {
							return err
						}
						payload = oplogPutPayload(mpuPayload)
					} else {
						if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
							return err
						}
					}
				}
				lastModified := payload.LastModified
				if lastModified == "" {
					lastModified = time.Now().UTC().Format(time.RFC3339Nano)
				}
				if _, err := tx.Exec(`
INSERT OR IGNORE INTO versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`,
					entry.VersionID, entry.Bucket, entry.Key, payload.ETag, payload.Size, lastModified, entry.HLCTS, entry.SiteID); err != nil {
					return err
				}
				var currentVersion string
				var currentHLC string
				var currentSite string
				err := tx.QueryRow(`
SELECT o.version_id, COALESCE(v.hlc_ts,''), COALESCE(v.site_id,'')
FROM objects_current o
LEFT JOIN versions v ON o.version_id=v.version_id
WHERE o.bucket=? AND o.key=?`, entry.Bucket, entry.Key).Scan(&currentVersion, &currentHLC, &currentSite)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					latestHLC, latestSite, ok, err := latestVersionHLC(tx, entry.Bucket, entry.Key)
					if err != nil {
						return err
					}
					if ok && compareHLC(entry.HLCTS, entry.SiteID, latestHLC, latestSite) < 0 {
						continue
					}
				}
				if errors.Is(err, sql.ErrNoRows) || compareHLC(entry.HLCTS, entry.SiteID, currentHLC, currentSite) > 0 {
					if _, err := tx.Exec(`
INSERT INTO objects_current(bucket, key, version_id)
VALUES(?, ?, ?)
ON CONFLICT(bucket, key) DO UPDATE SET version_id=excluded.version_id`,
						entry.Bucket, entry.Key, entry.VersionID); err != nil {
						return err
					}
				}
			case "delete":
				if entry.VersionID == "" {
					return errors.New("meta: delete entry requires version id")
				}
				var payload oplogDeletePayload
				if entry.Payload != "" {
					if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
						return err
					}
				}
				lastModified := payload.LastModified
				if lastModified == "" {
					lastModified = time.Now().UTC().Format(time.RFC3339Nano)
				}
				res, err := tx.Exec(`
UPDATE versions SET state='DELETED', hlc_ts=?, site_id=? WHERE version_id=?`,
					entry.HLCTS, entry.SiteID, entry.VersionID)
				if err != nil {
					return err
				}
				affected, _ := res.RowsAffected()
				if affected == 0 {
					if _, err := tx.Exec(`
INSERT INTO versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, '', 0, ?, ?, ?, 'DELETED')`,
						entry.VersionID, entry.Bucket, entry.Key, lastModified, entry.HLCTS, entry.SiteID); err != nil {
						return err
					}
				}
				var currentVersion string
				var currentHLC string
				var currentSite string
				err = tx.QueryRow(`
SELECT o.version_id, COALESCE(v.hlc_ts,''), COALESCE(v.site_id,'')
FROM objects_current o
LEFT JOIN versions v ON o.version_id=v.version_id
WHERE o.bucket=? AND o.key=?`, entry.Bucket, entry.Key).Scan(&currentVersion, &currentHLC, &currentSite)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if !errors.Is(err, sql.ErrNoRows) && compareHLC(entry.HLCTS, entry.SiteID, currentHLC, currentSite) >= 0 {
					if _, err := tx.Exec("DELETE FROM objects_current WHERE bucket=? AND key=?", entry.Bucket, entry.Key); err != nil {
						return err
					}
				}
			case "bucket_policy":
				var payload oplogBucketPolicyPayload
				if entry.Payload == "" {
					return errors.New("meta: bucket_policy payload required")
				}
				if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
					return err
				}
				if payload.Bucket == "" {
					payload.Bucket = entry.Bucket
				}
				_, err := tx.Exec(`
INSERT INTO bucket_policies(bucket, policy, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(bucket) DO UPDATE SET policy=excluded.policy, updated_at=excluded.updated_at`,
					payload.Bucket, payload.Policy, payload.UpdatedAt)
				if err != nil {
					return err
				}
			case "bucket_policy_delete":
				if entry.Bucket == "" {
					return errors.New("meta: bucket required")
				}
				_, err := tx.Exec(`DELETE FROM bucket_policies WHERE bucket=?`, entry.Bucket)
				if err != nil {
					return err
				}
			case "api_key":
				var payload oplogAPIKeyPayload
				if entry.Payload == "" {
					return errors.New("meta: api_key payload required")
				}
				if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
					return err
				}
				if payload.AccessKey == "" {
					payload.AccessKey = entry.Key
				}
				if payload.Deleted {
					if _, err := tx.Exec(`DELETE FROM api_keys WHERE access_key=?`, payload.AccessKey); err != nil {
						return err
					}
					if _, err := tx.Exec(`DELETE FROM api_key_bucket_allow WHERE access_key=?`, payload.AccessKey); err != nil {
						return err
					}
					break
				}
				enabledInt := 0
				if payload.Enabled {
					enabledInt = 1
				}
				_, err := tx.Exec(`
INSERT INTO api_keys(access_key, secret_hash, salt, enabled, created_at, label, last_used_at, secret_key, policy, inflight_limit)
VALUES(?, ?, '', ?, ?, '', '', ?, ?, ?)
ON CONFLICT(access_key) DO UPDATE SET
	secret_hash=excluded.secret_hash,
	salt=excluded.salt,
	enabled=excluded.enabled,
	secret_key=excluded.secret_key,
	policy=excluded.policy,
	inflight_limit=excluded.inflight_limit`,
					payload.AccessKey, payload.SecretKey, enabledInt, payload.UpdatedAt, payload.SecretKey, payload.Policy, payload.InflightLimit)
				if err != nil {
					return err
				}
			case "api_key_bucket":
				var payload oplogAPIKeyBucketPayload
				if entry.Payload == "" {
					return errors.New("meta: api_key_bucket payload required")
				}
				if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
					return err
				}
				if payload.AccessKey == "" {
					payload.AccessKey = entry.Key
				}
				if payload.Allowed {
					_, err := tx.Exec(`INSERT OR IGNORE INTO api_key_bucket_allow(access_key, bucket) VALUES(?, ?)`, payload.AccessKey, payload.Bucket)
					if err != nil {
						return err
					}
				} else {
					_, err := tx.Exec(`DELETE FROM api_key_bucket_allow WHERE access_key=? AND bucket=?`, payload.AccessKey, payload.Bucket)
					if err != nil {
						return err
					}
				}
			default:
				return errors.New("meta: unknown oplog op")
			}
			applied++
		}
		return nil
	})
	return applied, err
}

// ListOplog returns all oplog entries ordered by insert id.
func (s *Store) ListOplog(ctx context.Context) (out []OplogEntry, err error) {
	if s == nil || s.db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, site_id, hlc_ts, op_type, bucket, key, COALESCE(version_id,''), COALESCE(payload,''), created_at
FROM oplog
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var entry OplogEntry
		if err := scan(&entry.ID, &entry.SiteID, &entry.HLCTS, &entry.OpType, &entry.Bucket, &entry.Key, &entry.VersionID, &entry.Payload, &entry.CreatedAt); err != nil {
			return err
		}
		out = append(out, entry)
		return nil
	})
}

// ListOplogSince returns oplog entries with hlc_ts greater than the provided value.
func (s *Store) ListOplogSince(ctx context.Context, since string, limit int) (out []OplogEntry, err error) {
	if s == nil || s.db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	if limit <= 0 {
		limit = 1000
	}
	var rows *sql.Rows
	if since == "" {
		rows, err = s.db.QueryContext(ctx, `
SELECT id, site_id, hlc_ts, op_type, bucket, key, COALESCE(version_id,''), COALESCE(payload,''), created_at
FROM oplog
ORDER BY hlc_ts, id
LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
SELECT id, site_id, hlc_ts, op_type, bucket, key, COALESCE(version_id,''), COALESCE(payload,''), created_at
FROM oplog
WHERE hlc_ts > ?
ORDER BY hlc_ts, id
LIMIT ?`, since, limit)
	}
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var entry OplogEntry
		if err := scan(&entry.ID, &entry.SiteID, &entry.HLCTS, &entry.OpType, &entry.Bucket, &entry.Key, &entry.VersionID, &entry.Payload, &entry.CreatedAt); err != nil {
			return err
		}
		out = append(out, entry)
		return nil
	})
}

// MaxOplogHLC returns the max HLC value recorded in oplog.
func (s *Store) MaxOplogHLC(ctx context.Context) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("meta: db not initialized")
	}
	var hlc string
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(hlc_ts),'') FROM oplog").Scan(&hlc); err != nil {
		return "", err
	}
	return hlc, nil
}

// GetReplWatermark returns the last stored replication HLC watermark.
func (s *Store) GetReplWatermark(ctx context.Context) (string, error) {
	return s.GetReplPullWatermark(ctx)
}

// GetReplPullWatermark returns the last stored replication pull HLC watermark.
func (s *Store) GetReplPullWatermark(ctx context.Context) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("meta: db not initialized")
	}
	var hlc string
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(last_pull_hlc,'') FROM repl_state WHERE id=1").Scan(&hlc)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return hlc, nil
}

// SetReplWatermark stores the last replication HLC watermark.
func (s *Store) SetReplWatermark(ctx context.Context, hlc string) error {
	return s.SetReplPullWatermark(ctx, hlc)
}

// GetReplPushWatermark returns the last stored replication push HLC watermark.
func (s *Store) GetReplPushWatermark(ctx context.Context) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("meta: db not initialized")
	}
	var hlc string
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(last_push_hlc,'') FROM repl_state WHERE id=1").Scan(&hlc)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return hlc, nil
}

// SetReplPullWatermark stores the last replication pull HLC watermark.
func (s *Store) SetReplPullWatermark(ctx context.Context, hlc string) error {
	if s == nil || s.db == nil {
		return errors.New("meta: db not initialized")
	}
	if hlc == "" {
		return errors.New("meta: repl watermark required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO repl_state(id, updated_at, last_pull_hlc, last_push_hlc)
VALUES(1, ?, ?, '')
ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, last_pull_hlc=excluded.last_pull_hlc`, now, hlc)
	return err
}

// SetReplPushWatermark stores the last replication push HLC watermark.
func (s *Store) SetReplPushWatermark(ctx context.Context, hlc string) error {
	if s == nil || s.db == nil {
		return errors.New("meta: db not initialized")
	}
	if hlc == "" {
		return errors.New("meta: repl watermark required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO repl_state(id, updated_at, last_pull_hlc, last_push_hlc)
VALUES(1, ?, '', ?)
ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, last_push_hlc=excluded.last_push_hlc`, now, hlc)
	return err
}

// GetReplRemoteState returns replication watermarks for a specific remote.
func (s *Store) GetReplRemoteState(ctx context.Context, remote string) (*ReplRemoteState, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	if remote == "" {
		return nil, errors.New("meta: remote required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT remote, updated_at, COALESCE(last_pull_hlc,''), COALESCE(last_push_hlc,'')
FROM repl_state_remote
WHERE remote=?`, remote)
	var state ReplRemoteState
	if err := row.Scan(&state.Remote, &state.UpdatedAt, &state.LastPullHLC, &state.LastPushHLC); err != nil {
		return nil, err
	}
	return &state, nil
}

// ListReplRemoteStates returns all replication remote states ordered by remote.
func (s *Store) ListReplRemoteStates(ctx context.Context) (out []ReplRemoteState, err error) {
	if s == nil || s.db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT remote, updated_at, COALESCE(last_pull_hlc,''), COALESCE(last_push_hlc,'')
FROM repl_state_remote
ORDER BY remote`)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var state ReplRemoteState
		if err := scan(&state.Remote, &state.UpdatedAt, &state.LastPullHLC, &state.LastPushHLC); err != nil {
			return err
		}
		out = append(out, state)
		return nil
	})
}

// GetReplRemotePullWatermark returns pull watermark for a remote.
func (s *Store) GetReplRemotePullWatermark(ctx context.Context, remote string) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("meta: db not initialized")
	}
	if remote == "" {
		return "", errors.New("meta: remote required")
	}
	var hlc string
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(last_pull_hlc,'')
FROM repl_state_remote
WHERE remote=?`, remote).Scan(&hlc)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return hlc, nil
}

// GetReplRemotePushWatermark returns push watermark for a remote.
func (s *Store) GetReplRemotePushWatermark(ctx context.Context, remote string) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("meta: db not initialized")
	}
	if remote == "" {
		return "", errors.New("meta: remote required")
	}
	var hlc string
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(last_push_hlc,'')
FROM repl_state_remote
WHERE remote=?`, remote).Scan(&hlc)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return hlc, nil
}

// SetReplRemotePullWatermark stores replication pull watermark for a remote.
func (s *Store) SetReplRemotePullWatermark(ctx context.Context, remote, hlc string) error {
	if s == nil || s.db == nil {
		return errors.New("meta: db not initialized")
	}
	if remote == "" || hlc == "" {
		return errors.New("meta: remote and watermark required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO repl_state_remote(remote, updated_at, last_pull_hlc, last_push_hlc)
VALUES(?, ?, ?, '')
ON CONFLICT(remote) DO UPDATE SET updated_at=excluded.updated_at, last_pull_hlc=excluded.last_pull_hlc`, remote, now, hlc)
	return err
}

// SetReplRemotePushWatermark stores replication push watermark for a remote.
func (s *Store) SetReplRemotePushWatermark(ctx context.Context, remote, hlc string) error {
	if s == nil || s.db == nil {
		return errors.New("meta: db not initialized")
	}
	if remote == "" || hlc == "" {
		return errors.New("meta: remote and watermark required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO repl_state_remote(remote, updated_at, last_pull_hlc, last_push_hlc)
VALUES(?, ?, '', ?)
ON CONFLICT(remote) DO UPDATE SET updated_at=excluded.updated_at, last_push_hlc=excluded.last_push_hlc`, remote, now, hlc)
	return err
}

// MarkDamaged sets version state to DAMAGED.
func (s *Store) MarkDamaged(ctx context.Context, versionID string) error {
	if versionID == "" {
		return errors.New("meta: version id required")
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE versions SET state='DAMAGED' WHERE version_id=?`, versionID)
	return err
}

// CurrentVersion returns the current version id for a key.
func (s *Store) CurrentVersion(ctx context.Context, bucket, key string) (string, error) {
	var versionID string
	err := s.db.QueryRowContext(ctx, "SELECT version_id FROM objects_current WHERE bucket=? AND key=?", bucket, key).Scan(&versionID)
	if err != nil {
		return "", err
	}
	return versionID, nil
}

// ManifestPath returns the manifest path for a version id.
func (s *Store) ManifestPath(ctx context.Context, versionID string) (string, error) {
	if versionID == "" {
		return "", errors.New("meta: version id required")
	}
	var path string
	err := s.db.QueryRowContext(ctx, "SELECT path FROM manifests WHERE version_id=?", versionID).Scan(&path)
	if err != nil {
		return "", err
	}
	return path, nil
}

// MultipartUpload holds upload metadata.
type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	CreatedAt string
	State     string
}

// MultipartPart holds part metadata.
type MultipartPart struct {
	UploadID     string
	PartNumber   int
	VersionID    string
	ETag         string
	Size         int64
	LastModified string
}

// CreateMultipartUpload creates an upload and returns its id.
func (s *Store) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if bucket == "" || key == "" || uploadID == "" {
		return errors.New("meta: bucket, key, and upload id required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO multipart_uploads(upload_id, bucket, key, created_at, state)
VALUES(?, ?, ?, ?, 'ACTIVE')`, uploadID, bucket, key, now)
	return err
}

// ListMultipartUploads returns active uploads for a bucket and optional prefix/markers.
func (s *Store) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (out []MultipartUpload, err error) {
	if limit <= 0 {
		limit = 1000
	}
	pattern := escapeLike(prefix) + "%"
	rows, err := queryWithMarkers(ctx, s.db,
		`
SELECT upload_id, bucket, key, created_at, state
FROM multipart_uploads
WHERE bucket=? AND key LIKE ? ESCAPE '\' AND state='ACTIVE'`,
		"key", "upload_id", bucket, pattern, keyMarker, uploadIDMarker, limit,
	)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var up MultipartUpload
		if err := scan(&up.UploadID, &up.Bucket, &up.Key, &up.CreatedAt, &up.State); err != nil {
			return err
		}
		out = append(out, up)
		return nil
	})
}

// GetMultipartUpload returns upload metadata.
func (s *Store) GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT upload_id, bucket, key, created_at, state
FROM multipart_uploads
WHERE upload_id=?`, uploadID)
	var up MultipartUpload
	if err := row.Scan(&up.UploadID, &up.Bucket, &up.Key, &up.CreatedAt, &up.State); err != nil {
		return nil, err
	}
	return &up, nil
}

// AbortMultipartUpload marks an upload as aborted.
func (s *Store) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return errors.New("meta: upload id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "DELETE FROM multipart_parts WHERE upload_id=?", uploadID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM multipart_uploads WHERE upload_id=?", uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteMultipartUpload marks an upload as completed and clears its parts.
func (s *Store) CompleteMultipartUpload(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return errors.New("meta: upload id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `
UPDATE multipart_uploads SET state='COMPLETED' WHERE upload_id=?`, uploadID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM multipart_parts WHERE upload_id=?", uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// PutMultipartPart records or replaces a part.
func (s *Store) PutMultipartPart(ctx context.Context, uploadID string, partNumber int, versionID, etag string, size int64) error {
	if uploadID == "" || partNumber <= 0 || versionID == "" {
		return errors.New("meta: invalid part")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO multipart_parts(upload_id, part_number, version_id, etag, size, last_modified_utc)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(upload_id, part_number) DO UPDATE SET
	version_id=excluded.version_id,
	etag=excluded.etag,
	size=excluded.size,
	last_modified_utc=excluded.last_modified_utc`,
		uploadID, partNumber, versionID, etag, size, now)
	return err
}

// ListMultipartParts returns parts ordered by part number.
func (s *Store) ListMultipartParts(ctx context.Context, uploadID string) (out []MultipartPart, err error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT upload_id, part_number, version_id, etag, size, last_modified_utc
FROM multipart_parts
WHERE upload_id=?
ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var part MultipartPart
		if err := rows.Scan(&part.UploadID, &part.PartNumber, &part.VersionID, &part.ETag, &part.Size, &part.LastModified); err != nil {
			return nil, err
		}
		out = append(out, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListMultipartUploadsBefore returns active uploads created before cutoff.
func (s *Store) ListMultipartUploadsBefore(ctx context.Context, cutoff time.Time) (out []MultipartUpload, err error) {
	ts := cutoff.UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
SELECT upload_id, bucket, key, created_at, state
FROM multipart_uploads
WHERE state='ACTIVE' AND created_at < ?
ORDER BY created_at`, ts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var up MultipartUpload
		if err := rows.Scan(&up.UploadID, &up.Bucket, &up.Key, &up.CreatedAt, &up.State); err != nil {
			return nil, err
		}
		out = append(out, up)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// MultipartUploadStats returns part count and total bytes for an upload.
func (s *Store) MultipartUploadStats(ctx context.Context, uploadID string) (int, int64, error) {
	if uploadID == "" {
		return 0, 0, errors.New("meta: upload id required")
	}
	row := s.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(size),0) FROM multipart_parts WHERE upload_id=?", uploadID)
	var parts int
	var bytes int64
	if err := row.Scan(&parts, &bytes); err != nil {
		return 0, 0, err
	}
	return parts, bytes, nil
}

// DeleteMultipartUpload removes an upload and its parts.
func (s *Store) DeleteMultipartUpload(ctx context.Context, uploadID string) (int, int64, error) {
	if uploadID == "" {
		return 0, 0, errors.New("meta: upload id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var partsCount int
	var totalBytes int64
	row := tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(size),0) FROM multipart_parts WHERE upload_id=?", uploadID)
	if err = row.Scan(&partsCount, &totalBytes); err != nil {
		return 0, 0, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM multipart_parts WHERE upload_id=?", uploadID); err != nil {
		return 0, 0, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM multipart_uploads WHERE upload_id=?", uploadID); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return partsCount, totalBytes, nil
}

// ListMultipartPartManifestPaths returns manifest paths for active multipart parts.
func (s *Store) ListMultipartPartManifestPaths(ctx context.Context) (out []string, err error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT DISTINCT m.path
FROM multipart_parts p
JOIN multipart_uploads u ON u.upload_id = p.upload_id
JOIN manifests m ON m.version_id = p.version_id
WHERE u.state='ACTIVE'`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ObjectMeta describes the current object version metadata.
type ObjectMeta struct {
	Key          string
	VersionID    string
	ETag         string
	Size         int64
	LastModified string
	State        string
}

// GetObjectMeta returns metadata for the current object version.
func (s *Store) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT v.version_id, v.etag, v.size, v.last_modified_utc, v.state
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key=?`, bucket, key)
	var meta ObjectMeta
	meta.Key = key
	if err := row.Scan(&meta.VersionID, &meta.ETag, &meta.Size, &meta.LastModified, &meta.State); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetObjectVersion returns metadata for a specific object version.
func (s *Store) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*ObjectMeta, error) {
	if bucket == "" || key == "" || versionID == "" {
		return nil, errors.New("meta: bucket, key, and version id required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT version_id, etag, size, last_modified_utc, state
FROM versions
WHERE bucket=? AND key=? AND version_id=?`, bucket, key, versionID)
	var meta ObjectMeta
	meta.Key = key
	if err := row.Scan(&meta.VersionID, &meta.ETag, &meta.Size, &meta.LastModified, &meta.State); err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListObjects returns current objects for a bucket with optional prefix and continuation key/version.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, afterKey, afterVersion string, limit int) (out []ObjectMeta, err error) {
	if limit <= 0 {
		limit = 1000
	}
	pattern := escapeLike(prefix) + "%"
	rows, err := queryWithMarkers(ctx, s.db,
		`
SELECT o.key, v.version_id, v.etag, v.size, v.last_modified_utc
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key LIKE ? ESCAPE '\'`,
		"o.key", "v.version_id", bucket, pattern, afterKey, afterVersion, limit,
	)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var meta ObjectMeta
		if err := scan(&meta.Key, &meta.VersionID, &meta.ETag, &meta.Size, &meta.LastModified); err != nil {
			return err
		}
		out = append(out, meta)
		return nil
	})
}

func escapeLike(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '%', '_', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func queryWithMarkers(ctx context.Context, db *sql.DB, baseQuery, primary, secondary string, bucket, pattern, keyMarker, secondaryMarker string, limit int) (*sql.Rows, error) {
	if db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	if keyMarker != "" && secondaryMarker != "" {
		query := baseQuery + " AND (" + primary + " > ? OR (" + primary + " = ? AND " + secondary + " > ?)) ORDER BY " + primary + ", " + secondary + " LIMIT ?"
		return db.QueryContext(ctx, query, bucket, pattern, keyMarker, keyMarker, secondaryMarker, limit)
	}
	if keyMarker != "" {
		query := baseQuery + " AND " + primary + " > ? ORDER BY " + primary + ", " + secondary + " LIMIT ?"
		return db.QueryContext(ctx, query, bucket, pattern, keyMarker, limit)
	}
	query := baseQuery + " ORDER BY " + primary + ", " + secondary + " LIMIT ?"
	return db.QueryContext(ctx, query, bucket, pattern, limit)
}

func scanRows(rows *sql.Rows, scanFn func(scan func(dest ...any) error) error) (err error) {
	if rows == nil {
		return nil
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		if err := scanFn(rows.Scan); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// GetSegment returns segment metadata.
func (s *Store) GetSegment(ctx context.Context, segmentID string) (*Segment, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT segment_id, path, state, created_at, COALESCE(sealed_at, ''), COALESCE(size, 0), COALESCE(footer_checksum, x'')
FROM segments
WHERE segment_id=?`, segmentID)
	var seg Segment
	if err := row.Scan(&seg.ID, &seg.Path, &seg.State, &seg.CreatedAt, &seg.SealedAt, &seg.Size, &seg.FooterChecksum); err != nil {
		return nil, err
	}
	return &seg, nil
}

// ListSegments returns segment metadata.
func (s *Store) ListSegments(ctx context.Context) (out []Segment, err error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT segment_id, path, state, created_at, COALESCE(sealed_at, ''), COALESCE(size, 0), COALESCE(footer_checksum, x'')
FROM segments`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var seg Segment
		if err := rows.Scan(&seg.ID, &seg.Path, &seg.State, &seg.CreatedAt, &seg.SealedAt, &seg.Size, &seg.FooterChecksum); err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RecordOpsRun stores a completed ops report for observability.
func (s *Store) RecordOpsRun(ctx context.Context, mode string, report *ReportOps) error {
	if s == nil || s.db == nil || report == nil {
		return nil
	}
	if mode == "" || report.FinishedAt == "" {
		return errors.New("meta: ops run requires mode and finished_at")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO ops_runs(mode, finished_at, errors, warnings, candidates, candidate_bytes, deleted, reclaimed_bytes, rewritten_segments, rewritten_bytes, new_segments)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		mode, report.FinishedAt, report.Errors, report.Warnings, report.Candidates, report.CandidateBytes, report.Deleted, report.ReclaimedBytes, report.RewrittenSegments, report.RewrittenBytes, report.NewSegments)
	return err
}

// ReportOps is a slimmed view of ops.Report for storage.
type ReportOps struct {
	FinishedAt        string
	Errors            int
	Warnings          int
	Candidates        int
	CandidateBytes    int64
	Deleted           int
	ReclaimedBytes    int64
	RewrittenSegments int
	RewrittenBytes    int64
	NewSegments       int
}

// Stats aggregates minimal metrics for /v1/meta/stats.
type Stats struct {
	Objects            int64  `json:"objects"`
	Segments           int64  `json:"segments"`
	BytesLive          int64  `json:"bytes_live"`
	LastFsckAt         string `json:"last_fsck_at,omitempty"`
	LastFsckErrors     int    `json:"last_fsck_errors,omitempty"`
	LastScrubAt        string `json:"last_scrub_at,omitempty"`
	LastScrubErrors    int    `json:"last_scrub_errors,omitempty"`
	LastGCAt           string `json:"last_gc_at,omitempty"`
	LastGCErrors       int    `json:"last_gc_errors,omitempty"`
	LastGCReclaimed    int64  `json:"last_gc_reclaimed_bytes,omitempty"`
	LastGCRewritten    int64  `json:"last_gc_rewritten_bytes,omitempty"`
	LastGCNewSegments  int    `json:"last_gc_new_segments,omitempty"`
	LastMPUGCAt        string `json:"last_mpu_gc_at,omitempty"`
	LastMPUGCErrors    int    `json:"last_mpu_gc_errors,omitempty"`
	LastMPUGCDeleted   int    `json:"last_mpu_gc_deleted,omitempty"`
	LastMPUGCReclaimed int64  `json:"last_mpu_gc_reclaimed_bytes,omitempty"`
}

// ReplStat describes replication state and lag per remote.
type ReplStat struct {
	Remote         string  `json:"remote"`
	LastPullHLC    string  `json:"last_pull_hlc,omitempty"`
	LastPushHLC    string  `json:"last_push_hlc,omitempty"`
	PullLagSeconds float64 `json:"pull_lag_seconds,omitempty"`
	PushLagSeconds float64 `json:"push_lag_seconds,omitempty"`
	PushBacklog    int64   `json:"push_backlog,omitempty"`
	LastOplogHLC   string  `json:"last_oplog_hlc,omitempty"`
}

// GCTrend captures recent GC outcomes for trend reporting.
type GCTrend struct {
	Mode           string  `json:"mode"`
	FinishedAt     string  `json:"finished_at"`
	Errors         int     `json:"errors"`
	Deleted        int     `json:"deleted,omitempty"`
	ReclaimedBytes int64   `json:"reclaimed_bytes,omitempty"`
	RewrittenBytes int64   `json:"rewritten_bytes,omitempty"`
	NewSegments    int     `json:"new_segments,omitempty"`
	ReclaimRate    float64 `json:"reclaim_rate,omitempty"`
}

// GetStats returns aggregate counts.
func (s *Store) GetStats(ctx context.Context) (*Stats, error) {
	stats := &Stats{}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects_current").Scan(&stats.Objects); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM segments").Scan(&stats.Segments); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(size),0) FROM versions WHERE state='ACTIVE'").Scan(&stats.BytesLive); err != nil {
		return nil, err
	}
	_ = s.db.QueryRowContext(ctx, `
SELECT finished_at, errors
FROM ops_runs
WHERE mode='fsck'
ORDER BY finished_at DESC
LIMIT 1`).Scan(&stats.LastFsckAt, &stats.LastFsckErrors)
	_ = s.db.QueryRowContext(ctx, `
SELECT finished_at, errors
FROM ops_runs
WHERE mode='scrub'
ORDER BY finished_at DESC
LIMIT 1`).Scan(&stats.LastScrubAt, &stats.LastScrubErrors)
	_ = s.db.QueryRowContext(ctx, `
SELECT finished_at, errors, COALESCE(reclaimed_bytes,0), COALESCE(rewritten_bytes,0), COALESCE(new_segments,0)
FROM ops_runs
WHERE mode LIKE 'gc-%' AND mode NOT LIKE 'gc-plan%'
ORDER BY finished_at DESC
LIMIT 1`).Scan(&stats.LastGCAt, &stats.LastGCErrors, &stats.LastGCReclaimed, &stats.LastGCRewritten, &stats.LastGCNewSegments)
	_ = s.db.QueryRowContext(ctx, `
SELECT finished_at, errors, COALESCE(deleted,0), COALESCE(reclaimed_bytes,0)
FROM ops_runs
WHERE mode='mpu-gc-run'
ORDER BY finished_at DESC
LIMIT 1`).Scan(&stats.LastMPUGCAt, &stats.LastMPUGCErrors, &stats.LastMPUGCDeleted, &stats.LastMPUGCReclaimed)
	return stats, nil
}

// GetReplStats returns replication state and lag metrics.
func (s *Store) GetReplStats(ctx context.Context) ([]ReplStat, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("meta: db not initialized")
	}
	var lastOplog string
	_ = s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(hlc_ts),'') FROM oplog").Scan(&lastOplog)

	var totalOplog int64
	_ = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM oplog").Scan(&totalOplog)

	stats := make([]ReplStat, 0)

	// Default/global state (legacy).
	var defaultPull string
	var defaultPush string
	_ = s.db.QueryRowContext(ctx, "SELECT COALESCE(last_pull_hlc,''), COALESCE(last_push_hlc,'') FROM repl_state WHERE id=1").Scan(&defaultPull, &defaultPush)
	if defaultPull != "" || defaultPush != "" {
		stat := buildReplStat("default", defaultPull, defaultPush, lastOplog, totalOplog, s)
		stats = append(stats, stat)
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT remote, COALESCE(last_pull_hlc,''), COALESCE(last_push_hlc,'')
FROM repl_state_remote
ORDER BY remote`)
	if err != nil {
		return nil, err
	}
	err = scanRows(rows, func(scan func(dest ...any) error) error {
		var remote, pull, push string
		if err := scan(&remote, &pull, &push); err != nil {
			return err
		}
		stats = append(stats, buildReplStat(remote, pull, push, lastOplog, totalOplog, s))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func buildReplStat(remote, pull, push, lastOplog string, totalOplog int64, store *Store) ReplStat {
	stat := ReplStat{
		Remote:       remote,
		LastPullHLC:  pull,
		LastPushHLC:  push,
		LastOplogHLC: lastOplog,
	}
	if pull != "" {
		stat.PullLagSeconds = lagSeconds(pull)
	}
	if push != "" {
		stat.PushLagSeconds = lagSeconds(push)
		if store != nil {
			if backlog, err := store.countOplogSince(context.Background(), push); err == nil {
				stat.PushBacklog = backlog
			}
		}
	} else if totalOplog > 0 {
		stat.PushBacklog = totalOplog
	}
	return stat
}

func lagSeconds(hlc string) float64 {
	physical, ok := parseHLCPhysical(hlc)
	if !ok {
		return 0
	}
	now := time.Now().UTC().UnixNano()
	if now <= physical {
		return 0
	}
	return float64(now-physical) / float64(time.Second)
}

func parseHLCPhysical(hlc string) (int64, bool) {
	parts := strings.SplitN(hlc, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return 0, false
	}
	val, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func (s *Store) countOplogSince(ctx context.Context, since string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("meta: db not initialized")
	}
	var count int64
	if since == "" {
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM oplog").Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM oplog WHERE hlc_ts > ?", since).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// ListGCTrends returns recent GC runs (excluding plans) in reverse chronological order.
func (s *Store) ListGCTrends(ctx context.Context, limit int) (out []GCTrend, err error) {
	if limit <= 0 {
		limit = 30
	}
	if limit > 365 {
		limit = 365
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT mode, finished_at, errors, COALESCE(deleted,0), COALESCE(reclaimed_bytes,0), COALESCE(rewritten_bytes,0), COALESCE(new_segments,0)
FROM ops_runs
WHERE mode LIKE 'gc-%' AND mode NOT LIKE 'gc-plan%' AND mode NOT LIKE 'gc-rewrite-plan%'
ORDER BY finished_at DESC
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return out, scanRows(rows, func(scan func(dest ...any) error) error {
		var trend GCTrend
		if err := scan(&trend.Mode, &trend.FinishedAt, &trend.Errors, &trend.Deleted, &trend.ReclaimedBytes, &trend.RewrittenBytes, &trend.NewSegments); err != nil {
			return err
		}
		denom := trend.ReclaimedBytes + trend.RewrittenBytes
		if denom > 0 {
			trend.ReclaimRate = float64(trend.ReclaimedBytes) / float64(denom)
		}
		out = append(out, trend)
		return nil
	})
}

// ListLiveManifestPaths returns manifest paths for current versions.
func (s *Store) ListLiveManifestPaths(ctx context.Context) (out []string, err error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT m.path
FROM objects_current o
JOIN manifests m ON m.version_id = o.version_id`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListBuckets returns bucket names in lexical order.
func (s *Store) ListBuckets(ctx context.Context) (out []string, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT bucket FROM buckets ORDER BY bucket`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, err
		}
		out = append(out, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// BucketExists checks whether a bucket exists.
func (s *Store) BucketExists(ctx context.Context, bucket string) (bool, error) {
	if bucket == "" {
		return false, errors.New("meta: bucket required")
	}
	var name string
	err := s.db.QueryRowContext(ctx, "SELECT bucket FROM buckets WHERE bucket=? LIMIT 1", bucket).Scan(&name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// BucketHasObjects checks whether a bucket has any current objects.
func (s *Store) BucketHasObjects(ctx context.Context, bucket string) (bool, error) {
	if bucket == "" {
		return false, errors.New("meta: bucket required")
	}
	var any int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM objects_current WHERE bucket=? LIMIT 1", bucket).Scan(&any)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteBucket removes a bucket entry.
func (s *Store) DeleteBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return errors.New("meta: bucket required")
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM buckets WHERE bucket=?", bucket)
	return err
}

// DeleteObject removes the current object pointer and marks the version as deleted.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) (bool, error) {
	if bucket == "" || key == "" {
		return false, errors.New("meta: bucket and key required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var versionID string
	err = tx.QueryRowContext(ctx, "SELECT version_id FROM objects_current WHERE bucket=? AND key=?", bucket, key).Scan(&versionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return false, nil
		}
		return false, err
	}
	hlcTS, siteID := s.nextHLC()
	deletePayload, err := json.Marshal(oplogDeletePayload{LastModified: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM objects_current WHERE bucket=? AND key=?", bucket, key); err != nil {
		return false, err
	}
	_, _ = tx.ExecContext(ctx, "UPDATE versions SET state='DELETED', hlc_ts=?, site_id=? WHERE version_id=?", hlcTS, siteID, versionID)
	if err := s.recordOplogTx(tx, hlcTS, "delete", bucket, key, versionID, string(deletePayload)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// DeleteObjectVersion marks a specific version as deleted and updates objects_current if needed.
func (s *Store) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) (bool, error) {
	if bucket == "" || key == "" || versionID == "" {
		return false, errors.New("meta: bucket, key, and version id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var state string
	err = tx.QueryRowContext(ctx, `
SELECT state
FROM versions
WHERE bucket=? AND key=? AND version_id=?`, bucket, key, versionID).Scan(&state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return false, nil
		}
		return false, err
	}
	hlcTS, siteID := s.nextHLC()
	deletePayload, err := json.Marshal(oplogDeletePayload{LastModified: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE versions SET state='DELETED', hlc_ts=?, site_id=? WHERE version_id=?", hlcTS, siteID, versionID); err != nil {
		return false, err
	}
	var currentVersion string
	err = tx.QueryRowContext(ctx, "SELECT version_id FROM objects_current WHERE bucket=? AND key=?", bucket, key).Scan(&currentVersion)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if currentVersion == versionID {
		var nextVersion string
		err = tx.QueryRowContext(ctx, `
SELECT version_id
FROM versions
WHERE bucket=? AND key=? AND state='ACTIVE' AND version_id<>?
ORDER BY last_modified_utc DESC
LIMIT 1`, bucket, key, versionID).Scan(&nextVersion)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := tx.ExecContext(ctx, "DELETE FROM objects_current WHERE bucket=? AND key=?", bucket, key); err != nil {
					return false, err
				}
			} else {
				return false, err
			}
		} else {
			if _, err := tx.ExecContext(ctx, "UPDATE objects_current SET version_id=? WHERE bucket=? AND key=?", nextVersion, bucket, key); err != nil {
				return false, err
			}
		}
	}
	if err := s.recordOplogTx(tx, hlcTS, "delete", bucket, key, versionID, string(deletePayload)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// DeleteSegment removes a segment row.
func (s *Store) DeleteSegment(ctx context.Context, segmentID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM segments WHERE segment_id=?", segmentID)
	return err
}
