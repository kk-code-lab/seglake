package meta

import (
	"context"
	"database/sql"
	"errors"
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
	ID        int64
	SiteID    string
	HLCTS     string
	OpType    string
	Bucket    string
	Key       string
	VersionID string
	Payload   string
	CreatedAt string
}

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


// UpsertAPIKey inserts or updates an API key entry.
func (s *Store) UpsertAPIKey(ctx context.Context, accessKey, secretKey, policy string, enabled bool, inflightLimit int64) error {
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
	_, err := s.db.ExecContext(ctx, `
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
	return err
}

// UpdateAPIKeyPolicy updates policy for an API key.
func (s *Store) UpdateAPIKeyPolicy(ctx context.Context, accessKey, policy string) error {
	if accessKey == "" {
		return errors.New("meta: access key required")
	}
	if policy == "" {
		policy = "rw"
	}
	_, err := s.db.ExecContext(ctx, "UPDATE api_keys SET policy=? WHERE access_key=?", policy, accessKey)
	return err
}

// AllowBucketForKey adds a bucket allow entry for the given access key.
func (s *Store) AllowBucketForKey(ctx context.Context, accessKey, bucket string) error {
	if accessKey == "" || bucket == "" {
		return errors.New("meta: access key and bucket required")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO api_key_bucket_allow(access_key, bucket)
VALUES(?, ?)`, accessKey, bucket)
	return err
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
	if accessKey == "" || bucket == "" {
		return errors.New("meta: access key and bucket required")
	}
	_, err := s.db.ExecContext(ctx, `
DELETE FROM api_key_bucket_allow
WHERE access_key=? AND bucket=?`, accessKey, bucket)
	return err
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
func (s *Store) SetAPIKeyEnabled(ctx context.Context, accessKey string, enabled bool) error {
	if accessKey == "" {
		return errors.New("meta: access key required")
	}
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}
	_, err := s.db.ExecContext(ctx, "UPDATE api_keys SET enabled=? WHERE access_key=?", enabledInt, accessKey)
	return err
}

// DeleteAPIKey removes an API key and its bucket allowlist.
func (s *Store) DeleteAPIKey(ctx context.Context, accessKey string) error {
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
func (s *Store) SetBucketPolicy(ctx context.Context, bucket, policy string) error {
	if bucket == "" || policy == "" {
		return errors.New("meta: bucket and policy required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO bucket_policies(bucket, policy, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(bucket) DO UPDATE SET
	policy=excluded.policy,
	updated_at=excluded.updated_at`, bucket, policy, now)
	return err
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
func (s *Store) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if bucket == "" {
		return errors.New("meta: bucket required")
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM bucket_policies WHERE bucket=?", bucket)
	return err
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
	if err := s.recordOplogTx(tx, hlcTS, "put", bucket, key, versionID, ""); err != nil {
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
	if err := s.recordOplogTx(tx, hlcTS, "put", bucket, key, versionID, ""); err != nil {
		return err
	}
	return nil
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
	if _, err = tx.ExecContext(ctx, "DELETE FROM objects_current WHERE bucket=? AND key=?", bucket, key); err != nil {
		return false, err
	}
	_, _ = tx.ExecContext(ctx, "UPDATE versions SET state='DELETED', hlc_ts=?, site_id=? WHERE version_id=?", hlcTS, siteID, versionID)
	if err := s.recordOplogTx(tx, hlcTS, "delete", bucket, key, versionID, ""); err != nil {
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
	if err := s.recordOplogTx(tx, hlcTS, "delete", bucket, key, versionID, ""); err != nil {
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
