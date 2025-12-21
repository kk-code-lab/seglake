package meta

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite metadata database.
type Store struct {
	db *sql.DB
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
	store := &Store{db: db}
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

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
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

	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO buckets(bucket, created_at) VALUES(?, ?)", bucket, now); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
INSERT INTO versions(version_id, bucket, key, etag, size, last_modified_utc, hlc_ts, site_id, state)
VALUES(?, ?, ?, ?, ?, ?, '', '', 'ACTIVE')`,
		versionID, bucket, key, etag, size, now); err != nil {
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
	return tx.Commit()
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

// ListMultipartUploads returns active uploads for a bucket and optional prefix.
func (s *Store) ListMultipartUploads(ctx context.Context, bucket, prefix string, limit int) ([]MultipartUpload, error) {
	if limit <= 0 {
		limit = 1000
	}
	pattern := escapeLike(prefix) + "%"
	rows, err := s.db.QueryContext(ctx, `
SELECT upload_id, bucket, key, created_at, state
FROM multipart_uploads
WHERE bucket=? AND key LIKE ? ESCAPE '\' AND state='ACTIVE'
ORDER BY key
LIMIT ?`, bucket, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MultipartUpload
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
	_, err := s.db.ExecContext(ctx, `
UPDATE multipart_uploads SET state='ABORTED' WHERE upload_id=?`, uploadID)
	return err
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
func (s *Store) ListMultipartParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT upload_id, part_number, version_id, etag, size, last_modified_utc
FROM multipart_parts
WHERE upload_id=?
ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MultipartPart
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

// ObjectMeta describes the current object version metadata.
type ObjectMeta struct {
	Key          string
	VersionID    string
	ETag         string
	Size         int64
	LastModified string
}

// GetObjectMeta returns metadata for the current object version.
func (s *Store) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT v.version_id, v.etag, v.size, v.last_modified_utc
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key=?`, bucket, key)
	var meta ObjectMeta
	meta.Key = key
	if err := row.Scan(&meta.VersionID, &meta.ETag, &meta.Size, &meta.LastModified); err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListObjects returns current objects for a bucket with optional prefix and continuation key/version.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, afterKey, afterVersion string, limit int) ([]ObjectMeta, error) {
	if limit <= 0 {
		limit = 1000
	}
	pattern := escapeLike(prefix) + "%"
	var (
		rows *sql.Rows
		err  error
	)
	if afterKey != "" && afterVersion != "" {
		rows, err = s.db.QueryContext(ctx, `
SELECT o.key, v.version_id, v.etag, v.size, v.last_modified_utc
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key LIKE ? ESCAPE '\' AND (o.key > ? OR (o.key = ? AND v.version_id > ?))
ORDER BY o.key, v.version_id
LIMIT ?`, bucket, pattern, afterKey, afterKey, afterVersion, limit)
	} else if afterKey != "" {
		rows, err = s.db.QueryContext(ctx, `
SELECT o.key, v.version_id, v.etag, v.size, v.last_modified_utc
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key LIKE ? ESCAPE '\' AND o.key > ?
ORDER BY o.key
LIMIT ?`, bucket, pattern, afterKey, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
SELECT o.key, v.version_id, v.etag, v.size, v.last_modified_utc
FROM objects_current o
JOIN versions v ON v.version_id = o.version_id
WHERE o.bucket=? AND o.key LIKE ? ESCAPE '\'
ORDER BY o.key
LIMIT ?`, bucket, pattern, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ObjectMeta
	for rows.Next() {
		var meta ObjectMeta
		if err := rows.Scan(&meta.Key, &meta.VersionID, &meta.ETag, &meta.Size, &meta.LastModified); err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
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
