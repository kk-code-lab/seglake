package ops

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/lifecycle"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	_ "modernc.org/sqlite"
)

func TestLifecyclePlanCurrentExpirationForNullObject(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningDisabled)
	_, result, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>expire-logs</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)

	plan, report, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC().AddDate(0, 0, 2)})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if report.Candidates != 1 || report.CurrentExpirations != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if len(plan.Candidates) != 1 {
		t.Fatalf("candidate count=%d", len(plan.Candidates))
	}
	cand := plan.Candidates[0]
	if cand.Action != LifecycleActionExpireCurrent || cand.Key != "logs/a.txt" || cand.VersionID != result.VersionID {
		t.Fatalf("unexpected candidate: %+v", cand)
	}
}

func TestLifecyclePlanNoncurrentExpirationOnly(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_, first, err := eng.PutObject(context.Background(), "bucket", "k.txt", "", bytes.NewReader([]byte("one")))
	if err != nil {
		t.Fatalf("PutObject first: %v", err)
	}
	_, current, err := eng.PutObject(context.Background(), "bucket", "k.txt", "", bytes.NewReader([]byte("two")))
	if err != nil {
		t.Fatalf("PutObject current: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>old</ID><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`)

	plan, report, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC().AddDate(0, 0, 2)})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if report.Candidates != 1 || report.NoncurrentExpirations != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	cand := plan.Candidates[0]
	if cand.Action != LifecycleActionExpireNoncurrent || cand.VersionID != first.VersionID || cand.CurrentVersionID != current.VersionID {
		t.Fatalf("unexpected candidate: %+v", cand)
	}
}

func TestLifecyclePlanTagAndPrefixFilters(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_, tagged, err := eng.PutObject(context.Background(), "bucket", "logs/tagged.txt", "", bytes.NewReader([]byte("tagged")))
	if err != nil {
		t.Fatalf("PutObject tagged: %v", err)
	}
	_, _, err = eng.PutObject(context.Background(), "bucket", "logs/untagged.txt", "", bytes.NewReader([]byte("untagged")))
	if err != nil {
		t.Fatalf("PutObject untagged: %v", err)
	}
	if err := store.SetObjectTags(context.Background(), "bucket", "logs/tagged.txt", tagged.VersionID, []meta.ObjectTag{{Key: "env", Value: "dev"}}); err != nil {
		t.Fatalf("SetObjectTags: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>tagged</ID><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix><Tag><Key>env</Key><Value>dev</Value></Tag></And></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)

	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC().AddDate(0, 0, 2)})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].VersionID != tagged.VersionID {
		t.Fatalf("unexpected candidates: %+v", plan.Candidates)
	}
}

func TestLifecyclePlanAbortMPUAndLimit(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_ = eng
	for _, uploadID := range []string{"u1", "u2", "u3"} {
		if err := store.CreateMultipartUpload(context.Background(), "bucket", "tmp/"+uploadID, uploadID, ""); err != nil {
			t.Fatalf("CreateMultipartUpload %s: %v", uploadID, err)
		}
		if err := store.PutMultipartPart(context.Background(), uploadID, 1, "part-"+uploadID, "etag", 100); err != nil {
			t.Fatalf("PutMultipartPart %s: %v", uploadID, err)
		}
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>abort</ID><Status>Enabled</Status><Filter><Prefix>tmp/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)

	plan, report, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC().AddDate(0, 0, 2), Limit: 2})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if len(plan.Candidates) != 2 || report.MPUAborts != 2 || report.Warnings != 1 {
		t.Fatalf("unexpected plan/report: candidates=%+v report=%+v", plan.Candidates, report)
	}
	if plan.Candidates[0].UploadID != "u1" || plan.Candidates[1].UploadID != "u2" {
		t.Fatalf("expected deterministic upload order, got %+v", plan.Candidates)
	}
}

func TestLifecyclePlanDisabledRuleAndRoundTrip(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_, _, err := eng.PutObject(context.Background(), "bucket", "k.txt", "", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>disabled</ID><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)

	plan, report, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC().AddDate(0, 0, 2)})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if report.Candidates != 0 || len(plan.Candidates) != 0 {
		t.Fatalf("disabled rule produced candidates: %+v %+v", report, plan.Candidates)
	}
	if report.SkippedRules != 1 || plan.Counts.SkippedRules != 1 {
		t.Fatalf("expected disabled rule to be counted as skipped: %+v %+v", report, plan.Counts)
	}
	path := filepath.Join(t.TempDir(), "lifecycle.json")
	if err := WriteLifecyclePlan(path, plan); err != nil {
		t.Fatalf("WriteLifecyclePlan: %v", err)
	}
	read, err := ReadLifecyclePlan(path)
	if err != nil {
		t.Fatalf("ReadLifecyclePlan: %v", err)
	}
	if read.SchemaVersion != lifecyclePlanSchemaVersion || read.ConfigFingerprints["bucket"] == "" {
		t.Fatalf("unexpected round trip: %+v", read)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("plan is not valid json")
	}
}

func TestLifecycleRunVersionedCurrentCreatesDeleteMarker(t *testing.T) {
	assertLifecycleRunCurrentCreatesDeleteMarker(t, meta.BucketVersioningEnabled)
}

func TestLifecycleRunSuspendedCurrentCreatesDeleteMarker(t *testing.T) {
	assertLifecycleRunCurrentCreatesDeleteMarker(t, meta.BucketVersioningSuspended)
}

func assertLifecycleRunCurrentCreatesDeleteMarker(t *testing.T, versioning string) {
	t.Helper()
	metaPath, store, eng := newLifecyclePlanFixture(t, versioning)
	_, result, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setVersionModified(t, metaPath, result.VersionID, time.Now().UTC().AddDate(0, 0, -2))
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}

	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Deleted != 1 || report.CurrentExpirations != 1 || report.Skipped != 0 || report.Errors != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	current, err := store.GetObjectMeta(context.Background(), "bucket", "logs/a.txt")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if current.State != meta.VersionStateDeleteMarker || current.VersionID == result.VersionID {
		t.Fatalf("expected delete marker current, got %+v", current)
	}
	old, err := store.GetObjectVersion(context.Background(), "bucket", "logs/a.txt", result.VersionID)
	if err != nil {
		t.Fatalf("GetObjectVersion old: %v", err)
	}
	if old.State != meta.VersionStateActive {
		t.Fatalf("expected old version preserved, got %+v", old)
	}
}

func TestLifecycleRunUnversionedCurrentDeletesNullObject(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningDisabled)
	_, result, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setVersionModified(t, metaPath, result.VersionID, time.Now().UTC().AddDate(0, 0, -2))
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}

	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Deleted != 1 || report.CurrentExpirations != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if _, err := store.GetObjectMeta(context.Background(), "bucket", "logs/a.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no current object, err=%v", err)
	}
	old, err := store.GetObjectVersion(context.Background(), "bucket", "logs/a.txt", result.VersionID)
	if err != nil {
		t.Fatalf("GetObjectVersion old: %v", err)
	}
	if old.State != meta.VersionStateDeleted {
		t.Fatalf("expected old version deleted, got %+v", old)
	}
}

func TestLifecycleRunNoncurrentBeforeCurrent(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_, first, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("one")))
	if err != nil {
		t.Fatalf("PutObject first: %v", err)
	}
	_, current, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("two")))
	if err != nil {
		t.Fatalf("PutObject current: %v", err)
	}
	old := time.Now().UTC().AddDate(0, 0, -2)
	setVersionModified(t, metaPath, first.VersionID, old)
	setVersionModified(t, metaPath, current.VersionID, old)
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>expire-current</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule><Rule><ID>expire-old</ID><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}

	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Deleted != 2 || report.CurrentExpirations != 1 || report.NoncurrentExpirations != 1 || report.Skipped != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	firstMeta, err := store.GetObjectVersion(context.Background(), "bucket", "logs/a.txt", first.VersionID)
	if err != nil {
		t.Fatalf("GetObjectVersion first: %v", err)
	}
	if firstMeta.State != meta.VersionStateDeleted {
		t.Fatalf("expected noncurrent deleted, got %+v", firstMeta)
	}
	currentMeta, err := store.GetObjectMeta(context.Background(), "bucket", "logs/a.txt")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if currentMeta.State != meta.VersionStateDeleteMarker {
		t.Fatalf("expected current delete marker, got %+v", currentMeta)
	}
}

func TestLifecycleRunSkipsStaleTagAndFingerprint(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	_, result, err := eng.PutObject(context.Background(), "bucket", "logs/a.txt", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setVersionModified(t, metaPath, result.VersionID, time.Now().UTC().AddDate(0, 0, -2))
	if err := store.SetObjectTags(context.Background(), "bucket", "logs/a.txt", result.VersionID, []meta.ObjectTag{{Key: "env", Value: "dev"}}); err != nil {
		t.Fatalf("SetObjectTags: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>tagged</ID><Status>Enabled</Status><Filter><Tag><Key>env</Key><Value>dev</Value></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if err := store.SetObjectTags(context.Background(), "bucket", "logs/a.txt", result.VersionID, []meta.ObjectTag{{Key: "env", Value: "prod"}}); err != nil {
		t.Fatalf("SetObjectTags stale: %v", err)
	}
	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun tag stale: %v", err)
	}
	if report.Deleted != 0 || report.Skipped != 1 {
		t.Fatalf("expected tag-stale skip, got %+v", report)
	}

	if err := store.SetObjectTags(context.Background(), "bucket", "logs/a.txt", result.VersionID, []meta.ObjectTag{{Key: "env", Value: "dev"}}); err != nil {
		t.Fatalf("SetObjectTags restore: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>different</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	report, err = LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun fp stale: %v", err)
	}
	if report.Deleted != 0 || report.Skipped != 1 {
		t.Fatalf("expected fingerprint-stale skip, got %+v", report)
	}
}

func TestLifecycleRunAbortMPU(t *testing.T) {
	metaPath, store, _ := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	if err := store.CreateMultipartUpload(context.Background(), "bucket", "tmp/u1", "u1", ""); err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := store.PutMultipartPart(context.Background(), "u1", 1, "part-u1", "etag", 100); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	setUploadCreated(t, metaPath, "u1", time.Now().UTC().AddDate(0, 0, -2))
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>abort</ID><Status>Enabled</Status><Filter><Prefix>tmp/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}

	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Deleted != 1 || report.MPUAborts != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if _, err := store.GetMultipartUpload(context.Background(), "u1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected upload removed, err=%v", err)
	}
	parts, err := store.ListMultipartParts(context.Background(), "u1")
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected parts removed, got %+v", parts)
	}
}

func TestLifecycleRunSkipsMissingAndCompletedMPU(t *testing.T) {
	metaPath, store, _ := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	for _, uploadID := range []string{"u-missing", "u-completed"} {
		if err := store.CreateMultipartUpload(context.Background(), "bucket", "tmp/"+uploadID, uploadID, ""); err != nil {
			t.Fatalf("CreateMultipartUpload %s: %v", uploadID, err)
		}
		if err := store.PutMultipartPart(context.Background(), uploadID, 1, "part-"+uploadID, "etag", 100); err != nil {
			t.Fatalf("PutMultipartPart %s: %v", uploadID, err)
		}
		setUploadCreated(t, metaPath, uploadID, time.Now().UTC().AddDate(0, 0, -2))
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>abort</ID><Status>Enabled</Status><Filter><Prefix>tmp/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)
	plan, _, err := LifecyclePlanBuild(metaPath, LifecyclePlanOptions{AsOf: time.Now().UTC()})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if len(plan.Candidates) != 2 {
		t.Fatalf("expected two MPU candidates, got %+v", plan.Candidates)
	}
	if err := store.AbortMultipartUpload(context.Background(), "u-missing"); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	if err := store.CompleteMultipartUpload(context.Background(), "u-completed"); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	report, err := LifecycleRun(metaPath, plan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Deleted != 0 || report.MPUAborts != 0 || report.Skipped != 2 || report.Errors != 0 {
		t.Fatalf("expected missing/completed MPU candidates skipped, got %+v", report)
	}
}

func TestLifecycleRunValidationAndPerCandidateErrors(t *testing.T) {
	metaPath, store, eng := newLifecyclePlanFixture(t, meta.BucketVersioningEnabled)
	if _, _, err := eng.PutObject(context.Background(), "bucket", "k.txt", "", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	setLifecycleConfig(t, store, "bucket", `<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	empty := &LifecyclePlan{SchemaVersion: lifecyclePlanSchemaVersion, ConfigFingerprints: map[string]string{"bucket": "fp"}}
	if _, err := LifecycleRun(metaPath, empty, false); err == nil {
		t.Fatalf("expected force validation error")
	}
	bad := &LifecyclePlan{SchemaVersion: 999}
	if _, err := LifecycleRun(metaPath, bad, true); err == nil {
		t.Fatalf("expected schema validation error")
	}
	cfg, err := store.GetBucketLifecycle(context.Background(), "bucket")
	if err != nil {
		t.Fatalf("GetBucketLifecycle: %v", err)
	}
	report, err := LifecycleRun(metaPath, &LifecyclePlan{
		SchemaVersion:      lifecyclePlanSchemaVersion,
		ConfigFingerprints: map[string]string{"bucket": cfg.ConfigFingerprint},
		Candidates: []LifecyclePlanCandidate{
			{Action: "bogus", Bucket: "bucket", Key: "k.txt", VersionID: "v1", ConfigFingerprint: cfg.ConfigFingerprint},
			{Action: LifecycleActionExpireCurrent, Bucket: "bucket", Key: "missing.txt", VersionID: "missing", CurrentVersionID: "missing", ConfigFingerprint: cfg.ConfigFingerprint},
		},
	}, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if report.Errors != 1 || report.Skipped != 1 {
		t.Fatalf("expected one error and one skip, got %+v", report)
	}
}

func newLifecyclePlanFixture(t *testing.T, versioning string) (string, *meta.Store, *engine.Engine) {
	t.Helper()
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.CreateBucketWithVersioning(context.Background(), "bucket", versioning); err != nil {
		t.Fatalf("CreateBucketWithVersioning: %v", err)
	}
	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}
	return metaPath, store, eng
}

func setLifecycleConfig(t *testing.T, store *meta.Store, bucket, body string) {
	t.Helper()
	parsed, err := lifecycle.ParseXML(bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("ParseXML: %v", err)
	}
	if err := store.SetBucketLifecycle(context.Background(), meta.BucketLifecycleConfig{
		Bucket:            bucket,
		XML:               parsed.XMLText,
		NormalizedJSON:    parsed.NormalizedJSON,
		ConfigFingerprint: parsed.Fingerprint,
		RuleIDs:           parsed.RuleIDsJSON,
	}); err != nil {
		t.Fatalf("SetBucketLifecycle: %v", err)
	}
}

func setVersionModified(t *testing.T, metaPath, versionID string, ts time.Time) {
	t.Helper()
	execLifecycleTestSQL(t, metaPath, "UPDATE versions SET last_modified_utc=? WHERE version_id=?", ts.UTC().Format(time.RFC3339Nano), versionID)
}

func setUploadCreated(t *testing.T, metaPath, uploadID string, ts time.Time) {
	t.Helper()
	execLifecycleTestSQL(t, metaPath, "UPDATE multipart_uploads SET created_at=? WHERE upload_id=?", ts.UTC().Format(time.RFC3339Nano), uploadID)
}

func execLifecycleTestSQL(t *testing.T, metaPath, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", metaPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
