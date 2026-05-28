package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/lifecycle"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
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
