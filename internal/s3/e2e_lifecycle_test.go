//go:build e2e

package s3

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/ops"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func TestS3E2ELifecyclePlanRun(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	handler := &Handler{
		Engine: eng,
		Meta:   store,
		Auth: &AuthConfig{
			AccessKey:            "test",
			SecretKey:            "testsecret",
			Region:               "us-east-1",
			AllowUnsignedPayload: true,
			MaxSkew:              5 * time.Minute,
		},
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	do := func(method, target string, body io.Reader, headers map[string]string, wantStatus int) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+target, body)
		if err != nil {
			t.Fatalf("NewRequest %s %s: %v", method, target, err)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		signRequest(req, "test", "testsecret", "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, target, err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s %s response: %v", method, target, err)
		}
		if resp.StatusCode != wantStatus {
			t.Fatalf("%s %s status=%d want=%d body=%s", method, target, resp.StatusCode, wantStatus, data)
		}
		return resp, data
	}

	do(http.MethodPut, "/lifecycle-e2e", nil, nil, http.StatusOK)
	versioningXML := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	do(http.MethodPut, "/lifecycle-e2e?versioning", strings.NewReader(versioningXML), map[string]string{"Content-Type": "application/xml"}, http.StatusOK)
	lifecycleXML := `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Rule><ID>expire-current</ID><Status>Enabled</Status><Filter><Prefix>current/</Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule>` +
		`<Rule><ID>expire-tagged-history</ID><Status>Enabled</Status><Filter><Tag><Key>archive</Key><Value>yes</Value></Tag></Filter><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule>` +
		`<Rule><ID>abort-uploads</ID><Status>Enabled</Status><Filter><Prefix>uploads/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>` +
		`</LifecycleConfiguration>`
	do(http.MethodPut, "/lifecycle-e2e?lifecycle", strings.NewReader(lifecycleXML), map[string]string{"Content-Type": "application/xml"}, http.StatusOK)

	currentResp, _ := do(http.MethodPut, "/lifecycle-e2e/current/expire.txt", strings.NewReader("current"), nil, http.StatusOK)
	currentVersionID := currentResp.Header.Get("x-amz-version-id")
	if currentVersionID == "" {
		t.Fatal("current object version ID is empty")
	}
	oldResp, _ := do(http.MethodPut, "/lifecycle-e2e/history/item.txt", strings.NewReader("old"), map[string]string{"x-amz-tagging": "archive=yes"}, http.StatusOK)
	oldVersionID := oldResp.Header.Get("x-amz-version-id")
	if oldVersionID == "" {
		t.Fatal("old object version ID is empty")
	}
	newResp, _ := do(http.MethodPut, "/lifecycle-e2e/history/item.txt", strings.NewReader("new"), nil, http.StatusOK)
	newVersionID := newResp.Header.Get("x-amz-version-id")
	if newVersionID == "" || newVersionID == oldVersionID {
		t.Fatalf("unexpected current version ID %q", newVersionID)
	}

	_, initBody := do(http.MethodPost, "/lifecycle-e2e/uploads/pending.bin?uploads", nil, nil, http.StatusOK)
	var initResult initiateMultipartResult
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("decode initiate multipart response: %v", err)
	}
	if initResult.UploadID == "" {
		t.Fatal("multipart upload ID is empty")
	}
	partTarget := "/lifecycle-e2e/uploads/pending.bin?partNumber=1&uploadId=" + url.QueryEscape(initResult.UploadID)
	do(http.MethodPut, partTarget, bytes.NewReader([]byte("part-data")), nil, http.StatusOK)
	oldTimestamp := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	if err := store.WithTx(func(tx *sql.Tx) error {
		if err := meta.ExecTx(tx, "UPDATE versions SET last_modified_utc=? WHERE version_id IN (?, ?)", oldTimestamp, currentVersionID, oldVersionID); err != nil {
			return err
		}
		return meta.ExecTx(tx, "UPDATE multipart_uploads SET created_at=? WHERE upload_id=?", oldTimestamp, initResult.UploadID)
	}); err != nil {
		t.Fatalf("age lifecycle fixtures: %v", err)
	}

	plan, planReport, err := ops.LifecyclePlanBuild(metaPath, ops.LifecyclePlanOptions{
		Bucket: "lifecycle-e2e",
		AsOf:   time.Now().UTC(),
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("LifecyclePlanBuild: %v", err)
	}
	if planReport.Errors != 0 || len(plan.Candidates) != 3 {
		t.Fatalf("unexpected lifecycle plan report=%+v candidates=%+v", planReport, plan.Candidates)
	}
	actions := make([]string, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		actions = append(actions, candidate.Action)
	}
	sort.Strings(actions)
	wantActions := []string{ops.LifecycleActionAbortMPU, ops.LifecycleActionExpireCurrent, ops.LifecycleActionExpireNoncurrent}
	sort.Strings(wantActions)
	if strings.Join(actions, ",") != strings.Join(wantActions, ",") {
		t.Fatalf("unexpected lifecycle actions: %v", actions)
	}

	planPath := filepath.Join(dir, "lifecycle-plan.json")
	if err := ops.WriteLifecyclePlan(planPath, plan); err != nil {
		t.Fatalf("WriteLifecyclePlan: %v", err)
	}
	readPlan, err := ops.ReadLifecyclePlan(planPath)
	if err != nil {
		t.Fatalf("ReadLifecyclePlan: %v", err)
	}
	if _, err := store.SetMaintenanceState(context.Background(), "quiesced"); err != nil {
		t.Fatalf("SetMaintenanceState: %v", err)
	}
	runReport, err := ops.LifecycleRun(metaPath, readPlan, true)
	if err != nil {
		t.Fatalf("LifecycleRun: %v", err)
	}
	if runReport.Errors != 0 || runReport.Skipped != 0 || runReport.Deleted != 3 || runReport.CurrentExpirations != 1 || runReport.NoncurrentExpirations != 1 || runReport.MPUAborts != 1 {
		t.Fatalf("unexpected lifecycle run report: %+v", runReport)
	}

	_, versionsBody := do(http.MethodGet, "/lifecycle-e2e?versions", nil, nil, http.StatusOK)
	var versions listObjectVersionsResult
	if err := xml.Unmarshal(versionsBody, &versions); err != nil {
		t.Fatalf("decode object versions: %v", err)
	}
	if !hasLatestDeleteMarker(versions.DeleteMarkers, "current/expire.txt") {
		t.Fatalf("current expiration did not create latest delete marker: %+v", versions.DeleteMarkers)
	}
	if !hasVersion(versions.Versions, "current/expire.txt", "") {
		t.Fatalf("current expiration removed prior version: %+v", versions.Versions)
	}
	if hasVersion(versions.Versions, "history/item.txt", oldVersionID) {
		t.Fatalf("expired noncurrent version is still listed: %+v", versions.Versions)
	}
	if !hasVersion(versions.Versions, "history/item.txt", newVersionID) {
		t.Fatalf("current history version is missing: %+v", versions.Versions)
	}

	_, uploadsBody := do(http.MethodGet, "/lifecycle-e2e?uploads", nil, nil, http.StatusOK)
	if bytes.Contains(uploadsBody, []byte(initResult.UploadID)) || bytes.Contains(uploadsBody, []byte("uploads/pending.bin")) {
		t.Fatalf("aborted multipart upload is still listed: %s", uploadsBody)
	}
	do(http.MethodGet, "/lifecycle-e2e/history/item.txt", nil, nil, http.StatusOK)
	do(http.MethodGet, "/lifecycle-e2e/current/expire.txt", nil, nil, http.StatusNotFound)
}

func hasLatestDeleteMarker(markers []listDeleteMarker, key string) bool {
	for _, marker := range markers {
		if marker.Key == key && marker.IsLatest {
			return true
		}
	}
	return false
}

func hasVersion(versions []listObjectVersion, key, versionID string) bool {
	for _, version := range versions {
		if version.Key == key && (versionID == "" || version.VersionID == versionID) {
			return true
		}
	}
	return false
}
