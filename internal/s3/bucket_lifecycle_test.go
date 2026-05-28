package s3

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/lifecycle"
)

const bucketLifecycleBody = `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>expire-logs</ID><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix><Tag><Key>env</Key><Value>dev</Value></Tag></And></Filter><Expiration><Days>30</Days></Expiration></Rule><Rule><ID>abort-mpu</ID><Status>Disabled</Status><Filter><Prefix>tmp/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`

func TestBucketLifecycleParseAndNormalize(t *testing.T) {
	t.Parallel()
	parsed, err := lifecycle.ParseXML(strings.NewReader(bucketLifecycleBody))
	if err != nil {
		t.Fatalf("ParseXML: %v", err)
	}
	if parsed.XMLText != bucketLifecycleBody {
		t.Fatalf("expected original XML preserved")
	}
	if parsed.Fingerprint == "" || parsed.NormalizedJSON == "" {
		t.Fatalf("expected normalized config and fingerprint")
	}
	if parsed.RuleIDsJSON != `["abort-mpu","expire-logs"]` {
		t.Fatalf("unexpected rule ids: %s", parsed.RuleIDsJSON)
	}
	parsed2, err := lifecycle.ParseXML(strings.NewReader(strings.ReplaceAll(bucketLifecycleBody, "><", ">\n<")))
	if err != nil {
		t.Fatalf("ParseXML formatted: %v", err)
	}
	if parsed.NormalizedJSON != parsed2.NormalizedJSON || parsed.Fingerprint != parsed2.Fingerprint {
		t.Fatalf("expected stable normalized fingerprint")
	}
}

func TestBucketLifecycleValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		body        string
		unsupported bool
	}{
		{
			name: "malformed",
			body: "<LifecycleConfiguration>",
		},
		{
			name: "missing-rules",
			body: `<LifecycleConfiguration/>`,
		},
		{
			name: "invalid-status",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Paused</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
		},
		{
			name: "duplicate-id",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule><Rule><ID>x</ID><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule></LifecycleConfiguration>`,
		},
		{
			name: "missing-action",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`,
		},
		{
			name: "invalid-tag",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Filter><Tag><Key></Key><Value>v</Value></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
		},
		{
			name: "invalid-date",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Expiration><Date>tomorrow</Date></Expiration></Rule></LifecycleConfiguration>`,
		},
		{
			name:        "transition-unsupported",
			body:        `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>`,
			unsupported: true,
		},
		{
			name:        "expired-delete-marker-unsupported",
			body:        `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></LifecycleConfiguration>`,
			unsupported: true,
		},
		{
			name: "mpu-tag-filter-invalid",
			body: `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Filter><Tag><Key>env</Key><Value>dev</Value></Tag></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`,
		},
		{
			name:        "size-filter-unsupported",
			body:        `<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Filter><ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
			unsupported: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := lifecycle.ParseXML(strings.NewReader(tc.body))
			if err == nil {
				t.Fatalf("expected error")
			}
			if tc.unsupported && !errors.Is(err, lifecycle.ErrUnsupportedFeature) {
				t.Fatalf("expected unsupported feature error, got %v", err)
			}
			if !tc.unsupported && errors.Is(err, lifecycle.ErrUnsupportedFeature) {
				t.Fatalf("expected invalid argument style error, got %v", err)
			}
		})
	}
}

func TestPutGetDeleteBucketLifecycle(t *testing.T) {
	h := newTestHandler(t)
	createBucket(t, h, "demo")

	getMissing := httptest.NewRequest(http.MethodGet, "/demo?lifecycle", nil)
	getMissingW := httptest.NewRecorder()
	h.ServeHTTP(getMissingW, getMissing)
	if getMissingW.Code != http.StatusNotFound || !strings.Contains(getMissingW.Body.String(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("expected missing lifecycle, status=%d body=%s", getMissingW.Code, getMissingW.Body.String())
	}

	put := httptest.NewRequest(http.MethodPut, "/demo?lifecycle", strings.NewReader(bucketLifecycleBody))
	put.Header.Set("Content-Type", "application/xml")
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT lifecycle status=%d body=%s", putW.Code, putW.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/demo?lifecycle", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET lifecycle status=%d body=%s", getW.Code, getW.Body.String())
	}
	var cfg lifecycle.Configuration
	if err := xml.Unmarshal(getW.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("decode lifecycle: %v", err)
	}
	if len(cfg.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %+v body=%s", cfg.Rules, getW.Body.String())
	}
	stored, err := h.Meta.GetBucketLifecycle(get.Context(), "demo")
	if err != nil {
		t.Fatalf("GetBucketLifecycle: %v", err)
	}
	if stored.ConfigFingerprint == "" || stored.RuleIDs != `["abort-mpu","expire-logs"]` {
		t.Fatalf("unexpected stored lifecycle metadata: %+v", stored)
	}

	del := httptest.NewRequest(http.MethodDelete, "/demo?lifecycle", nil)
	delW := httptest.NewRecorder()
	h.ServeHTTP(delW, del)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("DELETE lifecycle status=%d body=%s", delW.Code, delW.Body.String())
	}
	if _, err := h.Meta.GetBucketLifecycle(get.Context(), "demo"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected lifecycle deleted, got %v", err)
	}
}

func TestBucketLifecycleHostedStyleAndUnsupported(t *testing.T) {
	h := newTestHandler(t)
	h.VirtualHosted = true
	if err := h.Meta.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	put := httptest.NewRequest(http.MethodPut, "http://localhost/?lifecycle", strings.NewReader(bucketLifecycleBody))
	put.Host = "demo.localhost"
	putW := httptest.NewRecorder()
	h.ServeHTTP(putW, put)
	if putW.Code != http.StatusOK {
		t.Fatalf("hosted PUT lifecycle status=%d body=%s", putW.Code, putW.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "http://localhost/?lifecycle", nil)
	get.Host = "demo.localhost"
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("hosted GET lifecycle status=%d body=%s", getW.Code, getW.Body.String())
	}

	unsupported := httptest.NewRequest(http.MethodPut, "/demo?lifecycle", strings.NewReader(`<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></LifecycleConfiguration>`))
	unsupported.Host = "localhost"
	unsupportedW := httptest.NewRecorder()
	h.ServeHTTP(unsupportedW, unsupported)
	if unsupportedW.Code != http.StatusNotImplemented || !strings.Contains(unsupportedW.Body.String(), "NotImplemented") {
		t.Fatalf("expected NotImplemented, status=%d body=%s", unsupportedW.Code, unsupportedW.Body.String())
	}
}

func TestBucketLifecyclePolicyActions(t *testing.T) {
	t.Parallel()
	pol, err := ParsePolicy(`{"version":"v1","statements":[{"effect":"allow","actions":["GetBucketLifecycle","PutBucketLifecycle","DeleteBucketLifecycle"],"resources":[{"bucket":"bucket"}]}]}`)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	for _, action := range []string{policyActionGetBucketLifecycle, policyActionPutBucketLifecycle, policyActionDeleteBucketLifecycle} {
		if allowed, denied := pol.DecisionWithContext(action, "bucket", "", nil); !allowed || denied {
			t.Fatalf("expected native action %s allowed denied=%v", action, denied)
		}
	}
	awsPol, err := ParsePolicy(`{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":["s3:GetLifecycleConfiguration","s3:PutLifecycleConfiguration","s3:DeleteLifecycleConfiguration"],"Resource":"arn:aws:s3:::bucket"}}`)
	if err != nil {
		t.Fatalf("ParsePolicy AWS: %v", err)
	}
	if allowed, denied := awsPol.DecisionWithContext(policyActionPutBucketLifecycle, "bucket", "", nil); !allowed || denied {
		t.Fatalf("expected AWS lifecycle action allowed denied=%v", denied)
	}
}
