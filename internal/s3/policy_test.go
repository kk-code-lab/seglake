package s3

import (
	"testing"
	"time"
)

func TestParsePolicyShortHands(t *testing.T) {
	pol, err := ParsePolicy("rw")
	if err != nil {
		t.Fatalf("ParsePolicy rw: %v", err)
	}
	if !pol.Allows("PutObject", "b", "k") {
		t.Fatalf("expected rw to allow write")
	}
	pol, err = ParsePolicy("ro")
	if err != nil {
		t.Fatalf("ParsePolicy ro: %v", err)
	}
	if pol.Allows("PutObject", "b", "k") {
		t.Fatalf("expected ro to deny write")
	}
}

func TestParsePolicyJSON(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"p/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy json: %v", err)
	}
	if !pol.Allows("GetObject", "demo", "p/x") {
		t.Fatalf("expected allow for prefix")
	}
	if pol.Allows("GetObject", "demo", "q/x") {
		t.Fatalf("expected deny for non-prefix")
	}
}

func TestPolicyDenyOverrides(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"*"}]},{"effect":"deny","actions":["GetObject"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy deny: %v", err)
	}
	if !pol.Allows("GetObject", "demo", "public/x") {
		t.Fatalf("expected allow for public")
	}
	if pol.Allows("GetObject", "demo", "secret/x") {
		t.Fatalf("expected deny for secret")
	}
}

func TestPolicyActionForRequest(t *testing.T) {
	op := policyActionForRequest("mpu_upload_part")
	if op != policyActionUploadPart {
		t.Fatalf("expected upload part action, got %q", op)
	}
}

func TestPolicyConditionsSourceIP(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"source_ip":["10.0.0.0/8"]}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), SourceIP: "10.1.1.1"}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow for source ip")
	}
	ctx.SourceIP = "192.168.1.1"
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny for non-matching source ip")
	}
}

func TestPolicyConditionsBeforeAfter(t *testing.T) {
	now := time.Now().UTC()
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"after":"` + now.Add(-time.Minute).Format(time.RFC3339) + `","before":"` + now.Add(time.Minute).Format(time.RFC3339) + `"}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: now}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow within time window")
	}
	ctx.Now = now.Add(2 * time.Minute)
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny outside time window")
	}
}

func TestPolicyConditionsHeaders(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["GetObject"],"resources":[{"bucket":"demo"}],"conditions":{"headers":{"x-tenant":"alpha"}}}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	ctx := &PolicyContext{Now: time.Now().UTC(), Headers: map[string]string{"x-tenant": "alpha"}}
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); !allowed {
		t.Fatalf("expected allow for header match")
	}
	ctx.Headers["x-tenant"] = "beta"
	if allowed, _ := pol.DecisionWithContext("GetObject", "demo", "k", ctx); allowed {
		t.Fatalf("expected deny for header mismatch")
	}
}
