package s3

import "testing"

func TestParsePolicyShortHands(t *testing.T) {
	pol, err := ParsePolicy("rw")
	if err != nil {
		t.Fatalf("ParsePolicy rw: %v", err)
	}
	if !pol.Allows("write", "b", "k") {
		t.Fatalf("expected rw to allow write")
	}
	pol, err = ParsePolicy("ro")
	if err != nil {
		t.Fatalf("ParsePolicy ro: %v", err)
	}
	if pol.Allows("write", "b", "k") {
		t.Fatalf("expected ro to deny write")
	}
}

func TestParsePolicyJSON(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"demo","prefix":"p/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy json: %v", err)
	}
	if !pol.Allows("read", "demo", "p/x") {
		t.Fatalf("expected allow for prefix")
	}
	if pol.Allows("read", "demo", "q/x") {
		t.Fatalf("expected deny for non-prefix")
	}
}

func TestPolicyDenyOverrides(t *testing.T) {
	raw := `{"version":"v1","statements":[{"effect":"allow","actions":["read"],"resources":[{"bucket":"*"}]},{"effect":"deny","actions":["read"],"resources":[{"bucket":"demo","prefix":"secret/"}]}]}`
	pol, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("ParsePolicy deny: %v", err)
	}
	if !pol.Allows("read", "demo", "public/x") {
		t.Fatalf("expected allow for public")
	}
	if pol.Allows("read", "demo", "secret/x") {
		t.Fatalf("expected deny for secret")
	}
}

func TestPolicyActionForRequest(t *testing.T) {
	op := policyActionForRequest("mpu_upload_part")
	if op != policyActionMPU {
		t.Fatalf("expected mpu action, got %q", op)
	}
}
