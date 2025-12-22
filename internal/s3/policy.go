package s3

import (
	"encoding/json"
	"errors"
	"strings"
)

type Policy struct {
	Version    string      `json:"version"`
	Statements []Statement `json:"statements"`
}

type Statement struct {
	Effect    string     `json:"effect"`
	Actions   []string   `json:"actions"`
	Resources []Resource `json:"resources"`
}

type Resource struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix,omitempty"`
}

const (
	policyEffectAllow = "allow"
	policyEffectDeny  = "deny"
)

const (
	policyActionAll   = "*"
	policyActionRead  = "read"
	policyActionWrite = "write"
	policyActionList  = "list"
	policyActionMPU   = "mpu"
	policyActionCopy  = "copy"
	policyActionMeta  = "meta"
)

var validPolicyActions = map[string]struct{}{
	policyActionAll:   {},
	policyActionRead:  {},
	policyActionWrite: {},
	policyActionList:  {},
	policyActionMPU:   {},
	policyActionCopy:  {},
	policyActionMeta:  {},
}

// ParsePolicy parses a policy JSON string or accepts short-hands "rw" and "ro".
func ParsePolicy(raw string) (*Policy, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "rw") {
		return &Policy{Version: "v1", Statements: []Statement{{
			Effect:  policyEffectAllow,
			Actions: []string{policyActionAll},
			Resources: []Resource{{
				Bucket: "*",
			}},
		}}}, nil
	}
	if strings.EqualFold(trimmed, "ro") || strings.EqualFold(trimmed, "read-only") {
		return &Policy{Version: "v1", Statements: []Statement{{
			Effect:  policyEffectAllow,
			Actions: []string{policyActionRead, policyActionList, policyActionMeta},
			Resources: []Resource{{
				Bucket: "*",
			}},
		}}}, nil
	}
	var pol Policy
	if err := json.Unmarshal([]byte(trimmed), &pol); err != nil {
		return nil, err
	}
	if err := pol.validate(); err != nil {
		return nil, err
	}
	return &pol, nil
}

func (p *Policy) validate() error {
	if p == nil {
		return errors.New("policy required")
	}
	if len(p.Statements) == 0 {
		return errors.New("policy requires statements")
	}
	for i := range p.Statements {
		stmt := &p.Statements[i]
		stmt.Effect = strings.ToLower(strings.TrimSpace(stmt.Effect))
		if stmt.Effect != policyEffectAllow && stmt.Effect != policyEffectDeny {
			return errors.New("policy statement effect must be allow or deny")
		}
		if len(stmt.Actions) == 0 {
			return errors.New("policy statement requires actions")
		}
		for j := range stmt.Actions {
			a := strings.ToLower(strings.TrimSpace(stmt.Actions[j]))
			if _, ok := validPolicyActions[a]; !ok {
				return errors.New("policy statement has invalid action")
			}
			stmt.Actions[j] = a
		}
		if len(stmt.Resources) == 0 {
			return errors.New("policy statement requires resources")
		}
		for j := range stmt.Resources {
			res := &stmt.Resources[j]
			res.Bucket = strings.TrimSpace(res.Bucket)
			res.Prefix = strings.TrimSpace(res.Prefix)
			if res.Bucket == "" {
				return errors.New("policy resource bucket required")
			}
		}
	}
	return nil
}

// Allows returns whether the policy permits the action for a bucket/key pair.
func (p *Policy) Allows(action, bucket, key string) bool {
	if p == nil {
		return false
	}
	action = strings.ToLower(strings.TrimSpace(action))
	bucket = strings.TrimSpace(bucket)
	key = strings.TrimSpace(key)
	allowed := false
	for _, stmt := range p.Statements {
		if !stmt.matches(action, bucket, key) {
			continue
		}
		if stmt.Effect == policyEffectDeny {
			return false
		}
		allowed = true
	}
	return allowed
}

func (s *Statement) matches(action, bucket, key string) bool {
	if s == nil {
		return false
	}
	if !actionsMatch(s.Actions, action) {
		return false
	}
	for _, res := range s.Resources {
		if res.matches(bucket, key) {
			return true
		}
	}
	return false
}

func actionsMatch(actions []string, action string) bool {
	for _, a := range actions {
		if a == policyActionAll || a == action {
			return true
		}
	}
	return false
}

func (r Resource) matches(bucket, key string) bool {
	if r.Bucket != "*" && r.Bucket != bucket {
		return false
	}
	if r.Prefix == "" {
		return true
	}
	return strings.HasPrefix(key, r.Prefix)
}

func policyActionForRequest(op string) string {
	switch op {
	case "meta_stats":
		return policyActionMeta
	case "list_buckets", "list_v1", "list_v2", "mpu_list_uploads", "mpu_list_parts":
		return policyActionList
	case "get", "head":
		return policyActionRead
	case "put", "delete", "delete_bucket":
		return policyActionWrite
	case "mpu_initiate", "mpu_upload_part", "mpu_complete", "mpu_abort":
		return policyActionMPU
	case "copy":
		return policyActionCopy
	default:
		return ""
	}
}
