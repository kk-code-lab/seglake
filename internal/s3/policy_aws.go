package s3

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type awsPolicy struct {
	Version   string           `json:"Version"`
	Statement awsStatementList `json:"Statement"`
}

type awsStatementList []awsStatement

func (l *awsStatementList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*l = nil
		return nil
	}
	if data[0] == '[' {
		var out []awsStatement
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		*l = out
		return nil
	}
	var single awsStatement
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*l = []awsStatement{single}
	return nil
}

type awsStatement struct {
	Effect      string                    `json:"Effect"`
	Action      awsStringList             `json:"Action"`
	NotAction   json.RawMessage           `json:"NotAction,omitempty"`
	Resource    awsStringList             `json:"Resource"`
	NotResource json.RawMessage           `json:"NotResource,omitempty"`
	Principal   any                       `json:"Principal,omitempty"`
	Condition   map[string]map[string]any `json:"Condition,omitempty"`
}

type awsStringList []string

func (l *awsStringList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*l = nil
		return nil
	}
	if data[0] == '[' {
		var out []string
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		*l = out
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*l = []string{single}
	return nil
}

var awsActionToPolicy = map[string]string{
	"*":                       policyActionAll,
	"listallmybuckets":        policyActionListBuckets,
	"listbuckets":             policyActionListBuckets,
	"listbucket":              policyActionListBucket,
	"listbucketversions":      policyActionListBucketVersions,
	"listobjectversions":      policyActionListBucketVersions,
	"getbucketlocation":       policyActionGetBucketLocation,
	"getbucketpolicy":         policyActionGetBucketPolicy,
	"putbucketpolicy":         policyActionPutBucketPolicy,
	"deletebucketpolicy":      policyActionDeleteBucketPolicy,
	"getbucketversioning":     policyActionGetBucketVersioning,
	"putbucketversioning":     policyActionPutBucketVersioning,
	"getobject":               policyActionGetObject,
	"headobject":              policyActionHeadObject,
	"putobject":               policyActionPutObject,
	"deleteobject":            policyActionDeleteObject,
	"deletebucket":            policyActionDeleteBucket,
	"copyobject":              policyActionCopyObject,
	"createmultipartupload":   policyActionCreateMultipartUpload,
	"uploadpart":              policyActionUploadPart,
	"completemultipartupload": policyActionCompleteMultipart,
	"abortmultipartupload":    policyActionAbortMultipart,
	"listmultipartuploads":    policyActionListMultipartUploads,
	"listmultipartparts":      policyActionListMultipartParts,
}

func isAWSPolicyJSON(raw string) bool {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return false
	}
	if _, ok := m["Statement"]; ok {
		return true
	}
	if _, ok := m["Version"]; ok {
		return true
	}
	return false
}

func parseAWSPolicy(raw string) (*Policy, error) {
	var ap awsPolicy
	if err := json.Unmarshal([]byte(raw), &ap); err != nil {
		return nil, err
	}
	if len(ap.Statement) == 0 {
		return nil, errors.New("aws policy requires Statement")
	}
	out := &Policy{Version: "v1"}
	for _, stmt := range ap.Statement {
		if len(stmt.NotAction) > 0 || len(stmt.NotResource) > 0 {
			return nil, errors.New("aws policy NotAction/NotResource not supported")
		}
		if stmt.Principal != nil {
			if err := validateAWSPrincipal(stmt.Principal); err != nil {
				return nil, err
			}
		}
		effect := strings.ToLower(strings.TrimSpace(stmt.Effect))
		if effect != policyEffectAllow && effect != policyEffectDeny {
			return nil, errors.New("aws policy statement effect must be allow or deny")
		}
		actions, err := mapAWSActions(stmt.Action)
		if err != nil {
			return nil, err
		}
		resources, err := mapAWSResources(stmt.Resource)
		if err != nil {
			return nil, err
		}
		conds, err := mapAWSConditions(stmt.Condition)
		if err != nil {
			return nil, err
		}
		out.Statements = append(out.Statements, Statement{
			Effect:     effect,
			Actions:    actions,
			Resources:  resources,
			Conditions: conds,
		})
	}
	if err := out.validate(); err != nil {
		return nil, err
	}
	return out, nil
}

func mapAWSActions(actions []string) ([]string, error) {
	if len(actions) == 0 {
		return nil, errors.New("aws policy statement requires Action")
	}
	out := make([]string, 0, len(actions))
	for _, action := range actions {
		act := normalizeAction(action)
		if act == "" {
			return nil, fmt.Errorf("aws policy has invalid action %q", action)
		}
		if mapped, ok := awsActionToPolicy[act]; ok {
			out = append(out, mapped)
			continue
		}
		return nil, fmt.Errorf("aws policy action not supported: %q", action)
	}
	return out, nil
}

func mapAWSResources(resources []string) ([]Resource, error) {
	if len(resources) == 0 {
		return nil, errors.New("aws policy statement requires Resource")
	}
	out := make([]Resource, 0, len(resources))
	for _, res := range resources {
		bucket, prefix, err := parseS3Resource(res)
		if err != nil {
			return nil, err
		}
		out = append(out, Resource{Bucket: bucket, Prefix: prefix})
	}
	return out, nil
}

func parseS3Resource(resource string) (bucket, prefix string, err error) {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return "", "", errors.New("aws policy resource required")
	}
	if resource == "*" {
		return "*", "", nil
	}
	if !strings.HasPrefix(resource, "arn:") {
		return "", "", fmt.Errorf("aws policy resource unsupported: %q", resource)
	}
	parts := strings.SplitN(resource, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "s3" {
		return "", "", fmt.Errorf("aws policy resource invalid arn: %q", resource)
	}
	res := parts[5]
	if res == "" {
		return "", "", fmt.Errorf("aws policy resource invalid arn: %q", resource)
	}
	if res == "*" {
		return "*", "", nil
	}
	bucket = res
	if idx := strings.Index(res, "/"); idx >= 0 {
		bucket = res[:idx]
		prefix = res[idx+1:]
	}
	if bucket == "" {
		return "", "", fmt.Errorf("aws policy resource invalid arn: %q", resource)
	}
	prefix = strings.TrimPrefix(prefix, "/")
	if strings.Contains(prefix, "*") {
		if !strings.HasSuffix(prefix, "*") || strings.Contains(prefix[:len(prefix)-1], "*") {
			return "", "", fmt.Errorf("aws policy resource wildcard not supported: %q", resource)
		}
		prefix = strings.TrimSuffix(prefix, "*")
	}
	return bucket, prefix, nil
}

func mapAWSConditions(conds map[string]map[string]any) (Conditions, error) {
	if len(conds) == 0 {
		return Conditions{}, nil
	}
	var out Conditions
	for op, entries := range conds {
		switch strings.ToLower(op) {
		case "stringequals", "stringlike":
			for key, value := range entries {
				switch strings.ToLower(key) {
				case "s3:prefix":
					prefix, like, err := awsPrefixCondition(value, strings.EqualFold(op, "stringlike"))
					if err != nil {
						return Conditions{}, err
					}
					if out.Prefix != "" && out.Prefix != prefix {
						return Conditions{}, errors.New("aws policy condition prefix specified multiple times")
					}
					out.Prefix = prefix
					out.PrefixLike = like
				case "s3:delimiter":
					delimiter, err := awsSingleString(value)
					if err != nil {
						return Conditions{}, err
					}
					if strings.Contains(delimiter, "*") {
						return Conditions{}, errors.New("aws policy delimiter wildcard not supported")
					}
					if out.Delimiter != "" && out.Delimiter != delimiter {
						return Conditions{}, errors.New("aws policy condition delimiter specified multiple times")
					}
					out.Delimiter = delimiter
				default:
					return Conditions{}, fmt.Errorf("aws policy condition key not supported: %q", key)
				}
			}
		case "bool":
			for key, value := range entries {
				if strings.ToLower(key) != "aws:securetransport" {
					return Conditions{}, fmt.Errorf("aws policy condition key not supported: %q", key)
				}
				b, err := awsBoolValue(value)
				if err != nil {
					return Conditions{}, err
				}
				out.SecureTransport = &b
			}
		case "ipaddress":
			for key, value := range entries {
				if strings.ToLower(key) != "aws:sourceip" {
					return Conditions{}, fmt.Errorf("aws policy condition key not supported: %q", key)
				}
				ips, err := awsStringValue(value)
				if err != nil {
					return Conditions{}, err
				}
				out.SourceIP = append(out.SourceIP, ips...)
			}
		case "dategreaterthan":
			for key, value := range entries {
				if strings.ToLower(key) != "aws:currenttime" {
					return Conditions{}, fmt.Errorf("aws policy condition key not supported: %q", key)
				}
				when, err := awsSingleString(value)
				if err != nil {
					return Conditions{}, err
				}
				if out.After != "" {
					return Conditions{}, errors.New("aws policy condition after specified multiple times")
				}
				out.After = when
			}
		case "datelessthan":
			for key, value := range entries {
				if strings.ToLower(key) != "aws:currenttime" {
					return Conditions{}, fmt.Errorf("aws policy condition key not supported: %q", key)
				}
				when, err := awsSingleString(value)
				if err != nil {
					return Conditions{}, err
				}
				if out.Before != "" {
					return Conditions{}, errors.New("aws policy condition before specified multiple times")
				}
				out.Before = when
			}
		default:
			return Conditions{}, fmt.Errorf("aws policy condition operator not supported: %q", op)
		}
	}
	return out, nil
}

func awsStringValue(value any) ([]string, error) {
	switch v := value.(type) {
	case string:
		return []string{v}, nil
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, errors.New("aws policy condition value must be string")
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, errors.New("aws policy condition value must be string or list")
	}
}

func awsSingleString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []any:
		if len(v) != 1 {
			return "", errors.New("aws policy condition requires single value")
		}
		s, ok := v[0].(string)
		if !ok {
			return "", errors.New("aws policy condition value must be string")
		}
		return s, nil
	default:
		return "", errors.New("aws policy condition value must be string")
	}
}

func awsBoolValue(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, errors.New("aws policy condition value must be true or false")
		}
	case []any:
		if len(v) != 1 {
			return false, errors.New("aws policy condition requires single value")
		}
		return awsBoolValue(v[0])
	default:
		return false, errors.New("aws policy condition value must be true or false")
	}
}

func awsPrefixCondition(value any, allowLike bool) (string, bool, error) {
	prefix, err := awsSingleString(value)
	if err != nil {
		return "", false, err
	}
	if strings.Contains(prefix, "*") {
		if !allowLike {
			return "", false, errors.New("aws policy prefix wildcard not supported")
		}
		if !strings.HasSuffix(prefix, "*") || strings.Contains(prefix[:len(prefix)-1], "*") {
			return "", false, errors.New("aws policy prefix wildcard not supported")
		}
		return strings.TrimSuffix(prefix, "*"), true, nil
	}
	return prefix, false, nil
}

func validateAWSPrincipal(principal any) error {
	switch v := principal.(type) {
	case string:
		if v == "*" {
			return nil
		}
		return fmt.Errorf("aws policy principal not supported: %q", v)
	case map[string]any:
		if len(v) != 1 {
			return errors.New("aws policy principal not supported")
		}
		raw, ok := v["AWS"]
		if !ok {
			return errors.New("aws policy principal not supported")
		}
		vals, err := awsStringValue(raw)
		if err != nil {
			return err
		}
		if len(vals) == 1 && vals[0] == "*" {
			return nil
		}
		return errors.New("aws policy principal not supported")
	default:
		return errors.New("aws policy principal not supported")
	}
}
