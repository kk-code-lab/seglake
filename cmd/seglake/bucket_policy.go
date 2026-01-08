package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/kk-code-lab/seglake/internal/admin"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runBucketPolicy(action, metaPath, bucket, policy, policyFile string, jsonOut bool) error {
	if policy == "" && policyFile != "" {
		data, err := os.ReadFile(policyFile)
		if err != nil {
			return err
		}
		policy = string(data)
	}
	if client, ok, err := adminClientIfRunning(filepath.Dir(metaPath)); err != nil {
		return err
	} else if ok {
		req := admin.BucketPolicyRequest{
			Action: action,
			Bucket: bucket,
			Policy: policy,
		}
		switch action {
		case "get":
			if bucket == "" {
				var policies map[string]string
				if err := client.postJSON("/admin/bucket-policy", req, &policies); err != nil {
					return err
				}
				return formatBucketPolicyList(policies, jsonOut)
			}
			var resp map[string]any
			if err := client.postJSON("/admin/bucket-policy", req, &resp); err != nil {
				return err
			}
			return formatBucketPolicyGet(resp, jsonOut)
		default:
			var resp map[string]string
			if err := client.postJSON("/admin/bucket-policy", req, &resp); err != nil {
				return err
			}
			if jsonOut {
				return writeJSON(resp)
			}
			fmt.Println("ok")
			return nil
		}
	}
	if metaPath == "" {
		return ErrMetaPathRequired
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	switch action {
	case "get":
		if bucket == "" {
			policies, err := store.ListBucketPolicies(context.Background())
			if err != nil {
				return err
			}
			return formatBucketPolicyList(policies, jsonOut)
		}
		value, err := store.GetBucketPolicy(context.Background(), bucket)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return formatBucketPolicyGet(map[string]any{"policy": nil}, jsonOut)
			}
			return err
		}
		return formatBucketPolicyGet(map[string]any{"policy": value}, jsonOut)
	case "set":
		if bucket == "" {
			return ErrBucketPolicyBucketNeeded
		}
		if policy == "" {
			return ErrBucketPolicyNeeded
		}
		if _, err := s3.ParsePolicy(policy); err != nil {
			return fmt.Errorf("invalid policy: %w", err)
		}
		if err := store.SetBucketPolicy(context.Background(), bucket, policy); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "delete":
		if bucket == "" {
			return ErrBucketPolicyBucketNeeded
		}
		if err := store.DeleteBucketPolicy(context.Background(), bucket); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	default:
		return fmt.Errorf("unknown bucket-policy-action %q", action)
	}
}

func formatBucketPolicyList(policies map[string]string, jsonOut bool) error {
	if jsonOut {
		if policies == nil {
			policies = map[string]string{}
		}
		return writeJSON(policies)
	}
	names := make([]string, 0, len(policies))
	for name := range policies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := policies[name]
		fmt.Printf("bucket=%s policy=%s\n", name, value)
	}
	return nil
}

func formatBucketPolicyGet(resp map[string]any, jsonOut bool) error {
	if jsonOut {
		return writeJSON(resp)
	}
	if resp == nil {
		return nil
	}
	value, ok := resp["policy"]
	if !ok || value == nil {
		return nil
	}
	fmt.Println(value)
	return nil
}
