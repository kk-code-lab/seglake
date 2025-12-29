package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runBucketPolicy(action, metaPath, bucket, policy, policyFile string, jsonOut bool) error {
	if metaPath == "" {
		return errors.New("meta path required")
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
		value, err := store.GetBucketPolicy(context.Background(), bucket)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if jsonOut {
					return writeJSON(map[string]any{"policy": nil})
				}
				return nil
			}
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"policy": value})
		}
		fmt.Println(value)
		return nil
	case "set":
		if bucket == "" {
			return errors.New("bucket-policy-bucket required")
		}
		if policy == "" && policyFile != "" {
			data, err := os.ReadFile(policyFile)
			if err != nil {
				return err
			}
			policy = string(data)
		}
		if policy == "" {
			return errors.New("bucket-policy required")
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
			return errors.New("bucket-policy-bucket required")
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
