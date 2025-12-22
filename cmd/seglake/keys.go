package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runKeys(action, metaPath, accessKey, secretKey, policy, bucket string, enabled bool, inflight int64, jsonOut bool) error {
	if metaPath == "" {
		return errors.New("meta path required")
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	switch action {
	case "list":
		keys, err := store.ListAPIKeys(context.Background())
		if err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(keys)
		}
		for _, key := range keys {
			state := "disabled"
			if key.Enabled {
				state = "enabled"
			}
			fmt.Printf("access_key=%s state=%s policy=%s inflight=%d last_used=%s\n", key.AccessKey, state, key.Policy, key.InflightLimit, key.LastUsedAt)
		}
		return nil
	case "create":
		if accessKey == "" || secretKey == "" {
			return errors.New("key-access and key-secret required")
		}
		if _, err := s3.ParsePolicy(policy); err != nil {
			return fmt.Errorf("invalid policy: %w", err)
		}
		if err := store.UpsertAPIKey(context.Background(), accessKey, secretKey, policy, enabled, inflight); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "allow-bucket":
		if accessKey == "" || bucket == "" {
			return errors.New("key-access and key-bucket required")
		}
		if err := store.AllowBucketForKey(context.Background(), accessKey, bucket); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "disallow-bucket":
		if accessKey == "" || bucket == "" {
			return errors.New("key-access and key-bucket required")
		}
		if err := store.DisallowBucketForKey(context.Background(), accessKey, bucket); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "list-buckets":
		if accessKey == "" {
			return errors.New("key-access required")
		}
		buckets, err := store.ListAllowedBuckets(context.Background(), accessKey)
		if err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(buckets)
		}
		for _, name := range buckets {
			fmt.Println(name)
		}
		return nil
	case "enable":
		if accessKey == "" {
			return errors.New("key-access required")
		}
		if err := store.SetAPIKeyEnabled(context.Background(), accessKey, true); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "disable":
		if accessKey == "" {
			return errors.New("key-access required")
		}
		if err := store.SetAPIKeyEnabled(context.Background(), accessKey, false); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "delete":
		if accessKey == "" {
			return errors.New("key-access required")
		}
		if err := store.DeleteAPIKey(context.Background(), accessKey); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "set-policy":
		if accessKey == "" {
			return errors.New("key-access required")
		}
		if _, err := s3.ParsePolicy(policy); err != nil {
			return fmt.Errorf("invalid policy: %w", err)
		}
		if err := store.UpdateAPIKeyPolicy(context.Background(), accessKey, policy); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	default:
		return fmt.Errorf("unknown keys-action %q", action)
	}
}

func writeJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
