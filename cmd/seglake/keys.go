package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kk-code-lab/seglake/internal/meta"
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
