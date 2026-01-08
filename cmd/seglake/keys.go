package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kk-code-lab/seglake/internal/admin"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runKeys(action, metaPath, accessKey, secretKey, policy, bucket string, enabled bool, inflight int64, jsonOut bool) error {
	if client, ok, err := adminClientIfRunning(filepath.Dir(metaPath)); err != nil {
		return err
	} else if ok {
		req := admin.KeysRequest{
			Action:    action,
			AccessKey: accessKey,
			SecretKey: secretKey,
			Policy:    policy,
			Bucket:    bucket,
			Inflight:  inflight,
		}
		if action == "create" {
			req.Enabled = &enabled
		}
		switch action {
		case "list":
			var keys []meta.APIKey
			if err := client.postJSON("/admin/keys", req, &keys); err != nil {
				return err
			}
			return formatKeysList(keys, jsonOut)
		case "list-buckets":
			var buckets []string
			if err := client.postJSON("/admin/keys", req, &buckets); err != nil {
				return err
			}
			return formatKeyBuckets(buckets, jsonOut)
		case "list-buckets-all":
			var keyBuckets map[string][]string
			if err := client.postJSON("/admin/keys", req, &keyBuckets); err != nil {
				return err
			}
			return formatAllKeyBuckets(keyBuckets, jsonOut)
		default:
			var resp map[string]string
			if err := client.postJSON("/admin/keys", req, &resp); err != nil {
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
	case "list":
		keys, err := store.ListAPIKeys(context.Background())
		if err != nil {
			return err
		}
		return formatKeysList(keys, jsonOut)
	case "create":
		if accessKey == "" || secretKey == "" {
			return ErrKeyAccessSecretNeeded
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
			return ErrKeyAccessBucketNeeded
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
			return ErrKeyAccessBucketNeeded
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
			return ErrKeyAccessNeeded
		}
		buckets, err := store.ListAllowedBuckets(context.Background(), accessKey)
		if err != nil {
			return err
		}
		return formatKeyBuckets(buckets, jsonOut)
	case "list-buckets-all":
		keyBuckets, err := store.ListAllKeyBuckets(context.Background())
		if err != nil {
			return err
		}
		return formatAllKeyBuckets(keyBuckets, jsonOut)
	case "enable":
		if accessKey == "" {
			return ErrKeyAccessNeeded
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
			return ErrKeyAccessNeeded
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
			return ErrKeyAccessNeeded
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
			return ErrKeyAccessNeeded
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

func formatKeysList(keys []meta.APIKey, jsonOut bool) error {
	if jsonOut {
		if keys == nil {
			keys = []meta.APIKey{}
		}
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
}

func formatKeyBuckets(buckets []string, jsonOut bool) error {
	if jsonOut {
		if buckets == nil {
			buckets = []string{}
		}
		return writeJSON(buckets)
	}
	for _, name := range buckets {
		fmt.Println(name)
	}
	return nil
}

func formatAllKeyBuckets(keyBuckets map[string][]string, jsonOut bool) error {
	if jsonOut {
		if keyBuckets == nil {
			keyBuckets = map[string][]string{}
		}
		return writeJSON(keyBuckets)
	}
	accessKeys := make([]string, 0, len(keyBuckets))
	for access := range keyBuckets {
		accessKeys = append(accessKeys, access)
	}
	sort.Strings(accessKeys)
	for _, access := range accessKeys {
		buckets := keyBuckets[access]
		if len(buckets) == 0 {
			fmt.Printf("access_key=%s buckets=\n", access)
			continue
		}
		fmt.Printf("access_key=%s buckets=%s\n", access, strings.Join(buckets, ","))
	}
	return nil
}
