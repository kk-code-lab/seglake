package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kk-code-lab/seglake/internal/admin"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runBuckets(action, metaPath, bucket, versioning string, force bool, jsonOut bool) error {
	if client, ok, err := adminClientIfRunning(filepath.Dir(metaPath)); err != nil {
		return err
	} else if ok {
		req := admin.BucketsRequest{
			Action:     action,
			Bucket:     bucket,
			Versioning: versioning,
			Force:      force,
		}
		switch action {
		case "list":
			var buckets []string
			if err := client.postJSON("/admin/buckets", req, &buckets); err != nil {
				return err
			}
			return formatBucketsList(buckets, jsonOut)
		case "exists":
			var resp map[string]bool
			if err := client.postJSON("/admin/buckets", req, &resp); err != nil {
				return err
			}
			return formatBucketExists(resp, jsonOut)
		default:
			var resp map[string]string
			if err := client.postJSON("/admin/buckets", req, &resp); err != nil {
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
		buckets, err := store.ListBuckets(context.Background())
		if err != nil {
			return err
		}
		return formatBucketsList(buckets, jsonOut)
	case "create":
		if bucket == "" {
			return ErrBucketRequired
		}
		if err := s3.ValidateBucketName(bucket); err != nil {
			return err
		}
		if strings.TrimSpace(versioning) == "" {
			if err := store.CreateBucket(context.Background(), bucket); err != nil {
				return err
			}
		} else {
			if err := store.CreateBucketWithVersioning(context.Background(), bucket, versioning); err != nil {
				return err
			}
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "delete":
		if bucket == "" {
			return ErrBucketRequired
		}
		exists, err := store.BucketExists(context.Background(), bucket)
		if err != nil {
			return err
		}
		if !exists {
			if jsonOut {
				return writeJSON(map[string]string{"status": "ok"})
			}
			fmt.Println("ok")
			return nil
		}
		if force {
			if err := deleteBucketObjects(context.Background(), store, bucket); err != nil {
				return err
			}
		} else {
			hasObjects, err := store.BucketHasObjects(context.Background(), bucket)
			if err != nil {
				return err
			}
			if hasObjects {
				return ErrBucketNotEmpty
			}
		}
		if err := store.DeleteBucket(context.Background(), bucket); err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]string{"status": "ok"})
		}
		fmt.Println("ok")
		return nil
	case "exists":
		if bucket == "" {
			return ErrBucketRequired
		}
		exists, err := store.BucketExists(context.Background(), bucket)
		if err != nil {
			return err
		}
		return formatBucketExists(map[string]bool{"exists": exists}, jsonOut)
	default:
		return fmt.Errorf("unknown bucket-action %q", action)
	}
}

func formatBucketsList(buckets []string, jsonOut bool) error {
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

func formatBucketExists(resp map[string]bool, jsonOut bool) error {
	if jsonOut {
		return writeJSON(resp)
	}
	if resp == nil {
		fmt.Println(false)
		return nil
	}
	fmt.Println(resp["exists"])
	return nil
}

func deleteBucketObjects(ctx context.Context, store *meta.Store, bucket string) error {
	versioningState, err := store.GetBucketVersioningState(ctx, bucket)
	if err != nil {
		return err
	}
	afterKey := ""
	afterVersion := ""
	for {
		objects, err := store.ListObjects(ctx, bucket, "", afterKey, afterVersion, 1000)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return nil
		}
		for _, obj := range objects {
			if versioningState == meta.BucketVersioningDisabled {
				if _, err := store.DeleteObjectUnversioned(ctx, bucket, obj.Key); err != nil {
					return err
				}
			} else {
				if _, err := store.DeleteObject(ctx, bucket, obj.Key); err != nil {
					return err
				}
			}
		}
		last := objects[len(objects)-1]
		afterKey = last.Key
		afterVersion = last.VersionID
	}
}
