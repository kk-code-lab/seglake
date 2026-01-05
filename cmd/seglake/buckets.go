package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runBuckets(action, metaPath, bucket, versioning string, force bool, jsonOut bool) error {
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
		buckets, err := store.ListBuckets(context.Background())
		if err != nil {
			return err
		}
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
	case "create":
		if bucket == "" {
			return errors.New("bucket required")
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
			return errors.New("bucket required")
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
				return errors.New("bucket not empty")
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
			return errors.New("bucket required")
		}
		exists, err := store.BucketExists(context.Background(), bucket)
		if err != nil {
			return err
		}
		if jsonOut {
			return writeJSON(map[string]bool{"exists": exists})
		}
		fmt.Println(exists)
		return nil
	default:
		return fmt.Errorf("unknown bucket-action %q", action)
	}
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
