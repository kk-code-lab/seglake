package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
)

func runBuckets(action, metaPath, bucket string, jsonOut bool) error {
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
		if err := store.CreateBucket(context.Background(), bucket); err != nil {
			return err
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
			return errors.New("bucket not found")
		}
		hasObjects, err := store.BucketHasObjects(context.Background(), bucket)
		if err != nil {
			return err
		}
		if hasObjects {
			return errors.New("bucket not empty")
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
			if err := writeJSON(map[string]bool{"exists": exists}); err != nil {
				return err
			}
			if !exists {
				return &exitCodeError{code: 1, quiet: true}
			}
			return nil
		}
		fmt.Println(exists)
		if !exists {
			return &exitCodeError{code: 1, quiet: true}
		}
		return nil
	default:
		return fmt.Errorf("unknown bucket-action %q", action)
	}
}
