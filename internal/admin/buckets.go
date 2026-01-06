package admin

import (
	"context"

	"github.com/kk-code-lab/seglake/internal/meta"
)

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
