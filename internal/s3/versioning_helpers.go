package s3

import (
	"context"
	"database/sql"
	"errors"

	"github.com/kk-code-lab/seglake/internal/meta"
)

func (h *Handler) bucketVersioningState(ctx context.Context, bucket string) (string, error) {
	if h == nil || h.Meta == nil {
		return meta.BucketVersioningEnabled, nil
	}
	state, err := h.Meta.GetBucketVersioningState(ctx, bucket)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return meta.BucketVersioningEnabled, nil
		}
		return "", err
	}
	return state, nil
}

func isNullVersioningState(state string) bool {
	return state == meta.BucketVersioningSuspended || state == meta.BucketVersioningDisabled
}

func versionIDHeaderForPut(state, versionID string) (string, bool) {
	switch state {
	case meta.BucketVersioningEnabled:
		if versionID != "" {
			return versionID, true
		}
	case meta.BucketVersioningSuspended:
		return "null", true
	case meta.BucketVersioningDisabled:
		return "", false
	}
	return "", false
}

func versionIDHeaderForMeta(state string, obj *meta.ObjectMeta) (string, bool) {
	if obj == nil {
		return "", false
	}
	switch state {
	case meta.BucketVersioningEnabled:
		if obj.VersionID != "" {
			return obj.VersionID, true
		}
	case meta.BucketVersioningSuspended:
		if obj.IsNull {
			return "null", true
		}
		if obj.VersionID != "" {
			return obj.VersionID, true
		}
	case meta.BucketVersioningDisabled:
		return "", false
	}
	return "", false
}
