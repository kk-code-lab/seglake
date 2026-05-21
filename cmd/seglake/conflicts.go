package main

import (
	"context"
	"fmt"

	"github.com/kk-code-lab/seglake/internal/meta"
)

type conflictListItem struct {
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	VersionID    string `json:"version_id"`
	ETag         string `json:"etag,omitempty"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified_utc"`
}

type conflictListResponse struct {
	Items       []conflictListItem `json:"items"`
	NextBucket  string             `json:"next_bucket,omitempty"`
	NextKey     string             `json:"next_key,omitempty"`
	NextVersion string             `json:"next_version,omitempty"`
}

func runConflicts(metaPath, bucket, prefix, afterBucket, afterKey, afterVersion string, limit int, jsonOut bool) error {
	if metaPath == "" {
		return ErrMetaPathRequired
	}
	resp, err := collectConflicts(metaPath, bucket, prefix, afterBucket, afterKey, afterVersion, limit)
	if err != nil {
		return err
	}
	return formatConflicts(resp, jsonOut)
}

func collectConflicts(metaPath, bucket, prefix, afterBucket, afterKey, afterVersion string, limit int) (*conflictListResponse, error) {
	if limit <= 0 || limit > 10000 {
		return nil, fmt.Errorf("invalid conflicts-limit %d", limit)
	}
	store, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	items, err := store.ListConflicts(context.Background(), bucket, prefix, afterBucket, afterKey, afterVersion, limit)
	if err != nil {
		return nil, err
	}
	resp := &conflictListResponse{
		Items: make([]conflictListItem, 0, len(items)),
	}
	for _, item := range items {
		resp.Items = append(resp.Items, conflictListItem{
			Bucket:       item.Bucket,
			Key:          item.Key,
			VersionID:    item.VersionID,
			ETag:         item.ETag,
			Size:         item.Size,
			LastModified: item.LastModified,
		})
	}
	if len(items) == limit {
		last := items[len(items)-1]
		resp.NextBucket = last.Bucket
		resp.NextKey = last.Key
		resp.NextVersion = last.VersionID
	}
	return resp, nil
}

func formatConflicts(resp *conflictListResponse, jsonOut bool) error {
	if resp == nil {
		resp = &conflictListResponse{}
	}
	if jsonOut {
		if resp.Items == nil {
			resp.Items = []conflictListItem{}
		}
		return writeJSON(resp)
	}
	for _, item := range resp.Items {
		fmt.Printf("bucket=%s key=%s version=%s size=%d last_modified=%s etag=%s\n",
			item.Bucket, item.Key, item.VersionID, item.Size, item.LastModified, item.ETag)
	}
	if resp.NextBucket != "" || resp.NextKey != "" || resp.NextVersion != "" {
		fmt.Printf("next_bucket=%s next_key=%s next_version=%s\n", resp.NextBucket, resp.NextKey, resp.NextVersion)
	}
	return nil
}
