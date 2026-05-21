package main

import (
	"context"
	"database/sql"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
)

func TestCollectConflictsFiltersAndPaginates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, metaPath := newTestMetaStore(t)

	for _, obj := range []struct {
		bucket  string
		key     string
		version string
	}{
		{"b1", "a/1", "v1"},
		{"b1", "a/2", "v2"},
		{"b2", "a/3", "v3"},
	} {
		if err := store.RecordPut(ctx, obj.bucket, obj.key, obj.version, "etag-"+obj.version, 1, "/tmp/"+obj.version, "text/plain"); err != nil {
			t.Fatalf("RecordPut %s: %v", obj.version, err)
		}
	}
	if err := store.WithTx(func(tx *sql.Tx) error {
		return meta.ExecTx(tx, "UPDATE versions SET state='CONFLICT' WHERE version_id IN (?, ?, ?)", "v1", "v2", "v3")
	}); err != nil {
		t.Fatalf("mark conflicts: %v", err)
	}

	page1, err := collectConflicts(metaPath, "b1", "a/", "", "", "", 1)
	if err != nil {
		t.Fatalf("collectConflicts page1: %v", err)
	}
	if len(page1.Items) != 1 || page1.Items[0].Bucket != "b1" || page1.Items[0].Key != "a/1" {
		t.Fatalf("unexpected page1: %+v", page1)
	}
	if page1.NextBucket != "b1" || page1.NextKey != "a/1" || page1.NextVersion != "v1" {
		t.Fatalf("unexpected next markers: %+v", page1)
	}

	page2, err := collectConflicts(metaPath, "b1", "a/", page1.NextBucket, page1.NextKey, page1.NextVersion, 10)
	if err != nil {
		t.Fatalf("collectConflicts page2: %v", err)
	}
	if len(page2.Items) != 1 || page2.Items[0].Bucket != "b1" || page2.Items[0].Key != "a/2" {
		t.Fatalf("unexpected page2: %+v", page2)
	}
}

func TestCollectConflictsRejectsInvalidLimit(t *testing.T) {
	t.Parallel()
	_, metaPath := newTestMetaStore(t)
	if _, err := collectConflicts(metaPath, "", "", "", "", "", 0); err == nil {
		t.Fatalf("expected invalid limit error")
	}
	if _, err := collectConflicts(metaPath, "", "", "", "", "", 10001); err == nil {
		t.Fatalf("expected invalid limit error")
	}
}
