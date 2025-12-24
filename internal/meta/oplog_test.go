package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestOplogPutDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.SetSiteID("site-a")

	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 123, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if _, err := store.DeleteObject(context.Background(), "bucket", "key"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].OpType != "put" || entries[0].Bucket != "bucket" || entries[0].Key != "key" || entries[0].VersionID != "v1" {
		t.Fatalf("unexpected put entry: %+v", entries[0])
	}
	if entries[0].SiteID != "site-a" || entries[0].HLCTS == "" {
		t.Fatalf("expected site/hlc, got site=%q hlc=%q", entries[0].SiteID, entries[0].HLCTS)
	}
	if entries[1].OpType != "delete" || entries[1].Bucket != "bucket" || entries[1].Key != "key" || entries[1].VersionID != "v1" {
		t.Fatalf("unexpected delete entry: %+v", entries[1])
	}
	if entries[1].SiteID != "site-a" || entries[1].HLCTS == "" {
		t.Fatalf("expected site/hlc, got site=%q hlc=%q", entries[1].SiteID, entries[1].HLCTS)
	}
}

func TestOplogSinceLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.SetSiteID("site-a")

	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 123, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v2", "etag", 124, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}

	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	since := entries[0].HLCTS
	filtered, err := store.ListOplogSince(context.Background(), since, 10)
	if err != nil {
		t.Fatalf("ListOplogSince: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(filtered))
	}
	if filtered[0].VersionID != "v2" {
		t.Fatalf("expected v2, got %s", filtered[0].VersionID)
	}

	limited, err := store.ListOplogSince(context.Background(), "", 1)
	if err != nil {
		t.Fatalf("ListOplogSince limit: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(limited))
	}
}

func TestApplyOplogEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         123,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	put := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000001-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	applied, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{put})
	if err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected applied=1, got %d", applied)
	}
	meta, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if meta.VersionID != "v1" {
		t.Fatalf("expected current v1, got %s", meta.VersionID)
	}

	deletePayload, err := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	del := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000002-0000000001",
		OpType:    "delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(deletePayload),
	}
	applied, err = store.ApplyOplogEntries(context.Background(), []OplogEntry{del, del})
	if err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected applied=1 for delete, got %d", applied)
	}
	if _, err := store.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected object to be deleted")
	}
}

func TestMaxOplogHLC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hlc, err := store.MaxOplogHLC(context.Background())
	if err != nil {
		t.Fatalf("MaxOplogHLC: %v", err)
	}
	if hlc != "" {
		t.Fatalf("expected empty HLC, got %q", hlc)
	}
	if err := store.RecordPut(context.Background(), "bucket", "key", "v1", "etag", 1, "", ""); err != nil {
		t.Fatalf("RecordPut: %v", err)
	}
	hlc, err = store.MaxOplogHLC(context.Background())
	if err != nil {
		t.Fatalf("MaxOplogHLC: %v", err)
	}
	if hlc == "" {
		t.Fatalf("expected HLC value")
	}
}

func TestApplyOplogPutTieBreakSite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         1,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	hlc := "0000000000000000100-0000000001"
	putA := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     hlc,
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	putB := OplogEntry{
		SiteID:    "site-z",
		HLCTS:     hlc,
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v2",
		Payload:   string(putPayload),
	}
	if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{putA, putB}); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	metaObj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != "v2" {
		t.Fatalf("expected site tie-break to pick v2, got %s", metaObj.VersionID)
	}
}

func TestApplyOplogDeleteConflicts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         123,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	mpuPayload, err := json.Marshal(oplogMPUCompletePayload{
		ETag:         "etag-mpu",
		Size:         10,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	delPayload, err := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	cases := []struct {
		name      string
		firstOp   string
		firstHLC  string
		firstV    string
		firstBody string
		secondHLC string
		secondV   string
	}{
		{"put_vs_delete", "put", "0000000000000000005-0000000001", "v1", string(putPayload), "0000000000000000006-0000000001", "v2"},
		{"mpu_vs_delete", "mpu_complete", "0000000000000000100-0000000001", "v1", string(mpuPayload), "0000000000000000101-0000000001", "v2"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			put := OplogEntry{
				SiteID:    "site-a",
				HLCTS:     tc.firstHLC,
				OpType:    tc.firstOp,
				Bucket:    "bucket",
				Key:       "key",
				VersionID: "v1",
				Payload:   tc.firstBody,
			}
			del := OplogEntry{
				SiteID:    "site-b",
				HLCTS:     tc.secondHLC,
				OpType:    "delete",
				Bucket:    "bucket",
				Key:       "key",
				VersionID: "v1",
				Payload:   string(delPayload),
			}
			if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{put, del}); err != nil {
				t.Fatalf("ApplyOplogEntries: %v", err)
			}
			if _, err := store.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
				t.Fatalf("expected delete to win")
			}
			next := OplogEntry{
				SiteID:    "site-c",
				HLCTS:     addHLC(tc.secondHLC, 1),
				OpType:    tc.firstOp,
				Bucket:    "bucket",
				Key:       "key",
				VersionID: tc.secondV,
				Payload:   tc.firstBody,
			}
			if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{next}); err != nil {
				t.Fatalf("ApplyOplogEntries: %v", err)
			}
			metaObj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
			if err != nil {
				t.Fatalf("GetObjectMeta: %v", err)
			}
			if metaObj.VersionID != tc.secondV {
				t.Fatalf("expected %s to win, got %s", tc.secondV, metaObj.VersionID)
			}
		})
	}
}

func TestRecordMPUCompleteWritesOplog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.RecordMPUComplete(context.Background(), "bucket", "key", "v1", "etag-mpu", 10); err != nil {
		t.Fatalf("RecordMPUComplete: %v", err)
	}
	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].OpType != "mpu_complete" {
		t.Fatalf("expected mpu_complete, got %s", entries[0].OpType)
	}
}

func TestRecordAPIKeyWritesOplog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertAPIKey(context.Background(), "access-1", "secret-1", "ro", true, 12); err != nil {
		t.Fatalf("UpsertAPIKey: %v", err)
	}
	entries, err := store.ListOplog(context.Background())
	if err != nil {
		t.Fatalf("ListOplog: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].OpType != "api_key" || entries[0].Bucket != metaOplogBucket || entries[0].Key != "access-1" {
		t.Fatalf("unexpected entry: %+v", entries[0])
	}
	var payload oplogAPIKeyPayload
	if err := json.Unmarshal([]byte(entries[0].Payload), &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload.AccessKey != "access-1" || payload.SecretKey != "secret-1" || payload.Policy != "ro" || payload.InflightLimit != 12 || !payload.Enabled {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestApplyOplogAPIKeyAndAllowlist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	keyPayload, err := json.Marshal(oplogAPIKeyPayload{
		AccessKey:     "access-1",
		SecretKey:     "secret-1",
		Enabled:       true,
		Policy:        "rw",
		InflightLimit: 5,
		UpdatedAt:     "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	allowPayload, err := json.Marshal(oplogAPIKeyBucketPayload{
		AccessKey: "access-1",
		Bucket:    "demo",
		Allowed:   true,
		UpdatedAt: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	denyPayload, err := json.Marshal(oplogAPIKeyBucketPayload{
		AccessKey: "access-1",
		Bucket:    "demo",
		Allowed:   false,
		UpdatedAt: "2025-12-22T12:02:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	entries := []OplogEntry{
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000100-0000000001",
			OpType:  "api_key",
			Bucket:  metaOplogBucket,
			Key:     "access-1",
			Payload: string(keyPayload),
		},
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000101-0000000001",
			OpType:  "api_key_bucket",
			Bucket:  metaOplogBucket,
			Key:     "access-1",
			Payload: string(allowPayload),
		},
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000102-0000000001",
			OpType:  "api_key_bucket",
			Bucket:  metaOplogBucket,
			Key:     "access-1",
			Payload: string(denyPayload),
		},
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	key, err := store.GetAPIKey(context.Background(), "access-1")
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if key.SecretKey != "secret-1" || !key.Enabled || key.Policy != "rw" || key.InflightLimit != 5 {
		t.Fatalf("unexpected key: %+v", key)
	}
	allowed, err := store.IsBucketAllowed(context.Background(), "access-1", "demo")
	if err != nil {
		t.Fatalf("IsBucketAllowed: %v", err)
	}
	if !allowed {
		t.Fatalf("expected demo to be allowed after deny removed the allowlist entry")
	}
	buckets, err := store.ListAllowedBuckets(context.Background(), "access-1")
	if err != nil {
		t.Fatalf("ListAllowedBuckets: %v", err)
	}
	if len(buckets) != 0 {
		t.Fatalf("expected allowlist to be empty, got %v", buckets)
	}
}

func TestApplyOplogBucketPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	policyPayload, err := json.Marshal(oplogBucketPolicyPayload{
		Bucket:    "demo",
		Policy:    "{\"Version\":\"2012-10-17\"}",
		UpdatedAt: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	entries := []OplogEntry{
		{
			SiteID:  "site-a",
			HLCTS:   "0000000000000000200-0000000001",
			OpType:  "bucket_policy",
			Bucket:  "demo",
			Key:     "demo",
			Payload: string(policyPayload),
		},
		{
			SiteID: "site-a",
			HLCTS:  "0000000000000000201-0000000001",
			OpType: "bucket_policy_delete",
			Bucket: "demo",
			Key:    "demo",
		},
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[:1]); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	policy, err := store.GetBucketPolicy(context.Background(), "demo")
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if policy != "{\"Version\":\"2012-10-17\"}" {
		t.Fatalf("unexpected policy: %s", policy)
	}
	if _, err := store.ApplyOplogEntries(context.Background(), entries[1:]); err != nil {
		t.Fatalf("ApplyOplogEntries delete: %v", err)
	}
	if _, err := store.GetBucketPolicy(context.Background(), "demo"); err == nil {
		t.Fatalf("expected policy to be deleted")
	}
}

func TestApplyOplogIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         1,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	entry := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000300-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	applied, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{entry, entry})
	if err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected 1 applied entry, got %d", applied)
	}
	applied, err = store.ApplyOplogEntries(context.Background(), []OplogEntry{entry})
	if err != nil {
		t.Fatalf("ApplyOplogEntries again: %v", err)
	}
	if applied != 0 {
		t.Fatalf("expected 0 applied entries, got %d", applied)
	}
	metaObj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != "v1" {
		t.Fatalf("expected v1, got %s", metaObj.VersionID)
	}
}

func TestApplyOplogSplitBrainConverges(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	storeA, err := Open(filepath.Join(dir, "meta-a.db"))
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := Open(filepath.Join(dir, "meta-b.db"))
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })

	entryA := makePutEntry("site-a", "0000000000000000400-0000000001", "bucket", "key", "v1")
	entryB := makePutEntry("site-z", "0000000000000000400-0000000001", "bucket", "key", "v2")

	if _, err := storeA.ApplyOplogEntries(context.Background(), []OplogEntry{entryA, entryB}); err != nil {
		t.Fatalf("ApplyOplogEntries A: %v", err)
	}
	if _, err := storeB.ApplyOplogEntries(context.Background(), []OplogEntry{entryB, entryA}); err != nil {
		t.Fatalf("ApplyOplogEntries B: %v", err)
	}
	objA, err := storeA.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta A: %v", err)
	}
	objB, err := storeB.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta B: %v", err)
	}
	if objA.VersionID != objB.VersionID {
		t.Fatalf("expected convergence, got %s vs %s", objA.VersionID, objB.VersionID)
	}
	if objA.VersionID != "v2" {
		t.Fatalf("expected site-z to win, got %s", objA.VersionID)
	}
}

func TestApplyOplogReorderAndDuplicatesConverge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	storeA, err := Open(filepath.Join(dir, "meta-a.db"))
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := Open(filepath.Join(dir, "meta-b.db"))
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })

	putA := makePutEntry("site-a", "0000000000000000500-0000000001", "bucket", "key", "v1")
	putB := makePutEntry("site-b", "0000000000000000501-0000000001", "bucket", "key", "v2")
	del := makeDeleteEntry("site-b", "0000000000000000502-0000000001", "bucket", "key", "v1")

	if _, err := storeA.ApplyOplogEntries(context.Background(), []OplogEntry{del, putA, putA, putB}); err != nil {
		t.Fatalf("ApplyOplogEntries A: %v", err)
	}
	if _, err := storeB.ApplyOplogEntries(context.Background(), []OplogEntry{putB, del, putA}); err != nil {
		t.Fatalf("ApplyOplogEntries B: %v", err)
	}
	if _, err := storeB.ApplyOplogEntries(context.Background(), []OplogEntry{putA}); err != nil {
		t.Fatalf("ApplyOplogEntries B dup: %v", err)
	}
	if _, err := storeA.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected delete to win")
	}
	if _, err := storeB.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected delete to win")
	}
}

func TestApplyOplogTracksConflicts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putOld := makePutEntry("site-a", "0000000000000000600-0000000001", "bucket", "key", "v1")
	putNew := makePutEntry("site-b", "0000000000000000601-0000000001", "bucket", "key", "v2")
	if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{putNew, putOld}); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.ReplConflicts != 1 {
		t.Fatalf("expected repl conflicts=1 got %d", stats.ReplConflicts)
	}
	metaObj, err := store.GetObjectVersion(context.Background(), "bucket", "key", "v1")
	if err != nil {
		t.Fatalf("GetObjectVersion: %v", err)
	}
	if metaObj.State != "CONFLICT" {
		t.Fatalf("expected conflict state, got %s", metaObj.State)
	}
}

func TestApplyOplogPutVsPut(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         1,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	put1 := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000010-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	put2 := OplogEntry{
		SiteID:    "site-b",
		HLCTS:     "0000000000000000011-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v2",
		Payload:   string(putPayload),
	}
	if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{put1, put2}); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	metaObj, err := store.GetObjectMeta(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if metaObj.VersionID != "v2" {
		t.Fatalf("expected v2 to win, got %s", metaObj.VersionID)
	}
}

func TestApplyOplogDeleteVsDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	delPayload, err := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	del1 := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000020-0000000001",
		OpType:    "delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(delPayload),
	}
	del2 := OplogEntry{
		SiteID:    "site-z",
		HLCTS:     "0000000000000000020-0000000001",
		OpType:    "delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(delPayload),
	}
	if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{del1, del2}); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if _, err := store.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected object to remain deleted")
	}
}

func TestApplyOplogDeleteThenPutOutOfOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putPayload, err := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         1,
		LastModified: "2025-12-22T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	delPayload, err := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	del := OplogEntry{
		SiteID:    "site-b",
		HLCTS:     "0000000000000000060-0000000001",
		OpType:    "delete",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(delPayload),
	}
	put := OplogEntry{
		SiteID:    "site-a",
		HLCTS:     "0000000000000000059-0000000001",
		OpType:    "put",
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
		Payload:   string(putPayload),
	}
	if _, err := store.ApplyOplogEntries(context.Background(), []OplogEntry{del, put}); err != nil {
		t.Fatalf("ApplyOplogEntries: %v", err)
	}
	if _, err := store.GetObjectMeta(context.Background(), "bucket", "key"); err == nil {
		t.Fatalf("expected delete to win with higher HLC even if applied first")
	}
}

func addHLC(hlc string, delta int64) string {
	parts := strings.SplitN(hlc, "-", 2)
	if len(parts) != 2 {
		return hlc
	}
	physical, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return hlc
	}
	return fmt.Sprintf("%019d-%s", physical+delta, parts[1])
}

func makePutEntry(siteID, hlc, bucket, key, versionID string) OplogEntry {
	payload, _ := json.Marshal(oplogPutPayload{
		ETag:         "etag",
		Size:         1,
		LastModified: "2025-12-22T12:00:00Z",
	})
	return OplogEntry{
		SiteID:    siteID,
		HLCTS:     hlc,
		OpType:    "put",
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Payload:   string(payload),
	}
}

func makeDeleteEntry(siteID, hlc, bucket, key, versionID string) OplogEntry {
	payload, _ := json.Marshal(oplogDeletePayload{
		LastModified: "2025-12-22T12:01:00Z",
	})
	return OplogEntry{
		SiteID:    siteID,
		HLCTS:     hlc,
		OpType:    "delete",
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Payload:   string(payload),
	}
}
