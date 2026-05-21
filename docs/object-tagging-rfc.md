# RFC: S3-Compatible Object Tagging

## Summary

Add S3-compatible object tagging for object versions. Tags are stored in SQLite,
replicated through oplog entries, and exposed through the standard S3 tagging
subresource plus the `x-amz-tagging` object creation header. Tags do not affect
object manifests, chunks, ETags, encryption metadata, or object bytes.

## Source Basis

- AWS S3 Object Tagging user guide: object tag limits, unique keys, key/value
  length limits, and basic behavior.
- AWS S3 `PutObjectTagging`, `GetObjectTagging`, and `DeleteObjectTagging`
  APIs: `?tagging` subresource behavior and `versionId` support.
- AWS S3 `PutObject` API: `x-amz-tagging` request header encoded as URL query
  parameters.
- AWS S3 `GetObject` API: `x-amz-tagging-count` response header.
- AWS S3 `CopyObject` API: `x-amz-tagging-directive` behavior.

## Goals

- Support object tags through the public S3-compatible API.
- Store tags per object version, including explicit `versionId` targeting.
- Preserve existing object data, manifest, ETag, encryption, GC, and replication
  behavior.
- Replicate tag changes so peers converge without object data transfer.
- Keep SDK/awscli compatibility for the common object tagging flows.

## Non-Goals

- Bucket tagging.
- Lifecycle, billing, analytics, inventory, or ABAC behavior based on tags.
- AWS-style tag condition keys.
- Batch Operations.
- Tag indexing/search APIs.
- Directory bucket behavior.

## API Behavior

### PutObjectTagging

Support:

```http
PUT /<bucket>/<key>?tagging
PUT /<bucket>/<key>?tagging&versionId=<version-id>
```

Request body:

```xml
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet>
    <Tag>
      <Key>project</Key>
      <Value>alpha</Value>
    </Tag>
  </TagSet>
</Tagging>
```

Behavior:

- Without `versionId`, update the current live object version.
- With `versionId`, update that exact object version.
- Missing bucket returns `NoSuchBucket`.
- Missing object/version returns `NoSuchKey` or the existing version-missing
  error style used by Seglake S3 handlers.
- On success, return `200 OK`.
- If versioning is enabled and a target version is returned by existing handler
  conventions, include `x-amz-version-id`.

### GetObjectTagging

Support:

```http
GET /<bucket>/<key>?tagging
GET /<bucket>/<key>?tagging&versionId=<version-id>
```

Response body uses the same `Tagging/TagSet/Tag/Key/Value` XML shape. Objects
with no tags return an empty `TagSet`.

### DeleteObjectTagging

Support:

```http
DELETE /<bucket>/<key>?tagging
DELETE /<bucket>/<key>?tagging&versionId=<version-id>
```

Behavior:

- Delete the complete tag set for the targeted version.
- Return `204 No Content` on success.
- Do not create a new object version.
- Do not delete object bytes or object metadata.

### PutObject Header Tags

Support `x-amz-tagging` on object creation:

```http
x-amz-tagging: project=alpha&env=dev
```

The header is parsed as URL query parameters. Tags are stored on the newly
created object version in the same commit path as the object metadata.

Invalid tags fail the request before the object becomes visible.

### CopyObject Tags

Support `x-amz-tagging-directive`:

- Omitted or `COPY`: copy tags from the selected source object version.
- `REPLACE`: parse destination tags from `x-amz-tagging`; if omitted, the
  destination receives an empty tag set.

Unsupported directive values return `400 InvalidArgument`.

Tag handling is independent of encryption handling. CopyObject still reads the
source through the normal read path and writes the destination according to the
effective destination encryption settings.

### GetObject and HeadObject Tag Count

When the caller is authorized to read object tags, `GET` and `HEAD` may return:

```http
x-amz-tagging-count: <n>
```

MVP recommendation: return the count when tags exist and the request is
authorized for the object read. If adding separate tag-read authorization to
GET/HEAD complicates the first pass, omit the header initially and document the
gap before enabling it.

## Validation Rules

Use S3-compatible limits:

- At most 10 tags per object version.
- Tag keys must be unique within a tag set.
- Key length: 1 to 128 Unicode characters.
- Value length: 0 to 256 Unicode characters.
- Keys and values are case-sensitive.

Invalid XML, duplicate keys, too many tags, empty keys, and over-limit keys or
values return `400 InvalidTag` or `400 InvalidArgument`, matching the nearest
existing Seglake error style.

MVP can accept UTF-8 strings and enforce length by Go rune count. A later pass
can tighten this to AWS's UTF-16 character-count semantics if SDK compatibility
tests show it matters.

## Metadata Model

Add SQLite storage for per-version tags. Suggested schema:

```sql
CREATE TABLE object_tags (
  version_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY(version_id, key),
  FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
);
```

Recommended meta methods:

- `GetObjectTags(ctx, versionID) ([]ObjectTag, error)`
- `SetObjectTags(ctx, versionID, tags []ObjectTag) error`
- `DeleteObjectTags(ctx, versionID) error`
- helper for object creation transactions so `x-amz-tagging` commits atomically
  with the new version.

Tags should not be embedded in manifests. They are metadata, not object layout.

## Replication

Replicate tag changes through new oplog entries:

- `object_tags_set`
- `object_tags_delete`

Each entry should identify bucket, key, version ID, and enough compact tag
payload to apply the change idempotently. Since the tag set is at most 10 pairs,
embedding the tag set as redacted/non-secret metadata in the oplog is acceptable.

Object creation with `x-amz-tagging` can either:

- include tags in the existing put oplog metadata if the oplog payload already
  has a suitable extension point; or
- emit a follow-up `object_tags_set` entry in the same metadata transaction.

The preferred first implementation is whichever matches current oplog patterns
with fewer special cases. Peers must converge without fetching object chunks.

## Authorization

Add native/S3 policy actions:

- `GetObjectTagging`
- `PutObjectTagging`
- `DeleteObjectTagging`
- aliases: `s3:GetObjectTagging`, `s3:PutObjectTagging`,
  `s3:DeleteObjectTagging`
- version aliases: `s3:GetObjectVersionTagging`,
  `s3:PutObjectVersionTagging`, `s3:DeleteObjectVersionTagging`

MVP does not add AWS tag condition keys. Existing bucket/prefix resource
matching should apply.

## Operations and Diagnostics

- Support bundle may include tag table counts but should not dump tag values by
  default.
- `fsck` and `scrub` do not need to inspect tags.
- `rebuild-index` cannot reconstruct tags from manifests because tags are not in
  manifests. Document this limitation if rebuild-index currently recreates only
  manifest-derived metadata.
- Object deletes and GC do not require segment changes. SQLite cascade or
  explicit cleanup should remove tag rows for deleted version rows when those
  rows are actually removed.

## Tests

### Unit Tests

- XML parse/render round trip.
- Header parse/render round trip for `x-amz-tagging`.
- Duplicate key rejected.
- More than 10 tags rejected.
- Empty key rejected.
- Key/value length limits enforced.
- URL escaping and spaces preserved.

### Metadata Tests

- Migration creates tag storage.
- Set/get/delete tags by version ID.
- Tags are version-scoped.
- Object creation with tags commits tags atomically with the new version.
- Replication apply set/delete is idempotent.

### S3 Handler Tests

- `PUT ?tagging`, `GET ?tagging`, `DELETE ?tagging`.
- `versionId` targets the requested version.
- `PUT Object` with `x-amz-tagging` stores tags on the new version.
- Invalid XML/header tags fail before object visibility.
- CopyObject default `COPY` preserves source tags.
- CopyObject `REPLACE` uses destination `x-amz-tagging`.
- Policy authorization for get/put/delete tagging actions.

### Replication Tests

- Object with creation header tags replicates to peer.
- `PUT ?tagging` on one peer replicates to another without chunk transfer.
- `DELETE ?tagging` replicates and clears tags.
- Version-specific tag update converges on the same version.

### E2E Smoke

Use `aws s3api`:

```sh
aws s3api put-object --bucket demo --key a.txt --body a.txt \
  --tagging 'project=alpha&env=dev' \
  --endpoint-url http://localhost:9000

aws s3api get-object-tagging --bucket demo --key a.txt \
  --endpoint-url http://localhost:9000

aws s3api put-object-tagging --bucket demo --key a.txt \
  --tagging 'TagSet=[{Key=project,Value=beta}]' \
  --endpoint-url http://localhost:9000

aws s3api delete-object-tagging --bucket demo --key a.txt \
  --endpoint-url http://localhost:9000
```

## Open Questions

Resolved decisions:

- `GET/HEAD Object` should return `x-amz-tagging-count` in MVP when the caller
  has tag-read authorization. If the policy engine cannot express that cleanly
  in the first implementation pass, omit the header rather than leaking tag
  presence across an authorization boundary.
- Object creation with `x-amz-tagging` should emit a separate
  `object_tags_set` oplog entry in the same metadata transaction as the object
  put. This keeps tag replication behavior uniform across creation-time tags
  and later `PUT ?tagging` updates.
- Manifest-only `rebuild-index` may lose tags in MVP because tags are SQLite
  metadata and are not embedded in manifests. Document this as an operational
  limitation. A sidecar tag export/import workflow can be added later if tags
  become critical recovery metadata.
- Tag length validation should use AWS-like UTF-16 character counts from the
  start instead of Go rune counts.
