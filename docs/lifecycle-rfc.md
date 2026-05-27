# RFC: S3-Compatible Bucket Lifecycle

## Summary

Add an S3-compatible bucket lifecycle MVP that stores lifecycle configuration in
SQLite, replicates configuration through oplog entries, and executes lifecycle
as a conservative metadata plan/run workflow. Lifecycle decides which objects,
versions, and multipart uploads should be logically removed; existing segment
and manifest GC remains responsible for reclaiming bytes later.

## Source Basis

- AWS S3 `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`,
  and `DeleteBucketLifecycle` APIs: `?lifecycle` subresource and XML shape.
- AWS S3 lifecycle configuration elements: `Rule`, `Status`, `Filter`,
  `Expiration`, `NoncurrentVersionExpiration`, and
  `AbortIncompleteMultipartUpload`.
- AWS S3 lifecycle filters: `Prefix`, object tag filters, and `And` combining
  prefix and tags.
- AWS S3 behavior around versioned buckets, delete markers, and noncurrent
  version expiration.

References checked while preparing this RFC:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html

## Goals

- Support common S3 lifecycle configuration flows through `?lifecycle`.
- Expire current objects, expire noncurrent versions, and abort incomplete
  multipart uploads.
- Support filters by prefix and object tags.
- Keep lifecycle execution safe, observable, and reviewable with plan/run.
- Preserve storage invariants: lifecycle never edits segments or manifests
  directly.
- Replicate lifecycle configuration so peers converge on the same policy.

## Non-Goals

- Storage class transitions, Intelligent-Tiering, Glacier, or restore flows.
- Object size filters.
- Lifecycle as a compliance/retention control. Object Lock remains a separate
  future feature.
- AWS lifecycle edge-case parity beyond the explicit MVP rules below.
- Lifecycle based on custom search/index APIs.
- Hard real-time deletion guarantees.

## API Behavior

### PutBucketLifecycleConfiguration

Support:

```http
PUT /<bucket>?lifecycle
```

Request XML follows AWS's `LifecycleConfiguration` shape:

```xml
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>logs/</Prefix>
        <Tag>
          <Key>class</Key>
          <Value>temporary</Value>
        </Tag>
      </And>
    </Filter>
    <Expiration>
      <Days>30</Days>
    </Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>90</NoncurrentDays>
    </NoncurrentVersionExpiration>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>
```

Validation:

- A configuration contains 1 to 1000 rules.
- `Status` is `Enabled` or `Disabled`.
- `ID` is optional and must be at most 255 UTF-8 bytes when present.
- Each rule must contain at least one supported action.
- Filters may be omitted, `Prefix`, `Tag`, or `And` with at most one prefix and
  one or more tags.
- Tag filters use the same key/value validation as object tags.
- `Expiration` supports either `Days` or `Date`, but not both.
- `NoncurrentVersionExpiration` supports `NoncurrentDays`.
- `AbortIncompleteMultipartUpload` supports `DaysAfterInitiation`.
- Unsupported elements such as `Transition`, `NoncurrentVersionTransition`,
  `ExpiredObjectDeleteMarker`, object-size filters, and storage class settings
  return `501 NotImplemented`.

### GetBucketLifecycleConfiguration

Support:

```http
GET /<bucket>?lifecycle
```

Return the stored XML configuration. Missing bucket returns `NoSuchBucket`.
Missing lifecycle configuration returns
`NoSuchLifecycleConfiguration` with `404`.

### DeleteBucketLifecycle

Support:

```http
DELETE /<bucket>?lifecycle
```

Delete the bucket lifecycle configuration and return `204 No Content`.

## Execution Behavior

Lifecycle execution is implemented as ops tooling first:

- `lifecycle-plan` scans metadata and writes a JSON plan.
- `lifecycle-run` requires a saved plan plus `-lifecycle-force`.

Flags:

- `-lifecycle-bucket <bucket>` optional bucket scope; default all buckets.
- `-lifecycle-plan <path>` required for plan output.
- `-lifecycle-from-plan <path>` required for run input.
- `-lifecycle-as-of <RFC3339>` optional deterministic evaluation time; default
  now.
- `-lifecycle-force` required for run.
- `-lifecycle-limit <n>` optional maximum actions in one plan, default 10000.

Plan JSON stores only metadata needed to revalidate candidates:

- schema version, generated time, as-of time, bucket scope, candidate counts;
- candidate action type;
- bucket, key, version ID or upload ID;
- rule ID;
- object/version/upload timestamp used for eligibility;
- current version marker or upload state for stale-plan detection.

Run revalidates each candidate before mutating metadata:

- skip if rule config changed since planning;
- skip if current object/version/upload state changed;
- skip if object tags no longer match the rule filter;
- skip if candidate is no longer old enough at run time;
- count skipped candidates rather than forcing stale deletes.

Execution actions:

- Current object expiration:
  - versioning enabled or suspended: create a normal delete marker through the
    existing delete path.
  - unversioned/disabled bucket: delete the current null version through the
    existing unversioned delete path.
- Noncurrent version expiration:
  - mark matching noncurrent versions `DELETED` through existing version delete
    semantics.
  - never delete the current version through this action.
- Abort incomplete MPU:
  - delete the upload and its part metadata through existing MPU abort/delete
    paths.

Lifecycle never deletes segment bytes, manifest files, or chunk refs directly.
After lifecycle removes object reachability, existing GC/manifest GC can reclaim
storage.

## Metadata Model

Add bucket lifecycle storage:

```sql
CREATE TABLE bucket_lifecycle (
  bucket TEXT PRIMARY KEY,
  config_xml TEXT NOT NULL,
  config_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

`config_xml` preserves the API round-trip shape. `config_json` stores normalized
rules for evaluation without XML parsing during plan scans.

Recommended metadata helpers:

- `GetBucketLifecycle(ctx, bucket)`
- `SetBucketLifecycle(ctx, bucket, xml, normalizedRules)`
- `DeleteBucketLifecycle(ctx, bucket)`
- `ListBucketLifecycle(ctx)` for lifecycle planning.
- `ListLifecycleObjectCandidates(ctx, bucket, prefix, asOf, limit)` style
  helpers if SQL-side filtering is useful.

## Filtering Semantics

Rule filters match:

- current object key prefix;
- exact tag key/value matches on the target version;
- `And` means all nested prefix/tag predicates must match.

For current object expiration, tag filters are evaluated against the current
version. For noncurrent expiration, tag filters are evaluated against each
candidate noncurrent version. For MPU abort, only prefix filters apply in MVP;
tag filters on MPU abort rules are accepted in config but do not match uploads
because MPU tags are out of scope.

Disabled rules are stored and returned but ignored by planning.

## Replication

Replicate lifecycle configuration changes, not lifecycle execution plans:

- `bucket_lifecycle`
- `bucket_lifecycle_delete`

Payload includes bucket, normalized config, XML, and update time. Peers apply
the config idempotently.

Lifecycle execution itself is local to a node. Since lifecycle mutations use
normal delete/delete-version/abort-MPU metadata paths, their resulting oplog
entries replicate like any other object or MPU metadata change.

## Authorization

Add native policy actions and AWS aliases:

- `GetBucketLifecycleConfiguration`
- `PutBucketLifecycleConfiguration`
- `DeleteBucketLifecycleConfiguration`
- aliases: `s3:GetLifecycleConfiguration`,
  `s3:PutLifecycleConfiguration`, `s3:DeleteLifecycleConfiguration`

`PUT/DELETE ?lifecycle` are write operations and are blocked while maintenance
is active.

Lifecycle ops modes are local/admin operations. They do not use bucket policies
for each candidate; operator access to lifecycle-run is the authorization
boundary.

## Operations and Diagnostics

`lifecycle-plan` report fields should include:

- buckets scanned;
- rules scanned;
- current object expirations;
- noncurrent version expirations;
- multipart uploads to abort;
- skipped rules and warnings;
- estimated logical bytes affected.

`lifecycle-run` report fields should include:

- delete markers created;
- versions deleted;
- multipart uploads aborted;
- candidates skipped after revalidation;
- errors.

Support bundles may include lifecycle config counts and rule IDs, but should not
dump tag values by default. A future explicit diagnostic flag can include full
lifecycle XML when an operator needs it.

## Test Plan

### Unit Tests

- XML parse/render round trip.
- Missing, disabled, and multiple-rule configurations.
- Prefix, tag, and `And` filters.
- Invalid status, duplicate rule IDs, invalid tag filters, malformed XML.
- Unsupported transitions and object-size filters return `501`.

### Metadata Tests

- Migration creates lifecycle storage.
- Set/get/delete bucket lifecycle.
- Oplog set/delete apply idempotently.
- Missing bucket/config errors match S3 handler expectations.

### Planning and Run Tests

- Current object expiration creates delete markers for versioned buckets.
- Current object expiration removes null/current version for unversioned buckets.
- Noncurrent version expiration deletes only noncurrent versions.
- Tag-filtered rules match only versions with matching tags.
- Prefix-filtered rules match only keys under the prefix.
- Abort incomplete MPU removes stale uploads and parts.
- Stale plan skips when config, current version, tags, or upload state changed.
- Lifecycle run never removes segment files or manifest files directly.

### S3 Handler Tests

- `PUT/GET/DELETE ?lifecycle` round trip.
- Missing lifecycle returns `NoSuchLifecycleConfiguration`.
- Missing bucket returns `NoSuchBucket`.
- Policy authorization covers get/put/delete lifecycle actions.
- Virtual-hosted-style routing works.
- Maintenance gating treats PUT/DELETE lifecycle as writes.

### Replication Tests

- Lifecycle config set/delete replicates to another store.
- A lifecycle-created delete marker replicates through existing delete oplog.
- A lifecycle-deleted noncurrent version replicates through existing version
  delete semantics.
- Abort MPU lifecycle behavior converges through existing MPU cleanup semantics
  where metadata is replicated.

### Verification

- Focused: `go test ./internal/meta ./internal/s3 ./internal/ops ./internal/repl`.
- Broader: `make check`.
- E2E smoke with `aws s3api put-bucket-lifecycle-configuration`,
  `get-bucket-lifecycle-configuration`, object writes with tags/prefixes,
  `lifecycle-plan`, `lifecycle-run`, then `list-object-versions`.

## Open Questions

- Should `ExpiredObjectDeleteMarker` be rejected in MVP or accepted as a no-op
  for SDK compatibility?
- Should MPU abort support tag-filtered lifecycle rules only after MPU tagging
  exists, or reject such rules up front?
- Should lifecycle plans include full rule XML hashes for stale-plan detection
  or a normalized config fingerprint only?
- Should lifecycle-run be allowed outside maintenance, or should it require a
  quiesced maintenance state because it can create many delete mutations?
- Should support bundles include full lifecycle XML by default, or only counts
  and rule IDs?

## Assumptions

- Lifecycle is not a retention/security boundary.
- Lifecycle does not rewrite objects or encrypted payloads.
- Lifecycle physical cleanup is delegated to existing GC and manifest GC.
- Bucket lifecycle config affects future lifecycle planning only; it does not
  mutate objects when configured.
- Tags are evaluated from SQLite `object_tags`; if tags are lost through
  manifest-only `rebuild-index`, tag-filtered lifecycle behavior changes until
  tags are restored by another mechanism.
