# RFC: Automatic Lifecycle Scheduler

## Summary

Add an opt-in background scheduler that evaluates and executes existing bucket
lifecycle rules without requiring recurring operator-driven maintenance windows.
The scheduler reuses the current lifecycle model and replicated mutation paths,
but it does not invoke the current quiesced `lifecycle-run` blindly. Before the
scheduler is enabled, lifecycle candidate revalidation and mutation must become
atomic and safe while normal object writes continue.

The first scheduler release uses one explicitly designated replication site as
the lifecycle executor. It does not implement a distributed lease or automatic
executor failover. Lifecycle remains logical metadata deletion; segment and
manifest reclamation stays in the existing separately reviewed GC workflows.

## Problem

Seglake currently provides a safe manual workflow:

1. quiesce writes;
2. create and review a lifecycle plan;
3. execute the saved plan;
4. verify results;
5. run physical GC separately.

That workflow is appropriate for controlled operations, but recurring expiration
requires an operator or external automation. Running the current workflow on a
timer would periodically stop writes. Running it without maintenance would leave
races between candidate revalidation and concurrent PUT, DELETE, tagging, MPU,
or lifecycle configuration changes.

## Goals

- Evaluate enabled bucket lifecycle configurations automatically on a bounded
  interval.
- Execute current expiration, noncurrent expiration, and incomplete MPU abort
  safely while normal S3 writes continue.
- Preserve the existing lifecycle rule semantics, normalized config fingerprint,
  object-version behavior, tags, and replication oplog formats.
- Guarantee that a stale candidate cannot remove a newer/current object version
  or act on changed tags, state, timestamps, MPU state, or lifecycle config.
- Ensure only one configured replication site schedules lifecycle mutations in
  the first release.
- Pause cleanly for maintenance and participate in write draining.
- Bound work per cycle and expose redacted run diagnostics.
- Keep physical GC independent and operator-controlled.

## Non-Goals

- Distributed consensus, leader election, or a cross-site lease in the first
  release.
- Automatic failover of the designated executor site.
- Cron expressions, per-bucket schedules, or exact-time deletion guarantees.
- Automatically entering maintenance mode.
- Automatically running segment GC, manifest GC, or MPU GC after lifecycle.
- Replicating scheduler process configuration through the S3 lifecycle API.
- Changing supported S3 lifecycle XML fields or adding transition/storage-class
  behavior.
- Treating lifecycle as a retention, Object Lock, or compliance boundary.
- Persisting full generated plans or candidate object keys in support bundles.

## Safety Requirement: Online Transactional Execution

The scheduler must not use the current read-then-mutate candidate path unless
that path is hardened for online execution. Every candidate action must perform
its final revalidation and mutation in one SQLite write transaction.

For every candidate, the transaction must re-read and verify:

- the bucket lifecycle config fingerprint;
- the matched rule and current wall-clock eligibility;
- the candidate bucket/key/version or upload identity;
- version/MPU state and relevant timestamps;
- current-version identity where applicable;
- object tags used by the matched filter;
- maintenance state and scheduler executor eligibility.

The mutation and its oplog entry must commit in the same transaction. A failed
predicate is a stale skip, not an error.

Action-specific requirements:

- `expire_current`: conditionally verify `objects_current` still points to the
  planned active version. Create at most one delete marker for that current
  version in enabled/suspended buckets, or delete only the unchanged null/current
  version in a disabled bucket.
- `expire_noncurrent`: verify the exact version is still active and is not the
  current version, then mark only that version deleted.
- `abort_mpu`: verify the exact upload remains active with the same bucket/key and
  creation timestamp, then remove its upload and part metadata.

SQLite transaction serialization alone is not the public contract. The metadata
helpers should express conditional intent and verify affected-row/current-pointer
state so tests can prove stale writes are rejected.

The manual `lifecycle-run` should reuse the same transactional candidate
executor. It remains classified as quiesced/unsafe initially; relaxing that
operator-facing requirement is a separate decision after online execution has
enough concurrency coverage.

## Scheduler Ownership

Scheduler configuration is local process configuration. The proposed MVP uses:

- a globally unique existing `-site-id` for each replication site;
- `-lifecycle-scheduler-enabled` to start the worker;
- `-lifecycle-scheduler-executor-site <site-id>` to identify the only site
  allowed to execute lifecycle cycles.

The worker executes cycles only when enabled and the local site ID exactly
matches the configured executor site. Startup fails if the scheduler is enabled
without a non-empty executor site or when the local site ID is the default
`local`. Scheduler enablement requires an explicitly configured deployment-unique
site ID. An enabled node whose site ID does not match is an observable standby:
it reports scheduler state but never plans or mutates. Operators may instead
leave the scheduler disabled on sites that should not participate in executor
failover.

This is an operational single-writer guarantee, not distributed fencing. Two
independent processes configured with the same site ID and executor role are a
deployment error. Existing per-data-directory server locking prevents duplicate
servers on one data directory but does not coordinate separate sites.

Failover in MVP is explicit: disable or stop the old executor, confirm it is no
longer running, then update local process configuration so the new executor's
site ID is selected consistently on participating sites. Transactional stale
checks and idempotent replication remain required because delayed oplog traffic
can still overlap with local metadata changes.

## Scheduling Model

Recommended local flags and environment equivalents:

- `-lifecycle-scheduler-enabled` / `SEGLAKE_LIFECYCLE_SCHEDULER_ENABLED`, default
  `false`;
- `-lifecycle-scheduler-executor-site` /
  `SEGLAKE_LIFECYCLE_SCHEDULER_EXECUTOR_SITE`, required when enabled;
- `-lifecycle-scheduler-interval` /
  `SEGLAKE_LIFECYCLE_SCHEDULER_INTERVAL`, default `1h`;
- `-lifecycle-scheduler-jitter` /
  `SEGLAKE_LIFECYCLE_SCHEDULER_JITTER`, default `5m`;
- `-lifecycle-scheduler-limit` / `SEGLAKE_LIFECYCLE_SCHEDULER_LIMIT`, default
  `10000` candidates per cycle;
- `-lifecycle-scheduler-error-backoff` /
  `SEGLAKE_LIFECYCLE_SCHEDULER_ERROR_BACKOFF`, default `15m`, capped by the
  normal interval.

The interval must be at least `5m`. This prevents accidental tight production
loops while still allowing faster-than-default operation when explicitly needed.

The scheduler waits for an initial jittered delay rather than executing inline
during server startup. Each cycle captures one UTC `as_of`, loads lifecycle
configs, creates candidates in the existing deterministic bucket/key/version
order, and executes at most the configured limit.

No persistent scan cursor is required in MVP. Successfully applied candidates
disappear from later plans, so a limit-truncated workload advances on subsequent
cycles. The run reports truncation; persistent truncation is an operational
signal to increase capacity or reduce the interval.

Lifecycle remains eventually applied. The scheduler does not promise execution
at an exact timestamp, and downtime delays expiration until a later successful
cycle.

A local admin-socket `run-now` operation triggers an immediate cycle for rollout,
testing, or backlog recovery. It is not exposed through the public S3 API. It
uses the same executor-site, maintenance, single-cycle, limit, and error rules as
timer-driven execution. Standby and disabled nodes reject it clearly; if a cycle
is already running, it returns a conflict instead of starting another one.

## Maintenance Coordination

The scheduler must use the same write-admission/inflight mechanism as S3 and
replication mutations:

- no new cycle starts unless maintenance state is `off`;
- each candidate transaction checks maintenance state before mutation;
- entering maintenance prevents new scheduler mutations;
- already admitted scheduler mutations count as inflight writes, so maintenance
  waits for them before reaching `quiesced`;
- a cycle interrupted by maintenance stops after its current admitted action and
  reports the remaining work as deferred, not failed.

The scheduler never changes maintenance state itself. This avoids hidden write
outages and prevents the worker from interfering with backups, rewrap, GC, or
manual lifecycle operations.

Only one lifecycle cycle may run per process. A manual lifecycle-run attempted
while the scheduler is active remains governed by maintenance and therefore
cannot overlap with an online cycle after maintenance reaches `quiesced`.

## Execution and Failure Semantics

Planning failures abort the cycle before mutations. Candidate execution keeps the
existing per-candidate behavior: stale candidates are skipped and unexpected
candidate failures are recorded while later candidates continue.

Systemic failures such as an unavailable metadata store stop the cycle and apply
bounded exponential backoff starting from the configured error-backoff value and
capped by the normal interval. Three consecutive systemically failed cycles mark
the scheduler `unhealthy`; they do not disable it. A later fully successful cycle
resets the consecutive-failure count and returns the scheduler to healthy state.
Candidate-level errors do not by themselves increment the systemic-failure
counter, but remain visible in cycle reports.

The worker must honor context cancellation and server shutdown, must not use
unbounded sleeps, and must not leave background goroutines after shutdown.

The scheduler never retries one candidate in a tight loop. A failed candidate is
reconsidered by a later fresh cycle. No in-memory candidate or plaintext object
data is persisted.

## Metadata and Observability

Reuse `ops_runs` for completed cycle summaries with a distinct mode such as
`lifecycle-scheduler`. Add only the minimal persistent scheduler state needed for
restart diagnostics, for example a singleton/local-site row containing:

- local site ID and configured executor site;
- last cycle start/finish time;
- last successful cycle time;
- last result counters;
- consecutive systemic failure count;
- last redacted error summary;
- whether the last cycle hit the candidate limit.

`/v1/meta/stats` should expose a redacted `lifecycle_scheduler` object with:

- enabled/role (`executor`, `standby`, or `disabled`);
- local and executor site IDs;
- interval, limit, last/next cycle timestamps;
- last candidate/applied/skipped/error counts;
- consecutive failures and limit-truncated state;
- current cycle running state.

Support bundles may include the same scheduler summary and recent aggregate run
results. Include at most the latest 30 scheduler run aggregates from `ops_runs`.
They must not include generated candidate keys, version/upload IDs, full
lifecycle XML, normalized rules, tag values, or plan files. No separate full
candidate history is persisted.

Logs should contain cycle-level counters and redacted error samples. Avoid one
normal log entry per candidate; high-cardinality candidate details belong only in
debug logging and must still contain no object data or tag values.

## Replication Behavior

Scheduler mutations use the existing object delete/delete-marker and `mpu_abort`
oplog operations. No scheduler-specific candidate or plan oplog entry is added.
Peers do not independently replay lifecycle decisions; they apply the resulting
metadata mutations as today.

The executor does not wait synchronously for every peer before committing a
candidate. Operators monitor replication lag separately. Automatic GC remains
decoupled so physical bytes can be retained until replication convergence is
verified.

Scheduler configuration and run state are local and are not replicated. Bucket
lifecycle configurations continue to replicate through the existing lifecycle
config oplog entries.

## Security and Operational Considerations

- Enabling the scheduler grants the process an automatic destructive metadata
  role. Keep it opt-in and restrict who can change process configuration.
- Bucket lifecycle policies remain the authorization boundary for S3 config
  changes; scheduler execution is an internal operator capability.
- A bad lifecycle config can cause large logical deletion batches. Candidate
  limits, diagnostics, delayed physical GC, backups, and replication monitoring
  are the primary safeguards.
- Disabling or changing lifecycle config prevents future actions but does not
  roll back completed mutations.
- Scheduler status exposes bucket-independent counters and site IDs only. Existing
  lifecycle diagnostics may expose bucket names and rule IDs as documented.

## Alternatives Considered

### External cron around current lifecycle-run

This reuses existing tooling but requires recurring global maintenance windows,
careful secret/admin access, plan-file management, and external retry logic. It
remains a valid deployment option but does not provide a good built-in automatic
lifecycle experience.

### Scheduler automatically enters maintenance

This preserves current execution assumptions but creates periodic write outages
and can conflict with operator maintenance. Reject for the built-in scheduler.

### Every replication site executes lifecycle

Transactional checks reduce damage but do not eliminate duplicate decisions,
delete-marker races, extra oplog traffic, or clock-dependent behavior. Reject for
MVP.

### Distributed lease with automatic failover

This gives stronger ownership but requires lease fencing across independently
replicated SQLite stores and careful partition semantics. Defer until operational
experience shows that explicit executor failover is insufficient.

### Scheduler generates plans only

This is safer and useful for alerts, but it does not remove the recurring manual
execution burden. It can be delivered as an intermediate mode while the online
transactional executor is being hardened.

## Implementation Phases

### Phase 1: Online executor hardening

- Move final candidate reads, rule/tag/config checks, mutation, and oplog write
  into one transaction.
- Add conditional metadata helpers for current, noncurrent, and MPU actions.
- Integrate lifecycle mutations with maintenance write admission/inflight state.
- Keep manual `lifecycle-run` quiesced.

### Phase 2: Scheduler worker and configuration

- Add opt-in flags/env validation and designated executor-site checks.
- Add a cancellable interval/jitter/backoff worker to server startup/shutdown.
- Reuse lifecycle planning and the transactional executor with bounded cycles.
- Add the local admin-socket `run-now` trigger with single-cycle exclusion.
- Persist cycle summaries and expose stats/support-bundle diagnostics.

### Phase 3: Hardening and operational validation

- Add concurrency, restart, maintenance, replication, and long-running soak
  coverage.
- Add runbook guidance for executor failover, disabling, lag, truncation, and
  incident response.
- Decide whether manual lifecycle-run can safely become an online mode.

### Future: Distributed ownership

- Evaluate a fenced distributed lease or external coordinator.
- Add automatic executor failover only with clear partition and clock semantics.

## Test Plan

Transactional executor tests:

- concurrent PUT between planning and execution never expires the new current
  version;
- concurrent tag replacement invalidates a tag-filtered candidate;
- concurrent lifecycle config replacement invalidates the old fingerprint;
- concurrent delete/complete/abort makes the candidate stale and idempotent;
- two executors racing the same current candidate create at most one delete
  marker;
- transaction rollback leaves no partial mutation or oplog entry.

Scheduler unit tests with a fake clock:

- disabled and standby workers never plan or mutate;
- executor runs after initial jitter and then at the configured interval;
- candidate limit is deterministic and reported;
- systemic failure applies bounded backoff;
- shutdown cancels the worker without leaked goroutines;
- maintenance prevents cycle start and drains an admitted action;
- resumed operation creates a fresh plan rather than replaying memory state.

Replication/integration tests:

- only the designated site emits lifecycle mutation oplog entries;
- peers converge on delete markers, deleted noncurrent versions, and aborted MPUs;
- scheduler mutation replication transfers no chunks/manifests;
- delayed config replication makes old-fingerprint candidates stale;
- explicit executor failover does not duplicate delete markers.

E2E tests:

- run the server with scheduler enabled, a short test interval, and a fake or
  controlled clock;
- configure lifecycle through S3, create current/noncurrent/tagged/MPU fixtures,
  and verify automatic results through S3 listings;
- enter maintenance during a cycle and verify writes drain and scheduling pauses;
- restart after a successful and failed cycle and verify persisted diagnostics.

Finish each implementation phase with focused tests, `make check`, and the tagged
E2E suite. Add a race-enabled focused scheduler test because ownership and worker
shutdown correctness are core requirements.

## MVP Decisions

- Build an embedded scheduler, not only external cron documentation.
- Require online transactional candidate execution before automatic mutation.
- Keep the scheduler disabled by default.
- Use one explicitly configured executor site in MVP.
- Do not add automatic failover or a distributed lease in MVP.
- Use interval + jitter, not cron syntax.
- Default to hourly evaluation and 10000 candidates per cycle.
- Enforce a minimum scheduler interval of `5m`.
- Reject scheduler enablement with the default `site-id=local`.
- Expose `run-now` through the local admin socket only.
- Continue after candidate errors; use bounded exponential backoff for systemic
  failures and report `unhealthy` after three consecutive failed cycles without
  automatically disabling the scheduler.
- Store aggregate cycle results in `ops_runs`; expose at most the latest 30 in a
  support bundle and never persist full candidate lists.
- Pause for maintenance and participate in write draining; never toggle
  maintenance automatically.
- Keep physical GC manual and separate.
- Keep manual lifecycle-run quiesced until online execution is proven.

## Open Questions

No MVP design questions remain. Changes to executor ownership, automatic failover,
distributed fencing, or automatic GC require a follow-up RFC rather than an
implementation-time default.
