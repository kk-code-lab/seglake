# Repository Guidelines

## Project Structure & Module Organization

- `cmd/seglake/main.go` holds the service entrypoint and wires core subsystems.
- `internal/` contains storage, metadata, API, and ops tooling; tests live alongside packages.
- `Makefile` is the primary dev entrypoint for build/test/lint targets.
- Seglake is a minimal S3-compatible object store focused on correctness and durability (append-only segments + manifests + SQLite WAL).

## Build, Test & Development Commands

- Use `make <target>` for builds/tests; targets include `build`, `run`, `test`, `test-coverage`, `test-race`, `test-e2e`, `test-all`, `fmt`, `lint`, `check`.
- `make check` runs lint, builds the binary, and compiles tests without executing them.
- Run local: `make run` or `make build` + `./build/seglake -data-dir ./data -access-key test -secret-key testsecret` (default addr `:9000`).

## Coding Style & Naming Conventions

- Follow idiomatic Go: tabs for indentation, `camelCase` for locals, `PascalCase` for exported identifiers, `_test.go` suffix for tests.
- Keep packages small and cohesive under `internal/`; prefer one domain per package.

## Testing Guidelines

- Prefer table-driven tests and subtests; mirror production file names to keep coverage obvious.
- Keep expensive tests gated; add focused tests when behavior changes.
- Real-HTTP tests must use the `e2e` build tag and run via `make test-e2e` (or `make test-all`); untagged tests should use in-process handlers.
- Smoke (awscli): `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls --endpoint-url http://localhost:9000`.
- Guideposts (non-prescriptive):
  - S3/API behavior changes → `go test ./internal/s3`, and run e2e when externally visible.
  - Storage/format/manifest/chunking changes → `go test ./internal/storage/...`.
  - Metadata schema/migrations → `go test ./internal/meta/...` and update/extend migration tests.
  - Durability/crash/ops flows → consider tagged harnesses as needed.

## Commit & Pull Request Guidelines

- Use imperative commit messages under 72 characters.

## Documentation & Scripts

- Docs map: `docs/spec.md` (API/behavior), `docs/ops.md` (ops/runbook), `docs/optimization.md` (perf), `docs/roadmap.md`, `docs/security/threat-model.md`.
- If changes affect security/ops surface or controls, update `docs/security/threat-model.md`. If unsure, consult it.
- Scripts: `scripts/` (smoke, security checks).

## Baseline Workflow

- Align on scope and compatibility expectations before coding; capture decisions explicitly.
- Scan relevant code/docs/tests to anchor changes in existing patterns.
- Implement in small, reviewable steps; keep changes localized.
- Add/adjust tests alongside behavior changes; prefer unit + targeted e2e when needed.
- Run quick, focused tests during development; finish with `make check` and any required e2e.
- Validate manually (smoke) when the change affects external behavior or APIs; prefer existing scripts in `scripts/` and awscli examples in `docs/ops.md`.
- Update docs/specs/roadmap if behavior or surface area changes.
- For e2e coverage: ensure tests use `//go:build e2e` and `TestS3E2E*` naming so `make test-e2e` picks them up.
- For format/storage/metadata changes (segments/manifests/SQLite schema): update `docs/spec.md` and note compatibility/migration expectations.
- For auth/policy/limits/ops endpoint changes: review/update `docs/security/threat-model.md` and `docs/ops.md`.
- Remember `make check` requires `golangci-lint` installed.
- Note and fix flakes/regressions; avoid leaving background goroutines or temp files behind.

## Workflow Variants

- Spec-first / RFC: start with a short RFC (problem, goals, non-goals, API/behavior, migration); code after approval.
- Test-first: write reproduction + assertions first, then implement; prefer for bugfixes/regressions.
- Spike → harden: quick prototype to de-risk, then implement cleanly; discard spike artifacts.
- Ops/Security-first: assess threat-model/ops impact and update docs before coding.
