# Repository Guidelines

## Project Structure & Module Organization

- `cmd/seglake/main.go` holds the service entrypoint and wires core subsystems.
- `internal/` contains storage, metadata, API, and ops tooling; tests live alongside packages.
- `Makefile` is the primary dev entrypoint for build/test/lint targets.
- Seglake is a minimal S3-compatible object store focused on correctness and durability (append-only segments + manifests + SQLite WAL).

## Build, Test & Development Commands

- Use `make <target>` for builds/tests; targets include `build`, `run`, `test`, `test-coverage`, `test-race`, `test-e2e`, `fmt`, `lint`, `check`.
- `make check` runs lint, builds the binary, and compiles tests without executing them.
- Run local: `make run` or `make build` + `./build/seglake -data-dir ./data -access-key test -secret-key testsecret` (default addr `:9000`).

## Coding Style & Naming Conventions

- Follow idiomatic Go: tabs for indentation, `camelCase` for locals, `PascalCase` for exported identifiers, `_test.go` suffix for tests.
- Keep packages small and cohesive under `internal/`; prefer one domain per package.

## Testing Guidelines

- Prefer table-driven tests and subtests; mirror production file names to keep coverage obvious.
- Keep expensive tests gated; add focused tests when behavior changes.
- Smoke (awscli): `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls --endpoint-url http://localhost:9000`.

## Commit & Pull Request Guidelines

- Use imperative commit messages under 72 characters.

## Documentation & Scripts

- Docs map: `docs/spec.md` (API/behavior), `docs/ops.md` (ops/runbook), `docs/optimization.md` (perf), `docs/roadmap.md`, `docs/security/threat-model.md`.
- If changes affect security/ops surface or controls, update `docs/security/threat-model.md`. If unsure, consult it.
- Scripts: `scripts/` (smoke, security checks).
