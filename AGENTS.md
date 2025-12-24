# Repository Guidelines

## Project Structure & Module Organization
- `cmd/seglake/main.go` holds the service entrypoint and wires core subsystems.
- `internal/` contains storage, metadata, API, and ops tooling; tests live alongside packages.
- `build/seglake` is the generated binary; it stays out of version control via `.gitignore`.
- `scripts/make.ps1` is the primary dev entrypoint for build/test/lint targets.

## Build, Test & Development Commands
- Use `./scripts/make.ps1 <target>` for builds/tests; targets include `build`, `run`, `test`, `test-coverage`, `test-race`, `fmt`, `lint`, `check`.
- `./scripts/make.ps1 build` compiles the binary to `build/seglake`; `run` rebuilds and launches it.
- `./scripts/make.ps1 check` runs lint, builds the binary, and compiles tests without executing them.

## Coding Style & Naming Conventions
- Follow idiomatic Go: tabs for indentation, `camelCase` for locals, `PascalCase` for exported identifiers, `_test.go` suffix for tests.
- Keep packages small and cohesive under `internal/`; prefer one domain per package.

## Testing Guidelines
- Prefer table-driven tests and subtests; mirror production file names to keep coverage obvious.
- Keep expensive tests gated; add focused tests when behavior changes.
- For S3 compatibility checks, start the server with `-access-key/-secret-key` and test with awscli/s3cmd against `http://localhost:9000`.
- Example awscli: `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 aws s3 ls --endpoint-url http://localhost:9000 s3://demo`.
- Example s3cmd: `s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 --access_key=test --secret_key=testsecret ls s3://demo`.
- Note: s3cmd may send SigV4 with region `US` (legacy alias for `us-east-1`); the server accepts it for auth but still signs with the raw `US` scope.
- Presigned URL testing can be done via `AuthConfig.Presign` helpers (see `internal/s3/presign.go`).
- Crash-consistency harness: `go test -tags crashharness ./internal/ops -run TestCrashHarness` (uses kill -9 + fsck + rebuild-index).
- GC rewrite: `gc-rewrite` or 2-phase `gc-rewrite-plan` + `gc-rewrite-run` with `-gc-rewrite-plan/-gc-rewrite-from-plan`, throttle via `-gc-rewrite-bps`, pause via `-gc-pause-file`.
- When auth is enabled, `/v1/meta/stats` also requires SigV4; use awscli `s3api` with `--endpoint-url` or a signed curl to fetch it.

## Commit & Pull Request Guidelines
- Use imperative commit messages under 72 characters.
- Before review, ensure binaries are ignored and `build/` is clean.
