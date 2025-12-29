# Repository Guidelines

## Project Structure & Module Organization

- `cmd/seglake/main.go` holds the service entrypoint and wires core subsystems.
- `internal/` contains storage, metadata, API, and ops tooling; tests live alongside packages.
- `Makefile` is the primary dev entrypoint for build/test/lint targets.

## Build, Test & Development Commands

- Use `make <target>` for builds/tests; targets include `build`, `run`, `test`, `test-coverage`, `test-race`, `fmt`, `lint`, `check`.
- `make check` runs lint, builds the binary, and compiles tests without executing them.

## Coding Style & Naming Conventions

- Follow idiomatic Go: tabs for indentation, `camelCase` for locals, `PascalCase` for exported identifiers, `_test.go` suffix for tests.
- Keep packages small and cohesive under `internal/`; prefer one domain per package.

## Testing Guidelines

- Prefer table-driven tests and subtests; mirror production file names to keep coverage obvious.
- Keep expensive tests gated; add focused tests when behavior changes.

## Commit & Pull Request Guidelines

- Use imperative commit messages under 72 characters.
