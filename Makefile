.PHONY: build install test test-coverage test-race test-e2e test-all clean fmt lint lint-astgrep test-astgrep check run help

BINARY_NAME := seglake
BUILD_DIR := build
BIN_PATH := $(BUILD_DIR)/$(BINARY_NAME)
MAIN_ENTRY := ./cmd/seglake
INTERNAL_PACKAGES := ./internal/...

GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_FLAGS := -ldflags "-X github.com/kk-code-lab/seglake/internal/app.BuildCommit=$(GIT_COMMIT) -X github.com/kk-code-lab/seglake/internal/app.Version=dev"

help:
	@echo "seglake - S3-compatible object store"
	@echo ""
	@echo "Available targets:"
	@echo "  build              - Build the binary to build/seglake"
	@echo "  install            - Install the binary to GOBIN (or GOPATH/bin)"
	@echo "  test               - Run all tests"
	@echo "  test-coverage      - Run tests with coverage report"
	@echo "  test-race          - Run tests with race detector"
	@echo "  test-e2e           - Run e2e tests only"
	@echo "  test-all           - Run unit + e2e tests"
	@echo "  check              - Lint, build, and compile tests without running them"
	@echo "  run                - Build and run the application (RUN_ARGS=\"...\")"
	@echo "  clean              - Remove build artifacts"
	@echo "  fmt                - Format code with go fmt"
	@echo "  lint               - Run linter (requires golangci-lint)"
	@echo "  lint-astgrep       - Run ast-grep rules"
	@echo "  test-astgrep       - Run ast-grep rule tests"
	@echo "  help               - Show this help message"

build:
	@echo "Building $(BINARY_NAME)..."
	@go build $(BUILD_FLAGS) -o $(BIN_PATH) $(MAIN_ENTRY)

install:
	@echo "Installing $(BINARY_NAME) to GOBIN (or GOPATH/bin)..."
	@go install $(BUILD_FLAGS) $(MAIN_ENTRY)

test:
	@echo "Running tests..."
	@go test $(INTERNAL_PACKAGES)

test-coverage:
	@echo "Running tests with coverage..."
	@go test $(INTERNAL_PACKAGES) -cover

test-race:
	@echo "Running tests with race detector..."
	@go test $(INTERNAL_PACKAGES) -race

test-e2e:
	@echo "Running e2e tests..."
	@go test $(INTERNAL_PACKAGES) -tags e2e -run '^TestS3E2E'

test-all:
	@echo "Running unit tests..."
	@go test $(INTERNAL_PACKAGES)
	@echo "Running e2e tests..."
	@go test $(INTERNAL_PACKAGES) -tags e2e -run '^TestS3E2E'

clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BIN_PATH)

fmt:
	@echo "Formatting code..."
	@go fmt ./...

lint:
	@echo "Linting code (requires golangci-lint)..."
	@golangci-lint run ./...
	@$(MAKE) lint-astgrep

lint-astgrep:
	@echo "Linting code with ast-grep..."
	@command -v ast-grep >/dev/null 2>&1 || (echo "ast-grep not found; install it to run lint-astgrep." && exit 1)
	@ast-grep scan

test-astgrep:
	@echo "Testing ast-grep rules..."
	@command -v ast-grep >/dev/null 2>&1 || (echo "ast-grep not found; install it to run test-astgrep." && exit 1)
	@ast-grep test

check:
	@echo "Linting code (requires golangci-lint)..."
	@golangci-lint run ./...
	@$(MAKE) lint-astgrep
	@echo "Building $(BINARY_NAME)..."
	@go build $(BUILD_FLAGS) -o $(BIN_PATH) $(MAIN_ENTRY)
	@echo "Type-checking tests (no execution)..."
	@go test -run '^$$' $(INTERNAL_PACKAGES)
	@$(MAKE) test-astgrep

run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BIN_PATH) $(RUN_ARGS)
