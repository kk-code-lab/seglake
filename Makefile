.PHONY: build install test test-coverage test-race clean fmt lint check run help

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
	@echo "  check              - Lint, build, and compile tests without running them"
	@echo "  run                - Build and run the application (RUN_ARGS=\"...\")"
	@echo "  clean              - Remove build artifacts"
	@echo "  fmt                - Format code with go fmt"
	@echo "  lint               - Run linter (requires golangci-lint)"
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

clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BIN_PATH)

fmt:
	@echo "Formatting code..."
	@go fmt ./...

lint:
	@echo "Linting code (requires golangci-lint)..."
	@golangci-lint run ./...

check:
	@echo "Linting code (requires golangci-lint)..."
	@golangci-lint run ./...
	@echo "Building $(BINARY_NAME)..."
	@go build $(BUILD_FLAGS) -o $(BIN_PATH) $(MAIN_ENTRY)
	@echo "Type-checking tests (no execution)..."
	@go test -run '^$$' $(INTERNAL_PACKAGES)

run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BIN_PATH) $(RUN_ARGS)
