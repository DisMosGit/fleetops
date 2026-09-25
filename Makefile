# FleetOps build tool.
#
# Every target here wraps — never replaces — the commands in the Definition of Done in
# AGENTS.md. AGENTS.md is the source of truth for what "done" means; this Makefile only runs
# those commands in order so contributors and CI share one entry point. When AGENTS.md changes,
# mirror the change here; `make help` shows the documented command for every target.

# Tools installed with `go install` live in GOPATH/bin, which is often not on PATH.
export PATH := $(shell go env GOPATH)/bin:$(PATH)

GO ?= go
PROTO_FILES := $(shell find api/proto -name '*.proto' -print)

.PHONY: help build vet lint test check proto up down

help: ## Show this help: every target with its documented command
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-8s %s\n", $$1, $$2}'

build: ## Compile all binaries: go build -o bin/ ./cmd/...
	@mkdir -p bin
	$(GO) build -o bin/ ./cmd/...

vet: ## Vet the tree: go vet ./...
	$(GO) vet ./...

lint: ## Lint the tree: golangci-lint run
	golangci-lint run

test: ## Run the race-enabled suite: go test -race -count=1 ./...
	$(GO) test -race -count=1 ./...

check: ## Definition of Done: goimports -w . && go vet ./... && golangci-lint run && go test -race -count=1 ./...
	goimports -w .
	$(GO) vet ./...
	golangci-lint run
	$(GO) test -race -count=1 ./...

proto: ## Regenerate Go from api/proto: protoc --go_out=... --go-grpc_out=... api/proto/*.proto
	@command -v protoc >/dev/null || { echo "make proto: protoc not found (install protoc and the Go protoc plugins)" >&2; exit 1; }
	@[ -n "$(PROTO_FILES)" ] || { echo "make proto: no .proto contracts under api/proto yet (delivered at stage 1)" >&2; exit 1; }
	protoc -I api/proto --go_out=paths=source_relative:api/proto --go-grpc_out=paths=source_relative:api/proto $(PROTO_FILES)

up: ## Bring up the local stack: kubectl apply -f deploy/
	@command -v kubectl >/dev/null || { echo "make up: kubectl not found (install the k3d/kubectl toolchain)" >&2; exit 1; }
	kubectl apply -f deploy/

down: ## Tear down the local stack: kubectl delete -f deploy/
	@command -v kubectl >/dev/null || { echo "make down: kubectl not found (install the k3d/kubectl toolchain)" >&2; exit 1; }
	kubectl delete -f deploy/
