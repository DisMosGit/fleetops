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

# Generator toolchain pins: the exact protoc plugin and goimports versions `make proto`
# compiles and formats with, so the same contracts produce byte-identical stubs on every
# machine and never fight the goimports pass in the Definition of Done. Bump a pin here and
# nowhere else.
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
GOIMPORTS_VERSION := v0.50.0

.PHONY: help build vet lint test check proto proto-tools up down

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

proto: ## Regenerate Go client and server stubs from api/proto: protoc --go_out=... --go-grpc_out=... && goimports -w api/proto
	@command -v protoc >/dev/null || { echo "make proto: protoc not found (install protoc and its include/ tree)" >&2; exit 1; }
	@command -v protoc-gen-go >/dev/null || { echo "make proto: protoc-gen-go not found (run 'make proto-tools')" >&2; exit 1; }
	@command -v protoc-gen-go-grpc >/dev/null || { echo "make proto: protoc-gen-go-grpc not found (run 'make proto-tools')" >&2; exit 1; }
	@command -v goimports >/dev/null || { echo "make proto: goimports not found (run 'make proto-tools')" >&2; exit 1; }
	@[ -n "$(PROTO_FILES)" ] || { echo "make proto: no .proto contracts under api/proto" >&2; exit 1; }
	protoc -I api/proto --go_out=paths=source_relative:api/proto --go-grpc_out=paths=source_relative:api/proto $(PROTO_FILES)
	goimports -w api/proto

proto-tools: ## Install pinned protoc-gen-go@$(PROTOC_GEN_GO_VERSION) + protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION) + goimports@$(GOIMPORTS_VERSION) via go install
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	$(GO) install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)

up: ## Bring up the local stack: kubectl apply -f deploy/
	@command -v kubectl >/dev/null || { echo "make up: kubectl not found (install the k3d/kubectl toolchain)" >&2; exit 1; }
	kubectl apply -f deploy/

down: ## Tear down the local stack: kubectl delete -f deploy/
	@command -v kubectl >/dev/null || { echo "make down: kubectl not found (install the k3d/kubectl toolchain)" >&2; exit 1; }
	kubectl delete -f deploy/
