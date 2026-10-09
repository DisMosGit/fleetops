# Tasks

## 1. v1 contract file

- [x] 1.1 Write `api/proto/agent/v1/agent.proto` with package `fleetops.agent.v1` and
      `go_package "github.com/DisMosGit/fleetops/api/proto/agent/v1;agentv1"`: `AgentService` with
      the `Connect` bidi stream (envelopes + correlation ids per design D2) and the unary `Report`,
      and every message from D3–D6 (registration, firmware download, update status, heartbeat,
      command, report pairs) with explicit field numbers starting at 1; verify it compiles
      cleanly with `protoc -I api/proto --descriptor_set_out=/dev/null api/proto/agent/v1/agent.proto`
- [x] 1.2 Comment every service, RPC, message, and enum value in the contract with why-level
      semantics (what the exchange does, how correlation and idempotency keys are used); verify
      every declared symbol above field level carries a comment
- [x] 1.3 Update `api/proto/README.md` to describe the landed contract (the two RPCs, the message
      pairs, the heartbeat/command vocabulary) and the freeze discipline (v1 evolves by adding
      fields only); verify the README no longer claims no `.proto` contract exists and matches
      what `agent.proto` declares

## 2. Generator configuration and tooling

- [x] 2.1 Add a pinned generator tool block to the `Makefile` declaring exact versions of
      `protoc-gen-go` and `protoc-gen-go-grpc` in one place, plus a `proto-tools` target that
      installs exactly those versions into GOPATH/bin; verify `make proto-tools` succeeds and the
      installed `protoc-gen-go --version` / `protoc-gen-go-grpc --version` match the pins
- [x] 2.2 Extend the `proto` target to fail loudly (non-zero, message naming the missing tool)
      when `protoc` or either pinned plugin is absent, and to generate with source-relative output
      beside the contracts for both `--go_out` and `--go-grpc_out`; verify the failure path by
      running the target without the plugins on `PATH`, and the success path by running
      `make proto` with the toolchain present
- [x] 2.3 Document the regeneration workflow in `api/proto/README.md` (`make proto-tools` then
      `make proto`) and keep the `make proto` help text in sync; verify the documented commands
      run exactly as written on the current tree

## 3. Generated stubs and Go dependencies

- [x] 3.1 Add `google.golang.org/protobuf` and `google.golang.org/grpc` to `go.mod` (one-line
      justification each in the PR description), run `go mod tidy`, then run `make proto` and
      commit the generated files beside the contract; verify `go build ./...` passes and
      `api/proto/agent/v1/` contains generated client **and** server interfaces for `AgentService`
- [x] 3.2 Add a hand-written test beside the generated package that round-trips each envelope
      payload and the report messages through protobuf marshal/unmarshal (table-driven, external
      `_test` package, `cmp.Diff` for assertions); verify `go test -race -count=1 ./api/...` passes
- [x] 3.3 Verify regeneration is idempotent: run `make proto` twice and confirm the second run
      leaves `git status` clean for `api/proto/`, and that hand edits to a generated file are
      overwritten by regeneration

## 4. Integration checks

- [x] 4.1 Run the full Definition of Done — `make check` (goimports, `go vet`, `golangci-lint`,
      `go test -race -count=1 ./...`) — and verify all four commands pass
- [x] 4.2 Verify the fresh-machine path end to end: from a clean tree, `make proto-tools` then
      `make proto` reproduces the committed stubs with no diff, closing out the reproducibility
      requirement in `specs/proto-codegen/spec.md`
