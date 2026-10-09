# Proposal

## Why

Stage 1 is "proto freeze": everything downstream — the agent emulator, the gRPC server, the
Temporal entity workflows, the telemetry pipeline — speaks to the control plane over a contract
that does not exist yet. `api/proto/` holds no `.proto` file today, so there is nothing to
compile, nothing to version, and no frozen v1 to evolve from. Defining the agent ↔ control-plane
contract now, together with a repeatable Go code-generation path, unblocks every later stage and
locks the wire format before any code depends on it.

## What Changes

- **Define the v1 agent ↔ control-plane protobuf contract** in `api/proto/` (one proto package for
  this bounded context):
  - A **bidirectional streaming RPC** — `AgentService.Connect` — one long-lived stream per agent
    carrying heartbeats inbound and commands outbound, per the engineering rules.
  - A **unary RPC** — `AgentService.Report` — for reporting command results, accepting and
    validating an idempotency key like every other mutating RPC.
  - **Request and response message pairs** for the three exchanges the fleet needs: device
    registration, firmware download, and update status. Field vocabulary matches the agreed data
    model (`model`, `region`, `current_fw`, `status`; firmware `version`, `checksum`).
- **Set up Protobuf code generation for Go**: generator configuration (proto package /
  `go_package` mapping, `protoc-gen-go` and `protoc-gen-go-grpc` invocation with pinned plugin
  versions) and a repeatable command — the existing `make proto` target — that regenerates both
  client and server stubs from the contract files and fails loudly when the toolchain is missing.
- **Freeze discipline**: v1 is frozen after this change; afterwards contracts evolve by adding
  fields only — never renumbered, never rewritten in place. The generated `*.pb.go` files are
  build artifacts of the contracts and are never hand-edited.

## Capabilities

### New Capabilities

- `agent-control-plane-api`: The v1 wire contract between device agents and the control plane —
  the `AgentService` streaming and unary RPCs, the stream envelope and correlation semantics, and
  the request/response message pairs for device registration, firmware download, and update
  status, including the idempotency-key and backpressure requirements the contract must express.
- `proto-codegen`: The Go stub-generation contract — how `.proto` files map to Go packages, which
  generator versions are pinned and where, and the repeatable regeneration command that
  reproduces client and server stubs from the contract files.

### Modified Capabilities

None (no main specs exist yet; the earlier `repo-scaffold` change already defines the `make proto`
target's shape and is unchanged by this change).

## Impact

- **Files created:** `api/proto/agent/v1/` contract file(s) (service + messages), the generator
  configuration and tool-version pinning for `protoc-gen-go` / `protoc-gen-go-grpc`, and the
  generated Go stubs committed alongside the contracts under `api/proto/`.
- **Files touched:** `Makefile` `proto` target only as needed to pick up the generator
  configuration and pinned plugin versions; `api/proto/README.md` drops its "not implemented yet"
  note once the contracts land.
- **Dependencies:** `google.golang.org/protobuf` and `google.golang.org/grpc` become `go.mod`
  requirements (one-line justification each: generated stubs and the gRPC service registration
  surface). Generator plugins are build-time tools, pinned, not runtime dependencies.
- **No impact on:** runtime behavior (no service is implemented in this change — contracts and
  generated stubs only), the MongoDB/RabbitMQ data contracts, or the staged delivery plan; this
  *is* the first deliverable of stage 1.
