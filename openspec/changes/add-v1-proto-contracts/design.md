# Design

## Context

See proposal.md — Why. Current state: `api/proto/` holds a README and no `.proto` file; the
`Makefile` already exposes a `proto` target that invokes `protoc` with `--go_out` /
`--go-grpc_out` (source-relative, output beside the contracts) and fails when `protoc` or the
contracts are missing; `go.mod` carries no gRPC/protobuf dependencies yet. The MongoDB data-model
change has already fixed the field vocabulary the wire contract must echo: device identity
(`model`, `region`, `current_fw`, `status`), firmware (`version`, `checksum`), and heartbeat
telemetry (`ts`, `device_id`, `cpu`, `mem`, `health`, keyed by an event id). Engineering rules fix
the RPC surface: `AgentService.Connect` (bidi) and `AgentService.Report` (unary, idempotency key),
one proto package per bounded context, v1 frozen with additive evolution only.

## Goals / Non-Goals

**Goals:**
- A concrete, compilable v1 contract: message and field-level design for every exchange named in
  the proposal, consistent with the data-model vocabulary.
- A code-generation setup that is reproducible on a fresh machine with one documented command.
- Design-level guardrails that keep v1 evolvable (envelope + correlation, additive fields).

**Non-Goals:**
- Implementing the gRPC server, the agent emulator's stream client, or any Temporal workflow —
  stage 1 delivers contracts and stubs only.
- Server-side dedupe storage for idempotency keys, retry/backoff policy, metrics, or interceptors
  — runtime concerns for the stages that implement the transport.
- Firmware distribution concerns that are explicitly out of scope (signing, CDN).

## Decisions

### D1: One `AgentService` in one proto package `fleetops.agent.v1`
Both RPCs named by the engineering rules (`Connect`, `Report`) belong to the agent ↔ control-plane
bounded context, so they share one proto package and one service: `api/proto/agent/v1/agent.proto`,
package `fleetops.agent.v1`, `go_package = "github.com/DisMosGit/fleetops/api/proto/agent/v1;agentv1"`.
*Alternative:* two services (a "streaming service" and a "reporting service") — rejected: the rule
"never reuse messages across services" would force duplicated message types for the same domain,
and the engineering rules already name both RPCs on `AgentService`. Later bounded contexts (e.g. an
operator/API-gateway surface) get their own `api/proto/<context>/v1/` package when their stage
lands.

### D2: Stream envelopes with correlation ids
`Connect` carries `stream AgentEnvelope` → `stream ControlEnvelope`. Each envelope is a `oneof`
payload plus a `correlation_id` (responses echo the id of their request; commands carry a
`command_id` instead).

```
AgentEnvelope      oneof: RegisterDeviceRequest | Heartbeat | FirmwareDownloadRequest | UpdateStatusRequest
ControlEnvelope    oneof: RegisterDeviceResponse | FirmwareDownloadResponse | UpdateStatusResponse | Command
```

*Alternative:* bare oneof without an envelope — rejected: concurrent exchanges on one long-lived
stream cannot be matched; a request/response pair could be answered out of order. *Alternative:*
one unary RPC per exchange — rejected: the request scopes exactly two RPCs, and per-exchange RPCs
would abandon the "one stream per agent" rule.

### D3: The three exchanges are agent-initiated request/response pairs on the stream
Device registration, firmware download, and update status are requests the agent sends (register
me; give me firmware X; here is my update progress), each answered by the control plane on the
same correlation id:

| Pair | Request carries | Response carries |
|------|-----------------|------------------|
| `RegisterDeviceRequest`/`Response` | `device_id`, `model`, `region`, `current_fw` | acceptance decision, resulting device `status` |
| `FirmwareDownloadRequest`/`Response` | `device_id`, `firmware_id` | `firmware_id`, `version`, `checksum`, binary as bounded chunks (`bytes chunk`, `offset`, end-of-transfer marker) |
| `UpdateStatusRequest`/`Response` | `device_id`, `firmware_id`, phase enum, `progress_percent`, failure detail | acknowledgment |

The v1 phase enum covers at least: downloading, applying, rebooting, completed, failed, rolled
back. *Alternative:* control-plane-pushed firmware (CP streams the binary as a command) — rejected:
pull with a checksum lets the agent verify what it assembles and keeps commands small enough to
respect stream backpressure; push would also make resume-after-reconnect awkward.

### D4: Commands go down the stream; terminal results go up through `Report`
`Command` is a `oneof` payload in v1 with two typed commands: `StartUpdate` (firmware id, version,
checksum — triggers the download/apply cycle) and `AbortUpdate` (reason — covers canary rollback's
downgrade path, which is just a `StartUpdate` for the older version). Fine-grained progress rides
`UpdateStatusRequest` on the stream; the **terminal** result of a command is reported via
`AgentService.Report` with an idempotency key + `command_id`, matching the rule that every mutating
RPC accepts an idempotency key and the `DeviceWorkflow` signal surface (`heartbeat`,
`command_result`). `ReportRequest`/`ReportResponse` state acceptance; a repeated idempotency key
returns acceptance with no second effect; an empty key is `INVALID_ARGUMENT`.
*Alternative:* results over the stream too — rejected: results must survive stream loss (they
complete workflow activities), and the unary RPC is where idempotency is mandated.

### D5: Heartbeat carries the telemetry event shape
`Heartbeat` carries `event_id` (idempotency key for ingestion), `device_id`, `current_fw`,
`status`, `ts`, and `cpu`/`mem`/`health` — exactly the fields of one `telemetry` document plus the
device-state fields a `devices` update needs. Aligning the wire shape with the storage shape means
the ingest path is a mapping, not a translation.

### D6: Chunked firmware transfer with a contract-level bound
The firmware binary travels as `bytes` chunks with an `offset` and an explicit end-of-transfer
marker; the `checksum` always arrives before the chunks. The contract mandates bounded chunk size
(implementation picks the exact size later) so one download can never force unbounded buffering —
the contract-level half of the backpressure rule; bounded send queues are runtime work at stage 3.

### D7: `protoc` + pinned Go plugins, configuration kept in the Makefile tool block
Generator configuration = the existing `proto` target's `protoc` invocation (source-relative
output beside contracts, `--go_out` + `--go-grpc_out` so both client **and** server stubs are
generated) plus exact pinned versions of `protoc-gen-go` and `protoc-gen-go-grpc` declared in one
Makefile tool block, with a `proto-tools` target that installs exactly those versions into
GOPATH/bin (already on the Makefile's PATH). The `proto` target keeps its loud failure when
`protoc` is missing and gains the same check for the plugins. *Alternative:* `buf` with
`buf.gen.yaml` — rejected for now: the Makefile and `api/proto/README.md` already commit to
`protoc` + the two Go plugins, buf is a heavier new tool dependency for a two-file contract
surface, and pinned plugin versions give the reproducibility that matters (identical generated
output). If the contract surface outgrows `protoc` argument lists, buf is a contained swap behind
the same `make proto` entry point.

### D8: Generated Go is committed, and only regeneration changes it
`*.pb.go` / `*_grpc.pb.go` land beside their contracts under `api/proto/` and are committed, so
`go build` and `go test` never require `protoc`. Regeneration is idempotent (same contracts +
pinned plugins ⇒ byte-identical output), which makes "run `make proto`, expect no diff" a review
check. Hand edits are overwritten by the next regeneration and forbidden by the engineering rules.

### D9: Dependencies justified one line each
`google.golang.org/protobuf` (runtime for generated messages) and `google.golang.org/grpc` (client
and server stub surface) enter `go.mod`; generator plugins are pinned build-time tools only and
are never imported by binaries.

## Risks / Trade-offs

- [A frozen v1 encodes a guess that a later stage wants differently] → envelopes + correlation ids
  and additive-only evolution leave room to add fields, messages, and RPCs; nothing is renumbered.
  Chunked transfer avoids a later transport swap; command set can grow by adding `oneof` arms.
- [Generator version skew across machines churns generated diffs] → one pinned tool block +
  `proto-tools` installs exactly those versions; idempotent-regeneration check surfaces skew
  immediately.
- [Firmware chunks on the shared stream compete with heartbeats] → bounded chunk size is
  contractual; ordering and queue bounds are resolved at stage 3 where the runtime backpressure
  policy lives. If it proves insufficient, an additive RPC can carry bulk transfer later without
  breaking v1.
- [Two visible responsibilities on one stream (requests/responses and commands)] → correlation ids
  and `command_id` keep them disjoint; the spec tests the pairing explicitly.
- [Idempotency semantics live in the contract but dedupe storage does not exist yet] → acceptable:
  the contract defines what a key means; the Report handler at its implementing stage owns
  dedupe. No premature storage design here.

## Migration Plan

Contracts-only change, nothing depends on the wire yet. Land the `.proto`, the pinned tool block,
and the committed generated stubs together; run `make proto` twice (second run must be a clean
tree), then the Definition of Done (`make check`). Update `api/proto/README.md` (drop "not
implemented yet") and the README stage table only if the stage-1 row's wording needs it. Rollback
is a plain revert: no runtime code compiles against the stubs yet.

## Open Questions

None that would change the specs, the approach, or the task breakdown. (Exact pinned plugin
versions and the firmware chunk size are implementation-time picks constrained by D7 and D6.)
