# Proposal

## Why

The fleet registers, heartbeats, and can receive commands, but there is no firmware to deliver:
nothing accepts a firmware binary, nothing stores or validates one, no path moves a binary to an
agent, and the agent's command seam is a logging stub. This change lands the firmware pipeline
(roadmap items 19–23) — upload and validation, chunked delivery, agent download, and a
deterministic apply stub — which the rollout saga will drive once it arrives.

## What Changes

- **Firmware upload endpoint** (`internal/firmware`, wired in `cmd/controlplane`): an HTTP
  `POST /api/firmwares` accepting `multipart/form-data` with a binary part and `version` /
  `models` metadata. The server computes the SHA-256 checksum while the payload streams into
  GridFS (never buffering the whole binary), then records the metadata document in the `firmware`
  collection. A rejected upload stores nothing; a metadata write that fails after the binary
  landed deletes the orphaned GridFS object.
- **Upload validation**: the declared target models must be non-empty and every model must be one
  the device registry knows (a model at least one registered device runs); the version must not
  already exist in `firmware` (409 on conflict, 422 on incompatible models, 400 on malformed
  input). The version uniqueness index is the race backstop behind the pre-check.
- **Server-streaming firmware delivery** (`internal/agentserver`): `AgentService.DownloadFirmware`
  streams a firmware binary to an agent in bounded chunks read from GridFS under constant memory,
  opening with a metadata message (version, checksum, total size) and closing with an explicit
  end-of-transfer marker. The frozen v1 contract evolves additively: one new RPC on `AgentService`
  reusing the existing `FirmwareDownloadRequest` / `FirmwareDownloadResponse` messages, plus one
  new `total_size` field on the response (no field is renumbered or rewritten).
- **Agent firmware download** (`internal/agent`): the client side of the transfer — write chunks
  to a disk file as they arrive, hash incrementally, and verify the assembled binary against both
  the stream checksum and the checksum carried by the `StartUpdate` command (plus contiguous
  offsets). A verified download is renamed to its final path; any failure removes the partial
  file. The agent reports download progress and success/failure back to the control plane as
  `UpdateStatusRequest` phases on the Connect stream, and the terminal command result goes through
  `Report` under an idempotency key derived from the command id.
- **Update status intake** (`internal/agentserver`, `internal/temporal`): the control plane stops
  answering `Unimplemented` to `UpdateStatusRequest` — it validates the report, acknowledges it on
  its correlation id, and forwards it to the device workflow, which records the latest update
  phase and failure detail in its entity state (visible in state snapshots and Temporal UI).
- **Deterministic firmware apply stub** (`internal/agent`): the application step is simulated —
  a configurable delay (context-aware) and a configurable success rate drawn from the emulator's
  seeded random source, so a given seed replays the same pass/fail sequence without real hardware.
  A successful apply bumps the emulated device's `current_fw`, so later heartbeats show the new
  version and rollout health can react.

Explicitly out of scope (they arrive with their own stages): rollout orchestration and
`FirmwareWorkflow` (upload is a plain request path; no Temporal workflow is involved), real
firmware flashing and signing, `AbortUpdate` cancellation semantics, CDN distribution, and the
stage-5 SSE/UI routes on the HTTP gateway.

## Capabilities

### New Capabilities

- `firmware-registry`: The firmware catalog and its upload path — the `POST /api/firmwares`
  endpoint shape and responses, checksum computation while streaming, GridFS binary storage with
  metadata-only records, model-compatibility and version-conflict validation, and the no-orphan
  guarantees on rejection or partial failure.
- `firmware-download-server`: The control-plane side of firmware delivery — the
  `AgentService.DownloadFirmware` server-streaming RPC, its bounded chunking and constant-memory
  streaming from GridFS, and its error surface (unknown firmware → `NOT_FOUND`).
- `agent-firmware-download`: The agent side of the transfer — writing chunks to disk in order,
  incremental checksum verification against the stream and the command, cleanup of partial
  downloads, and reporting download progress and success/failure back to the control plane.
- `agent-firmware-apply`: The agent-side firmware application step — a deterministic stub with a
  simulated apply delay and configurable success rate that adopts the new firmware version on
  success and reports the terminal command result.

### Modified Capabilities

- `agent-control-plane-api`: The "Firmware download exchange" requirement grows the delivery
  shape — the response message gains `total_size`, and the exchange is exposed as the
  server-streaming `AgentService.DownloadFirmware` RPC (an additive v1 evolution).
- `agent-stream-server`: A new requirement — the stream server accepts and acknowledges
  `UpdateStatusRequest` reports for enrolled devices and forwards them to the device workflow
  instead of failing them as unimplemented.
- `device-workflow`: A new requirement — the device workflow accepts an `update_status` signal and
  records the reported firmware update phase and failure detail in its entity state.
- `mongo-data-model`: The "Firmware metadata records" requirement gains the firmware's target
  device models (required, for compatibility validation) and the recorded size and creation time
  of the upload.
- `runtime-config`: The configuration table gains the apply-stub knobs —
  `simulation.apply_delay` and `simulation.apply_success_rate`.

## Impact

- **Files created:** `internal/firmware` (upload handler, metadata + GridFS store, validation,
  with `*_test.go` beside the code and integration coverage behind `//go:build integration`);
  `internal/agent/download.go`, `internal/agent/apply.go`, and the update command handler with
  tests.
- **Files touched:** `api/proto/agent/v1/agent.proto` (one RPC, one field — regenerated via
  `make proto`, never hand-edited), `internal/agentserver` (download streaming, update-status
  intake), `internal/temporal` (update-status signal + state), `internal/devices` (known-models
  query for validation), `internal/config` (apply-stub knobs), `cmd/controlplane` (serve the
  upload route on `-http-addr`), `cmd/agent` (wire the update handler), `deploy/mongo/init.js`
  and `deploy/mongo/verify.sh` (firmware document fields).
- **Dependencies:** none new — the Mongo driver's GridFS API, stdlib `net/http`,
  `mime/multipart`, and `crypto/sha256` cover the whole change.
