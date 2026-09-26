# Design

## Context

See proposal.md — Why. What shapes the approach:

- The frozen v1 proto already defines `FirmwareDownloadRequest` / `FirmwareDownloadResponse`
  (bounded chunks, offset, end-of-transfer marker, checksum) but only as payloads of the Connect
  stream's oneof, and the stream server answers both `firmware_download` and `update_status`
  envelopes with `Unimplemented` today (`internal/agentserver/server.go`). The proto's own
  evolution rule allows adding fields, messages, and RPCs; nothing may be renumbered.
- `agent.CommandHandler` is the seam where firmware apply lands (`internal/agent/client.go`);
  `cmd/agent` currently wires a logging handler. `AgentService.Report` (unary, idempotency-keyed)
  already reaches the device workflow through `agentserver.DeviceSignaler` → `temporal.Signaler`
  → the `command_result` signal.
- MongoDB schema (`deploy/mongo/init.js`) already declares the `firmware` collection
  (`version`, `checksum`, `gridfs_id`, unique `version` index) and verifies that GridFS has no
  TTL. The Mongo driver is v2, where GridFS is `mongo.GridFSBucket` with
  `OpenUploadStream`/`OpenDownloadStream`/`Delete`.
- The control plane already loads one config file and runs three listeners (probes, gRPC,
  metrics); `-http-addr` is a stub flag reserved for the HTTP gateway.

## Goals / Non-Goals

**Goals:**

- Constant-memory firmware movement in both directions: operator upload → GridFS, GridFS →
  agent download stream → disk. No step holds a whole binary in memory.
- One authority per fact: binaries in GridFS, firmware metadata in `firmware`, device/update
  state in the device workflow.
- Deterministic, reproducible failure injection in the apply stub (seeded), so rollout
  experiments are replayable.

**Non-Goals:**

- Rollout orchestration (`FirmwareWorkflow`, `RolloutWorkflow`, `UpdateDevice` activity) — the
  update handler is exercised by direct command dispatch until that stage.
- `AbortUpdate` cancellation semantics; an abort command stays log-and-ignore.
- Orphan sweeping for GridFS objects left by a crash (the delete-on-failure path covers the
  handled cases; a janitor is out of scope).
- Configurable download directory, chunk size tuning, resumable/ranged downloads.

## Decisions

1. **Delivery is a new server-streaming RPC, reusing the existing messages.** Add
   `rpc DownloadFirmware(FirmwareDownloadRequest) returns (stream FirmwareDownloadResponse)` to
   `AgentService`; both messages are already purpose-built for chunked transfer (offset, eof,
   checksum) and were frozen for exactly this data. *Alternative:* implement the Connect-stream
   `firmware_download` exchange as designed — rejected because the requirement is explicitly a
   server-streaming method, and a dedicated stream avoids correlation bookkeeping and gets
   backpressure from HTTP/2 flow control for free. The frozen Connect-stream exchange stays in
   the contract but unserved; `DownloadFirmware` is the only delivery path.

2. **`total_size` is added to `FirmwareDownloadResponse` (new field number).** The opening
   message carries version, checksum, and total size; without a size the contract's
   `progress_percent` cannot be computed for the downloading phase. This is a legal additive v1
   evolution (new field, new RPC; nothing renumbered), which the delta spec pins down.

3. **Checksums are SHA-256, lowercase hex, computed by the server while streaming.** Upload
   writes the multipart part through `io.MultiWriter(hasher, uploadStream)`, so hashing and
   GridFS storage share one pass. *Alternative:* trust a client-supplied checksum — rejected; the
   control plane computes it (the requirement says so) and the agent independently recomputes it.

4. **Upload API shape and placement.** `POST /api/firmwares` takes `multipart/form-data` with
   part `binary` plus `version` and `models` (comma-separated), answers `201` with the record
   JSON, and is the first route on the `-http-addr` gateway listener (`cmd/controlplane` builds
   a `http.ServeMux` the stage-5 SSE/UI routes will join). *Alternative:* mount on the probe
   server — rejected; probes/metrics are infrastructure surfaces, the operator API is not.

5. **Validate first, store second, clean up on failure.** Order: parse/validate metadata →
   model-compatibility check → version pre-check → stream binary into GridFS → insert the
   metadata document. The existing unique `version` index is the concurrency backstop behind the
   friendly pre-check (a duplicate-key insert maps to `409` and triggers cleanup); any failure
   after the binary landed deletes the GridFS object, so a rejected upload stores nothing.
   Model compatibility is checked against the device registry — `models` must be non-empty,
   duplicate-free, and each model must appear as a registered device's `model` — through a small
   interface consumed by the firmware package and satisfied by `internal/devices`. *Alternatives:*
   a separate model-catalog collection (no such source of truth exists) and a firmware-side
   compatibility matrix (YAGNI).

6. **Reports split by semantics: `UpdateStatusRequest` for progress, `Report` for the terminal
   result.** The contract already draws this line ("fine-grained and frequent, it travels on the
   stream; the terminal command result goes through Report"). The download step reports
   `DOWNLOADING` progress and then either hands over to `APPLYING` or reports `FAILED` with
   detail; the apply step reports `COMPLETED`/`FAILED`; exactly one `Report` concludes the
   `StartUpdate` command, with its idempotency key derived deterministically from the command id
   so any redelivery is a no-op. *Alternative:* conclude the command with `Report` right after
   the download — rejected: the workflow deduplicates results by command id, so the apply
   outcome would have nowhere to report.

7. **Update status lands in the device workflow.** The stream server validates the report,
   answers `accepted: false` (stream survives) for malformed or unenrolled reports, and forwards
   accepted ones over the `DeviceSignaler` seam: new `SignalUpdateStatus` → `update_status`
   signal → entity state field holding the latest phase/progress/detail, persisted by the
   existing snapshot mechanism. No `workflow.GetVersion` guard is needed: the handler acts only
   on receipt of the new signal name, which no pre-existing history contains, so replaying old
   histories generates the same commands as before. *Alternatives:* log-only acknowledgment
   (the report would vanish before the rollout stage can use it) and a `devices`-collection write
   (the workflow is the device-state authority).

8. **Agent download is disk-streamed with incremental hashing.** Chunks go to
   `<firmware_id>.part` in a per-process temp directory via `io.MultiWriter(file, hasher)`; on
   the end marker with matching checksums (stream **and** command) and contiguous offsets the
   file is fsync'd and renamed to `<firmware_id>.bin`; any failure removes the partial file.
   The per-download footprint is one chunk plus the hash state.

9. **Apply stub determinism comes from the emulator's seed.** A `*rand.Rand` seeded from the
   same logged `simulation_seed` decides `Float64() < apply_success_rate` after a context-aware
   sleep of `simulation.apply_delay` — same seed, same pass/fail sequence (the existing
   `agent.Simulation` pattern). Success mutates the simulated device's `current_fw` so later
   heartbeats carry the new version; failure leaves it untouched. `cmd/agent` replaces
   `logHandler` with the update handler wired to three consumed interfaces: firmware fetcher
   (the download RPC), status/result reporters (stream request seam + `Report` stub), and the
   applier.

## Risks / Trade-offs

- [Fleet registry is the model source of truth] → an upload before any device registers is
  refused with `422` ("no registered devices"). Accepted: the demo sequence registers agents
  first, and the error names the real condition.
- [Crash window between GridFS write and metadata insert can orphan a binary] → handled
  failures delete the orphan; a crash is out of scope (no janitor). A later consistency sweep
  can reuse the same delete path.
- [Duplicate-version race between two uploads] → the unique `version` index collapses them to
  one record; the loser maps duplicate-key to `409` and deletes its binary.
- [New `update_status` signal grows entity history] → the existing `ContinueAsNew` threshold
  (`maxSignalsPerRun`) already bounds history; update status is one more signal type under the
  same counter.
- [Additive proto change regenerates `*.pb.go`] → regenerate only via `make proto`; never
  hand-edit generated files.

## Migration Plan

No data migration: no firmware records exist yet. Deploy order: run `make proto`, apply the
updated `deploy/mongo/init.js` schema (firmware validator gains `models`, `size`, `created_at`),
rebuild `cmd/controlplane` and `cmd/agent`. Rollback is redeploying the previous build — the new
field/RPC additions are wire-compatible with the old server and clients.

## Open Questions

- Whether a periodic orphan sweep for GridFS objects should join the existing liveness sweeper —
  deferred until firmware re-uploads make it observable.
- Whether the emulated apply should also simulate a separate reboot phase (`REBOOTING`) —
  deferred; the rollout stage decides what phases its health gates read.
