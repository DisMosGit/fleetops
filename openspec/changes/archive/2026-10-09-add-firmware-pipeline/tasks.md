# Tasks

## 1. Wire contract (`api/proto/agent/v1`)

- [x] 1.1 Add `DownloadFirmware` as a server-streaming `AgentService` RPC returning
      `stream FirmwareDownloadResponse` and add `total_size` to `FirmwareDownloadResponse` under
      a fresh field number (comment-only edits elsewhere), regenerate with `make proto`, and
      verify `go build ./...` passes, the generated diff touches no existing field numbers, and
      `go test ./api/proto/...` passes
- [x] 1.2 Refresh `api/proto/README.md` with the added RPC and field and verify the documented
      regeneration command (`make proto`) reproduces the committed generated files unchanged

## 2. Firmware registry (`internal/firmware`, `deploy/mongo`)

- [x] 2.1 Implement the firmware store: metadata documents in the `firmware` collection
      (`version`, `models`, `checksum`, `size`, `created_at`, `gridfs_id`) plus GridFS storage
      streamed through a SHA-256 hasher (lowercase hex, whole binary never buffered), with the
      delete-the-orphan path when the metadata write fails, and verify with table-driven unit
      tests over a fake database seam plus a `//go:build integration` test on testcontainers
      Mongo covering the GridFS round trip, checksum correctness for multi-chunk payloads, and
      orphan deletion
- [x] 2.2 Implement upload validation (metadata parsing of `version` and comma-separated
      `models`; model compatibility against the device-registry seam — non-empty, duplicate-free,
      every model registered; version conflict detection) and verify table-driven tests cover
      each rejection reason and the sentinel errors that map to 400/409/422
- [x] 2.3 Implement the `POST /api/firmwares` multipart handler (part `binary` + `version` +
      `models`; 201 with id, version, models, checksum, size; 400/409/422 with operator-safe
      bodies; nothing stored on rejection) and verify `httptest` tests cover the happy path, an
      upload larger than the transfer buffer, and every rejection, asserting no store side
      effects on failure (`go test ./internal/firmware/...`)
- [x] 2.4 Update `deploy/mongo/init.js` and `deploy/mongo/verify.sh` so the firmware validator
      and checks cover `models`, `size`, and `created_at` alongside the existing fields, and
      verify the schema integration test and `deploy/mongo/verify.sh` pass against a fresh
      database; document the firmware fields in `deploy/README.md` and `internal/README.md`

## 3. Apply-stub configuration (`internal/config`)

- [x] 3.1 Add `simulation.apply_delay` (default `"1s"`, positive Go duration) and
      `simulation.apply_success_rate` (default `1.0`, in `[0, 1]`) to the shared config with
      fail-fast validation naming the offending field, and verify table-driven tests cover
      defaults, overrides, and both rejection cases (`go test ./internal/config/...`)

## 4. Control-plane delivery and update-status intake (`internal/agentserver`)

- [x] 4.1 Implement `AgentService.DownloadFirmware`: validate the request, open the firmware
      stream through a consumed binary-reader seam, send a metadata message (firmware id,
      version, checksum, total size) first, then bounded chunks with contiguous offsets and the
      end-of-transfer marker last, constant memory whatever the size; verify tests with a fake
      reader cover multi-chunk delivery, offset continuity, `NOT_FOUND` for an unknown firmware,
      `INVALID_ARGUMENT` for incomplete requests, and a mid-stream failure surfacing as a gRPC
      status error (`go test ./internal/agentserver/...`)
- [x] 4.2 Replace the `Unimplemented` update-status path: validate `UpdateStatusRequest`, answer
      `accepted: false` on the correlation id for malformed reports or devices not enrolled on
      the stream (stream survives), and forward accepted reports to the new
      `DeviceSignaler.SignalUpdateStatus` seam; verify tests with a fake signaler assert the
      forwarding, the rejection responses, and that the stream keeps serving
- [x] 4.3 Wire the firmware reader into `cmd/controlplane` and verify `go build ./...` plus the
      existing `cmd/controlplane` tests still pass

## 5. Device workflow update status (`internal/temporal`)

- [x] 5.1 Add the `update_status` signal (firmware id, phase, progress percent, detail) to the
      device workflow: record the latest reported status in entity state under the existing
      meaningful-transition/snapshot rules, keeping command conclusion on `command_result`, and
      verify `TestWorkflowEnvironment` tests cover phase recording, latest-progress-wins, the
      pending command staying pending, and deterministic replay (`go test ./internal/temporal/...`)
- [x] 5.2 Implement `Signaler.SignalUpdateStatus` mapping the wire report onto the signal and
      verify unit tests with the fake signal client assert the signaled payload and the
      signal-with-start behavior

## 6. Agent download client (`internal/agent`)

- [x] 6.1 Implement the firmware downloader: stream chunks in order to `<firmware_id>.part` in
      the configured download directory through an incremental SHA-256 hasher, verify contiguous
      offsets, the end-of-transfer marker, and agreement of the stream checksum with both the
      computed digest and the `StartUpdate` checksum, then fsync and rename to
      `<firmware_id>.bin`; verify tests with a fake chunk stream cover success, checksum
      mismatch, stream/command checksum disagreement, truncation, and that no partial file
      survives failure (`go test ./internal/agent/...`)
- [x] 6.2 Implement download reporting: expose the update-status request seam on the stream
      client (correlation id per report) and report downloading progress followed by the
      outcome; conclude the command with `Report` under an idempotency key derived from the
      command id; verify tests assert the report sequence for success and failure and that a
      redelivered result reuses the same key

## 7. Apply stub and update handler (`internal/agent`, `cmd/agent`)

- [x] 7.1 Implement the deterministic apply stub: a `rand.Source` seeded from the emulator's
      logged seed decides success against `apply_success_rate` after a ctx-aware
      `apply_delay` sleep; verify tests assert same-seed sequences replay identically, rates
      `0.0`/`1.0` are all-fail/all-succeed, and ctx cancellation interrupts the delay promptly
- [x] 7.2 Implement the `StartUpdate` command handler orchestrating download → verify → apply →
      report, adopting the new firmware version on the simulated device (visible in later
      heartbeats) and reporting applying/completed or failed phases plus the single terminal
      report; verify tests cover the happy path, a download failure, and an apply failure,
      asserting the version adoption and the reports each path emits
- [x] 7.3 Wire `cmd/agent`: per-process download directory, the seeded applier under
      `simulation.apply_delay` / `simulation.apply_success_rate`, and the update handler in
      place of `logHandler`; verify `go build ./...`, `go test ./cmd/...`, and that
      `AbortUpdate` remains log-and-ignore

## 8. Integration verification

- [x] 8.1 Land an end-to-end integration test behind `//go:build integration`: upload a firmware
      through the HTTP handler onto testcontainers Mongo, serve it over `DownloadFirmware`, run
      the agent downloader against it, and assert the on-disk checksum, the recorded update
      reports, and the adopted version (`go test -race -tags integration ./...`)
- [x] 8.2 Run the definition of done — `goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...` — and verify all four pass with dependencies unchanged
      (`go mod tidy` leaves `go.mod`/`go.sum` untouched)
