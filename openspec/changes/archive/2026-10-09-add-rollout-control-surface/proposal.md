# Proposal

## Why

The rollout workflow drives a canary sequence end to end, but nobody can operate it. A rollout can
only be started programmatically from Go, its state can only be read through the Temporal CLI, an
operator who sees something wrong has no way to stop it, and no run is findable in the Temporal UI
by anything but its id. And a wave counts as complete as soon as its commands were *accepted* by
device workflows: nothing confirms that a device actually applied the firmware, so "the wave was
dispatched" rests on an assumption, and a device that silently fails its update stays invisible
unless its heartbeats happen to drag the health ratio down.

This change gives the rollout the surface its operator needs — start it, watch it, hold it, stop
waiting on it, find it in the Temporal UI — and makes every wave's completion rest on reported
per-device results instead of accepted signals.

## What Changes

- **Every wave update is confirmed per device.** A new `update-device` activity commands exactly one
  device through the device workflow's command seam — under the deterministic per-wave command id,
  so a redelivery is still a no-op for the device — and then waits for that device's workflow to
  record the command's outcome, up to the configured `rollout.result_timeout`. It reports
  `succeeded`, `failed` (carrying the device's own detail), or `unreported` (the device never
  reported before the deadline). A wave schedules one such activity per target device and is
  complete once every one of them has settled; which devices failed or never reported is recorded
  on the wave document, and the configured health ratio remains the promotion decision. The
  wave-level `dispatch-wave-update` activity is removed — dispatch is per device now.
- **A running rollout can be paused and resumed.** The `pause_rollout` signal stops the rollout from
  starting another wave; the `resume_rollout` signal lets it continue from exactly where it stopped.
  A wave already in flight is still driven to its recorded decision and a regression still rolls the
  rollout back, because a pause holds promotion and never safety. Recorded wave outcomes, promoted
  waves, and a banked approval all survive a pause and survive a worker restart, and a paused
  rollout reports the new `paused` lifecycle status.
- **The rollout gains an operator HTTP surface.** `POST /api/rollouts` starts a rollout (firmware id
  plus region-plus-model selector, with the caller-supplied rollout id as the idempotency key),
  `GET /api/rollouts/{id}` answers with the workflow's own state view — status, each wave's outcome,
  what the rollout is waiting for, and why it ended — and `POST /api/rollouts/{id}/approve`,
  `/pause`, and `/resume` deliver the three operator signals. The routes join the existing gateway
  listener beside the firmware upload API, and the control plane becomes the process that starts
  rollouts.
- **Rollout runs are filterable in the Temporal UI.** Each rollout run carries the custom search
  attributes `RolloutFirmware` (the deployed firmware's version), `RolloutRegion` (the target
  selector's region), and `RolloutStatus` (Keyword), kept current as the rollout loads its firmware
  and moves through its statuses, so the UI can list runs by firmware version, target region, and
  current status, alone or combined. The worker's startup bootstrap registers them beside the
  device attributes.

Explicitly **not** in this change: rollback compensations (downgrade, inventory, notification — the
saga, Phase 8), the SSE live-status stream and the React UI (stage 5), a rollout *list* endpoint,
firmware or device endpoints, metrics and tracing for rollout progress (stage 4), and the question
of whether per-device update failures should decide a wave by themselves (the health gate stays the
promotion decision).

## Capabilities

### New Capabilities

- `device-update`: updating one device and confirming it — the per-device update activity, its
  command identity, the wait for the device's reported result within the configured result timeout,
  the three outcomes it reports, its behaviour under retries and superseded commands, and the
  concluded command a device entity exposes so a waiting caller can read its result.
- `rollout-control`: holding and continuing a running rollout — the pause and resume signals, what a
  paused rollout does and does not do, how a resume continues without losing recorded progress, and
  the `paused` lifecycle status.
- `rollout-api`: the operator HTTP surface over a rollout — starting one, reading its state, and
  sending approve, pause, and resume commands, with request and response shapes, status codes, and
  the idempotency rule.
- `rollout-search-attributes`: the custom Temporal search attributes rollout runs carry, the values
  they mirror, when they are upserted, and their registration.

### Modified Capabilities

- `rollout-workflow`: a wave's dispatch becomes one update activity per target device whose reported
  outcomes the wave collects before it is gated; the wave records which devices failed or never
  reported; a command that cannot be delivered still fails the wave.
- `device-workflow`: the entity records the outcome of the command it concluded — the command, its
  outcome, and the failure detail — and exposes it through the state query, which is what a waiting
  caller reads.
- `mongo-data-model`: the `rollouts` status vocabulary gains `paused`, and wave documents gain the
  failed and unreported device sets.
- `runtime-config`: `rollout.result_timeout` — the longest a wave waits for a device's reported
  update result — with its default and validation.
- `temporal-worker`: the worker registers `update-device` in place of the removed wave-level
  dispatch activity, and the namespace search-attribute bootstrap registers the rollout attributes
  beside the device ones.

## Impact

- **Code**: `internal/temporal` (the device-state read seam and the `update-device` activity, the
  per-device dispatch and outcome collection in `RolloutWorkflow`, the pause/resume signals and
  state, the rollout search attributes, and the Temporal-client adapters the HTTP surface needs),
  `internal/rolloutapi` (new: the rollout HTTP handler), `internal/rollout` (the wave record's
  device-outcome sets), `internal/config` (`rollout.result_timeout`), `cmd/worker` (registration and
  wiring), `cmd/controlplane` (the gateway routes, the rollout start path, and the client adapters
  behind them), `deploy/config.yaml`, `deploy/mongo/init.js` + `deploy/mongo/verify.sh`,
  `deploy/README.md`, `cmd/README.md`.
- **Contracts**: three new HTTP routes on the existing gateway listener. No proto change — the
  frozen `fleetops.agent.v1` and `fleetops.rollout.v1` contracts are untouched; the operator surface
  reads the workflow state query and sends the workflow's signals.
- **Storage**: no new collection, no new index. `rollouts` accepts one more status value, and `waves`
  documents gain two device-id arrays. Nothing else writes those documents, so no migration is
  needed — but a wave document written before this change lacks the new arrays, so the validator
  treats them as optional while the store always writes them.
- **Behaviour**: a wave now takes at least as long as its devices take to report, bounded by
  `rollout.result_timeout` (default five minutes, the same as the health window, so a healthy wave's
  length is unchanged in the normal case). A wave whose commands cannot be delivered still fails the
  rollout; a device that fails or never reports is recorded but does not by itself decide the wave.
- **Compatibility**: this changes `RolloutWorkflow`'s logic in place and `DeviceWorkflow`'s carried
  state, so in-flight rollout runs and device run chains must be drained or terminated before the
  new worker build polls — the same local-stack migration the earlier workflow changes documented
  (see design — Migration Plan, which also records why no `workflow.GetVersion` branch is kept).
