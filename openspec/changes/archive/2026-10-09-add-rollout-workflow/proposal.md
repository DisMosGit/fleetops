# Proposal

## Why

Phase 6 landed the one number a canary gate decides on — a wave's health over a sliding window of
heartbeat telemetry — and a gRPC way to ask for it, but nothing runs a rollout. No component starts
one, dispatches a wave, holds its health window, promotes to the next wave, or stops the rollout
when health regresses; the aggregation has no writer, so on a live stack every wave reads as an
empty window. Every rule that makes the canary safe currently lives nowhere, and the fleet can only
be updated by hand. This change is the workflow that owns a rollout from start to finish.

## What Changes

- **A rollout workflow owns one rollout from start to finish.** Its input is the firmware to deploy
  and a target selector (region + model); it drives the configured canary sequence, records every
  decision, and concludes in a terminal state. One run chain per rollout id.
- **Waves resolve into a recorded membership before anything is dispatched.** Each wave's share is
  cumulative (1% → 5% → 25% → 100% of the eligible pool), so a wave targets the devices its share
  adds to the shares already promoted; membership is resolved once, written to the wave document,
  and never re-derived. A share that resolves to no device is recorded and skipped rather than
  gated.
- **Waves dispatch firmware updates through the existing device-workflow command seam.** Each
  target device's workflow receives the update command (firmware id, version, checksum) under a
  deterministic command id, so a redelivery is a no-op. A wave is complete when every target has
  accepted its command and the health window has elapsed; per-device result waiting is roadmap item
  38 and stays out of this change.
- **Each wave is held open by a durable health-window timer.** The window is anchored at the wave's
  recorded start on the workflow clock and waited with a Temporal timer, so a worker restart neither
  shortens nor restarts it — an elapsed window is never re-waited, and the remainder resumes after
  a restart.
- **The promotion decision is explicit and total.** A healthy verdict advances to the next wave; an
  unhealthy verdict transitions the rollout into rollback, recorded with the failing wave, ratio,
  sample size, and window; an undecided verdict keeps the gate measuring, sliding its window, until
  the configured decision timeout, after which the wave is treated as unhealthy. Thin evidence can
  never promote a wave, and a silent fleet cannot hold a rollout open forever.
- **A wave configured to require approval does not start without one.** The approval signal lets an
  operator approve moving to the next wave; the rollout reports `awaiting_approval` while it waits,
  holds the approval across worker restarts, and consumes one approval per gated wave.
- **Rollout and wave progress is recorded in the fleet database.** The rollout document carries its
  lifecycle status (running, awaiting approval, rolled back, completed, failed) and the wave
  documents their membership, start, status, and evaluated success rate — idempotent writes, so a
  retried activity converges instead of duplicating.
- **Configuration grows a wave sequence and a decision timeout**: `rollout.waves` (ordered entries
  of `percent` plus `require_approval`) and `rollout.decision_timeout`, both validated.

Explicitly **not** in this change: rollback compensations (downgrade, inventory, notification — the
rollback saga, Phase 8), pause and resume signals (item 37), the per-device update activity that
waits for a reported result (item 38), rollout HTTP endpoints (item 39), and rollout search
attributes (item 40).

## Capabilities

### New Capabilities

- `rollout-workflow`: the rollout's spine — workflow identity and lifecycle, the start input
  (firmware + target selector), the configured wave sequence, target-group resolution and recorded
  wave membership, wave dispatch through the device command seam, recorded rollout/wave progress,
  terminal outcomes, and the state query operators and tests observe the rollout through.
- `rollout-health-gate`: the per-wave promotion decision — the durable health-window timer, the
  health evaluation over the wave's recorded membership, the advance/rollback boundary, the bounded
  undecided policy, and the human approval signal that holds a gated wave until an operator
  approves it.

### Modified Capabilities

- `runtime-config`: a `rollout` section gains the wave sequence (`waves`, entries of `percent` and
  `require_approval`) and `decision_timeout`, with defaults and validation; `cmd/worker` joins
  `cmd/controlplane` in consuming the `rollout` section.
- `mongo-data-model`: the rollout and wave records gain their status vocabularies and write
  semantics — which statuses the rollout workflow records, when a wave's `success_rate` is filled
  in, and that a wave document is addressed by a deterministic id so a retried resolution converges
  on one record.
- `temporal-worker`: the worker hosts the rollout workflow and the rollout activities whose side
  effects it owns (fleet-database reads and writes, device-workflow command signals) beside the
  device workflow it already hosts.

## Impact

- **Code**: `internal/temporal` (the rollout workflow, its state and signal contract, its
  activities, and the starter that begins a rollout), `internal/rollout` (new: the eligible-pool
  query, wave membership resolution, and the rollout/wave record writes), `internal/config` (new
  `rollout` fields and validation), `cmd/worker` (registration and wiring), `deploy/config.yaml`
  (the new settings), `deploy/README.md` (the status vocabularies and wave write path).
  `internal/wavehealth` is consumed as-is through its `Aggregator`; `internal/devices`,
  `internal/agentserver`, and the frozen `fleetops.agent.v1` contract are untouched.
- **Dependencies**: none — Temporal SDK, the Mongo driver, and the existing packages cover this.
- **Contracts**: no proto change. The workflow consumes the existing
  `fleetops.rollout.v1.RolloutService.GetWaveHealth` aggregation through the library and the
  existing device `command_issued` signal; nothing is added to a frozen contract.
- **Storage**: no new collection and no new index — the workflow writes the existing `rollouts` and
  `waves` collections, whose `region`/`model` and `(rollout_id, percent)` indexes already serve the
  queries. Nothing writes wave documents today, so this change fills records instead of migrating
  them.
- **Ops**: the rollout workflow runs on the existing `temporal.task_queue` and adds no listener and
  no broker traffic; rollouts are started programmatically until the HTTP endpoints land (item 39).
