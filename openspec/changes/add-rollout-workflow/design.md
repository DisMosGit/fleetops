# Design

## Context

See proposal.md — Why. What shapes the approach:

- `internal/wavehealth` already turns a (rollout, wave) pair into a success ratio, a sample size,
  and a three-state verdict, reading the wave's recorded `device_ids` and `started_at` and clipping
  the window at the wave start. It exposes `Aggregator.Evaluate(ctx, Query, at)` — the evaluation
  moment is the caller's — and two consumed interfaces (`WaveSource`, `SampleStore`) with Mongo
  implementations beside them. Nothing writes wave documents yet.
- The device side of a rollout already exists. `DeviceWorkflow` owns the pending command, accepts a
  `command_issued` signal carrying a command id, kind, firmware id, version, and checksum, and
  dispatches it to the agent; a repeated command id changes nothing. `Signaler.SignalCommandIssued`
  is the producer for that signal, documented as arriving "with the rollout stage".
- `rollouts` (firmware id, status, workflow id, region, model) and `waves` (rollout id, percent,
  status, success rate, device ids, start time) exist with strict validators; the rollout document
  requires the target group as `region` + `model`, which is the selector shape this change takes.
- `cmd/worker` hosts `DeviceWorkflow` and its snapshot activity on `temporal.task_queue`;
  `cmd/controlplane` hosts `dispatch-command`, whose side effect is a live agent stream. Both
  processes have a Temporal client; both poll the same queue.
- The rolling-out device fleet heartbeats every 5 seconds; a five-minute window over a ten-device
  wave is roughly 600 samples and the 100% wave of a 1000-device fleet is roughly 60k.
- There is no HTTP API yet (roadmap item 39), so a rollout is started programmatically, the way
  device signals are today.

## Goals / Non-Goals

**Goals:**

- The rollout's progression is decided in one deterministic place — the workflow — and every wait
  is durable, so a worker restart is a non-event: no re-dispatch, no restarted window, no lost
  approval.
- The gate decides on the same number the operator surface reports, by consuming the existing
  aggregation instead of reimplementing the formula.
- Wave membership is resolved once, recorded before dispatch, and never re-derived: the canary
  denominator cannot move under a decision.
- Thin evidence never promotes a wave, and no wave can hold a rollout open forever.
- The recorded rollout and wave documents are the workflow's projection, and every write is
  idempotent, so a retried activity converges instead of duplicating.

**Non-Goals:**

- Rollback compensations (downgrade, inventory reconciliation, notification) — the saga, Phase 8.
  This change's rollback is the decision and the recorded transition.
- Pause and resume signals (item 37), the per-device update activity that waits for a reported
  result (item 38), rollout HTTP endpoints (item 39), rollout search attributes (item 40).
- Metrics and tracing for rollout progress (stage 4).
- Change-stream-driven eligible-pool recalculation: the pool is queried when a wave starts, and a
  device that enters the pool later can still join a later wave of a running rollout.

## Decisions

1. **The workflow lives in `internal/temporal`; the fleet-database reads and writes live in a new
   `internal/rollout` package.** The workflow, its state, its signal contract, and its activities
   sit beside `DeviceWorkflow`; the Mongo-backed eligible-pool query, membership resolution, and
   rollout/wave record writes sit beside `internal/devices` and `internal/firmware`, which are the
   same kind of package. The interfaces the workflow consumes (`TargetResolver`, `WaveRecorder`,
   `RolloutRecorder`, `FirmwareSource`, `HealthEvaluator`, `DeviceCommander`) are declared in
   `internal/temporal`, where they are consumed, and satisfied by the store, the wave-health
   aggregator, and a signaler — so the workflow is unit-testable with hand-written fakes.
   *Alternatives:* a store inside `internal/devices` (not a device-registry concern) and a separate
   workflow package (there is one Temporal package in this project; a second would be a new
   pattern for no gain).

2. **The wave sequence is configured as entries of `percent` plus `require_approval`, and the
   percentages are cumulative shares of the eligible pool.** Wave *k* targets the devices its share
   adds beyond the shares of waves 1..k−1, so:
   - no device is commanded twice by one rollout;
   - each wave's health measures exactly the devices it just updated, instead of being diluted by
     already-promoted devices that were healthy on the old firmware;
   - the last wave (100%) covers the rest of the pool, so "completed" means the whole target group
     runs the new firmware.
   Membership is resolved by an activity that orders the pool deterministically and excludes the
   device ids recorded on the rollout's earlier waves. *Alternatives:* each wave targeting its own
   share without exclusion (duplicate commands, diluted health) and freezing the whole pool in
   workflow state at start (large state, and a fleet that grows mid-rollout is never covered).

3. **A wave's id is derived from the rollout, the wave's sequence position, and its percentage**
   (`<rollout_id>-w<index>-<percent>`), so a retried resolution or record write addresses the same
   document. The resolution activity treats an existing wave document as the answer: it returns the
   recorded membership and start time instead of resolving again. *Alternatives:* a generated id
   per attempt (a retry would create a second wave record) and an id from rollout + percentage
   (a rollout that ever re-runs a wave size would collide; the deploy docs already note that a
   rollback may re-run a wave size, so a later change can extend the id with an attempt number).

4. **Wave windows are anchored at the recorded wave start, not at the dispatch.** After resolution
   the workflow starts a durable timer for the configured health window and dispatches the wave
   concurrently, then waits for both — the dispatch result and the timer — before evaluating. A
   slow dispatch therefore eats into the window rather than extending it, and the window a gate
   reads is the window the wave document claims. *Alternatives:* starting the timer after dispatch
   (the recorded `started_at` and the real window would disagree, so the aggregation would read
   heartbeats from before the timer) and evaluating immediately after dispatch (no evidence).

5. **Dispatch goes through the existing device command seam, one activity per wave.** The activity
   signals every target device's workflow with a `command_issued` update command whose id is
   derived from the wave and the device (`<wave_id>-<device_id>`), so a retry of the whole wave
   re-signals the same commands and every device deduplicates them. The wave is complete when the
   activity returns. *Alternatives:* sending straight to the agent hub from the worker (bypasses
   the device entity that owns the pending command and its dedup, and would need the control
   plane's hub) and one workflow-scheduled activity per device (thousands of history events and
   futures for no behaviour this change needs; item 38 will restructure dispatch per device when it
   waits for reported results).

6. **One outstanding approval, consumed by the next wave that requires one.** The approval signal
   sets a single boolean "an approval is outstanding" whenever the rollout is running; a wave whose
   entry requires approval starts without waiting when that boolean is set and clears it, otherwise
   the rollout records `awaiting_approval` and waits for the signal. *Alternatives:* ignoring
   approvals that arrive before the gate (an operator who approves early gets silence) and
   per-wave approval tokens (more state for the same behaviour).

7. **An undecided verdict keeps the gate measuring, bounded by `decision_timeout`.** Each
   re-evaluation waits one more health window, so the window slides forward and the wave start
   clipping keeps the whole period; once `decision_timeout` has passed since the wave's start
   without a decided verdict, the wave is treated as unhealthy. *Alternatives:* rolling back on the
   first undecided verdict (a fleet that is merely slow to report reads as a regression) and
   unbounded waiting (a wave whose devices never report holds the rollout open forever).

8. **The health evaluation is a worker activity over the aggregation library, at the workflow's
   decided time.** The workflow passes its own clock reading as the evaluation moment, exactly as
   the snapshot activity does, so a replay evaluates the same window. *Alternatives:* calling the
   `RolloutService.GetWaveHealth` RPC from the workflow (a network hop and a proto dependency
   inside the workflow for no gain — the RPC exists for operators) and evaluating in the workflow
   itself (impossible: it is I/O).

9. **A wave that resolves to no devices is recorded and skipped.** A canary share smaller than one
   device targets nobody; no sample can ever exist for it, so gating it would guarantee an
   undecided timeout and a rollback. The wave is recorded as `skipped` and the rollout advances.
   *Alternatives:* treating it as healthy with an empty window (records a promotion that never
   happened) and aborting the rollout (refuses a legitimate rounding case).

10. **Rollback is the decision and the recorded transition: the rollout is recorded as
    `rolled_back` with the failing wave and its decision, and concludes.** The compensation steps
    are the saga (Phase 8), which will insert them between the decision and the terminal record
    without changing the vocabulary. *Alternatives:* leaving the rollout in a `rolling_back` state
    with nothing to move it and a minimal downgrade now (both pre-empt the saga cycle).

11. **The worker hosts the rollout workflow and all of its activities.** Fleet-database reads and
    writes, device-workflow command signals, and health evaluation run beside the workflow that
    owns them; only the activity whose side effect is a live agent stream (`dispatch-command`)
    stays in the control plane. *Alternatives:* hosting dispatch in the control plane beside the
    signaler (the rollout would stop progressing whenever the control plane is down, for no
    isolation benefit — the signal it sends is Temporal state, not a stream).

12. **Configuration gains `rollout.waves` and `rollout.decision_timeout`; the worker starts
    consuming the `rollout` section.** Defaults stay the demo's canary (1% and 5% un-gated, 25% and
    100% gated, 5-minute windows, 30-minute decision timeout) and validation keeps the sequence
    honest: non-empty, percentages strictly increasing within `(0, 100]`, ending at 100, and a
    decision timeout no shorter than the health window. *Alternatives:* a per-rollout sequence in
    the workflow input (no caller exists until item 39, and the file is this project's single
    configuration surface) and an approval threshold ("approve every wave at or above X%") instead
    of a per-entry flag (the flag says what it means for each wave).

13. **Tests split by what they can prove.** The workflow's progression, gating, approval, and
    restart behaviour are asserted with `TestWorkflowEnvironment` over mocked activities (the repo's
    existing pattern), including time-skipped windows and a signal delivered while the workflow is
    "down"; target resolution, record idempotency, and the config rules are table-driven unit tests
    over fakes and the real value types; the Mongo writes are covered by `//go:build integration`
    tests following the `internal/mongotest` pattern, because a fake cannot prove a validator,
    index, or upsert.

## Risks / Trade-offs

- [Health is measured only over the wave's devices, and silence contributes no samples] → a wave
  whose devices stop heartbeating drifts to undecided and, past `decision_timeout`, rolls back.
  Accepted: liveness has its own detection (`devices.Sweeper`), and the same question is already
  open in the wave-health capability.
- [The eligible pool is queried per wave, not frozen at rollout start] → a device that re-registers
  or changes model mid-rollout can join a later wave. Accepted and bounded: every wave records its
  membership, so no decided wave's denominator moves, and exclusion by recorded membership keeps
  waves disjoint.
- [One activity dispatches a whole wave] → its runtime grows with wave size (750 signals for a
  1000-device fleet), and a retry re-signals devices that already accepted. Mitigated by
  deterministic command ids, which make the re-signal a device-side no-op; item 38 restructures
  dispatch per device.
- [The rollout document is a projection, not the authority] → the workflow's state is authoritative,
  as with `DeviceWorkflow`. The document carries `temporal_wf_id` as the handle, and the state query
  is the way to read the workflow's own view.
- [Rolling back the worker build strands in-flight rollouts] → a redeploy that stops registering the
  rollout workflow leaves running rollouts unpolled. The deploy note is to let in-flight rollouts
  conclude (or terminate them in the Temporal UI) before reverting, since wave state is durable and
  a resumed worker continues from it.
- [The 100% wave's evaluation is the expensive one] → order 10^5 documents scanned per call at demo
  scale, on window expiry rather than per heartbeat. Already accepted by the wave-health change;
  stage-4 tracing will make it visible.

## Migration Plan

No data migration. Nothing wrote `rollouts` or `waves` before this change, and no existing
collection, index, or validator changes shape — the workflow fills the records the schema already
describes. Deploy order: rebuild `cmd/worker` (new registrations and wiring), restart it, optionally
add the `rollout.waves` and `rollout.decision_timeout` settings to `deploy/config.yaml` (the
defaults already describe the demo canary). The control plane needs no change. Rollback: revert the
worker build once in-flight rollouts have concluded, or terminate them in the Temporal UI; the
recorded documents stay as historical records.

## Open Questions

- Should a wave whose target devices fall silent be treated as unhealthy, i.e. should missing
  expected heartbeats count against the ratio? Deferred with the same question in the wave-health
  capability; it needs an expected-cadence source and belongs with offline detection.
- Should the eligible pool exclude offline devices at wave resolution? Deferred: the selector is
  region plus model, and liveness already has its own detection; decide when the operator UI needs
  to express "only reachable devices".
- How a rollout id and its start input reach the workflow from an operator request is item 39's
  HTTP surface; this change's starter derives an id and is what the endpoints will call.
