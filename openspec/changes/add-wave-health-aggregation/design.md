# Design

## Context

See proposal.md — Why. What shapes the approach:

- Heartbeats are already durable: the ingest writer batches accepted heartbeats into the
  `telemetry` collection (`_id` = event id, `ts`, `meta.device_id/region/model`, top-level `cpu`,
  `mem`, `health`), and the unique `_id` makes a redelivery a no-op. Nothing else reads that
  collection yet.
- `rollouts` and `waves` collections exist with strict validators, but nothing writes them:
  `RolloutWorkflow` is Phase 7. `waves` currently records `rollout_id`, `percent`, `status`, and
  `success_rate` — no membership and no start time.
- `fleetops.agent.v1` is frozen (stage 1) and describes the agent transport only. The control
  plane runs one gRPC listener, registered in `cmd/controlplane` through
  `agentserver.ServerOptions`, and all configuration is one validated YAML file
  (`internal/config`, `deploy/config.yaml`).
- Indexes this query can rely on: `telemetry` `{meta.device_id: 1, ts: -1}`, `waves`
  `{rollout_id: 1, percent: 1}`, and the unique `_id` on both.
- The emulator heartbeats every 5 seconds (`agent.DefaultPeriod`), so a five-minute window over a
  ten-device wave is roughly 600 samples; the 100% wave of a 1000-device fleet is roughly 60k.
- Health is a 0..1 score whose emulator bands are healthy `0.85`–`1.0` and degraded `0.2`–`0.5`;
  `alerting.health_threshold` (0.6) already draws the degraded line for alerts.

## Goals / Non-Goals

**Goals:**

- One definition of "wave is healthy" shared by the workflow gate, the operator surface, and the
  tests — the formula, the window, and the decision boundary live in exactly one place.
- The formula is unit-testable without a database: both storage dependencies are narrow interfaces
  the tests fake, so the empty/partial/transient/boundary cases are fast, hermetic tests.
- The query path is read-only. It observes `rollouts`, `waves`, and `telemetry`; the durable
  `success_rate` on the wave record stays the rollout workflow's write (Phase 7).

**Non-Goals:**

- Rollout orchestration: `RolloutWorkflow`, wave progression, the durable health-window timer,
  regression decisions, and the rollback saga are Phases 7–8. This change supplies the number they
  gate on, not the gate.
- Writing wave membership or `success_rate` — this change defines and reads the fields; their
  producer arrives with the rollout workflow.
- Counting silence as failure. A device that stops heartbeating emits no samples; liveness stays
  with the existing offline sweeper.
- Per-request windows (the UI's 5m/15m/1h/24h views). The window is a wave-gate property; a
  range-query read model for the UI is a separate, later capability.
- Metrics and tracing for the query (stage 4), and `docs/api.md` — planned since stage 1 but still
  unwritten, and not this change's to create.

## Decisions

1. **A new proto package, `fleetops.rollout.v1`, with `RolloutService.GetWaveHealth`.** An operator
   health query is a different bounded context from the agent transport, and messages are never
   shared across services; `fleetops.agent.v1` is frozen and must not grow an operator RPC.
   *Alternatives:* an RPC on `AgentService` (puts an operator query on the device transport and
   mixes contexts) and the HTTP gateway (the requirement is a gRPC method, and the gateway is the
   UI's surface). The new package starts its own v1 and freezes under the same additive rule.

2. **Wave membership is recorded on the wave document (`device_ids`, `started_at`), not re-derived
   per query.** Re-resolving region+model at query time evaluates whoever matches *now*, so a
   device that re-registered or changed model mid-rollout silently changes the denominator, and
   the `percent` share is not reproducible. Recording the dispatched set makes the ratio auditable
   and gives an empty window an honest cause. *Alternatives:* derive from the target group plus
   `percent` (nondeterministic membership, cannot tell a dead wave from a wrong denominator) and
   pass device ids in the request (the contract is "given a rollout and a wave"; every caller would
   resolve membership its own way).

3. **A sample is a stored heartbeat, and it succeeds when `health >= sample_health_threshold`.**
   Matches the project's stated formula (`success_heartbeats / total`), keeps the ratio
   explainable in one sentence, and inherits telemetry's `_id` dedup so a redelivered heartbeat
   cannot inflate the denominator. *Alternatives:* one vote per device (newest sample in the
   window) — sample-weighted evidence is what the roadmap asks for, and a per-device view is
   derivable later; `Report` outcomes — that is the update path, and it does not exist yet.

4. **The effective window is `[max(at − health_window, wave.started_at), at]` and is reported
   back.** Without clipping at the wave start, a wave's first minutes are dominated by the same
   devices' pre-update heartbeats — they were healthy on the old firmware — so a regressing wave
   would look healthiest exactly when the gate must be strictest. Reporting the bounds lets a
   caller tell a clipped window from a full one. *Alternatives:* the naive sliding window
   (rejected above) and refusing to answer until the wave is older than the window (delays the
   gate without adding evidence; the sample-count gate is the real evidence rule).

5. **Three-state verdict with a minimum-sample gate, inclusive at the boundary.** Below
   `min_samples` the verdict is *undecided*; at or above it, `ratio >= min_success_ratio` is
   healthy and below it is unhealthy. A thin window can never promote a wave, and *undecided* is
   distinguishable from *unhealthy* so Phase 7's gate can wait instead of rolling back. The
   boundary is inclusive (`>=`), matching the convention already used for health scores in
   `internal/telemetry/alerting.go`.

6. **The aggregation lives in `internal/wavehealth`, behind two consumed interfaces.**
   `WaveSource` resolves a rollout and wave into membership plus start time (returning the
   package's sentinel not-found error), and `SampleStore` returns the window's total and
   successful sample counts for a device set. Mongo-backed implementations sit beside the
   aggregation; unit tests hand-write small fakes per AGENTS.md (stdlib + `cmp.Diff`, no testify).
   The gRPC service is a thin adapter in the same package: it validates the request, calls the
   aggregation, and maps the sentinel to `NOT_FOUND`, everything else to `INTERNAL` with an
   operator-safe message. *Alternatives:* putting it in `internal/telemetry` (it is a rollout
   gate, not an ingestion concern) and a separate `internal/rollouts` store package now (nothing
   else reads rollouts yet — YAGNI; the wave source is one query).

7. **Counting is one aggregation-pipeline round trip.** The Mongo sample source matches
   `meta.device_id` `$in` the wave's membership plus the `ts` range, then `$group`s into `total`
   and `successful` (`$sum` over a `$cond` on the threshold), served by the existing
   `{meta.device_id: 1, ts: -1}` index and transferring two numbers. Empty membership short-circuits
   without touching the database. *Alternatives:* two `CountDocuments` calls (two round trips that
   can disagree under concurrent writes) and fetching samples to count in Go (binds the aggregation
   to data volume — tens of thousands of documents per call at fleet scale).

8. **A `rollout` config section: `health_window` 5m, `sample_health_threshold` 0.6,
   `min_success_ratio` 0.95, `min_samples` 10.** The per-sample threshold is deliberately its own
   knob even though its default matches `alerting.health_threshold`: alert sensitivity and the
   promote/rollback boundary are different decisions, and sharing one value would let an alerting
   tweak silently move the gate. Validation mirrors the existing rules (positive duration, positive
   integer, floats in `[0, 1]`), and `deploy/config.yaml` documents the section.

9. **Tests are table-driven, hermetic, and split by what they can prove.** Fakes drive the four
   required families — empty windows (no samples, no membership, not started), partial windows
   (clipped at the wave start, below the sample minimum), transient failures (a dip above and below
   the gate, and recovery once the dip ages out), and the boundary (a ratio exactly at the minimum
   is healthy; one more failing sample flips it) — plus the gRPC adapter's status mapping. The
   `$group` pipeline and the validator changes cannot be proven by fakes, so they are covered by
   `//go:build integration` tests following the repo's `testcontainers` pattern
   (`internal/mongotest`, `deploy/mongo/init.js` applied to a real MongoDB).

## Risks / Trade-offs

- [Silence is not failure] → a device that stops heartbeating contributes no samples, so a wave can
  look healthy while its devices are dead. Accepted: liveness has its own detection
  (`devices.Sweeper` and `liveness.offline_threshold`), and Phase 7 can weigh it. Revisited as an
  open question.
- [Sampling skew] → a chatty device contributes more samples than a quiet one; the ratio is
  sample-weighted by design. `min_samples` guards thin evidence, and a device-weighted variant can
  reuse the same query shape if a wave ever needs one.
- [`device_ids` is required by the validator but nothing writes wave documents yet] → waves can
  only be created by hand until Phase 7, and `init.js` skips existing collections, so a running
  local stack keeps the old `waves` validator until it is recreated. Same precedent as the firmware
  change's schema update; the deploy docs state it.
- [A live stack answers *undecided* for every wave] → wave membership and start time do not exist
  until the rollout workflow writes them. Accepted: the contract lands before its producer, and the
  populated path is exercised by unit and integration tests.
- [Query cost grows with wave size] → the 100% wave is the worst case (order 10^4–10^5 documents
  scanned per call, two numbers returned). The gate calls it on window expiry rather than per
  heartbeat, and stage-4 tracing will make the cost visible.

## Migration Plan

No data migration: nothing writes wave documents yet and no rollout depends on this query. Deploy
order: `make proto` (adds `api/proto/rollout/v1`), apply the updated `deploy/mongo/init.js` (the
`waves` validator gains `device_ids` and `started_at`, `verify.sh` asserts them), rebuild
`cmd/controlplane`. Rollback is redeploying the previous build — the new package is additive, no
existing contract, collection, or index changed shape, and an unserved service is invisible to
existing clients.

## Open Questions

- Should a wave whose target devices fall silent be treated as unhealthy — that is, should missing
  expected heartbeats count against the ratio? Deferred: it needs an expected-cadence source
  (emulator period vs configuration) and belongs with offline detection, not with the ratio.
- Should the evaluated ratio be persisted onto the wave's `success_rate` by this path or by the
  rollout workflow when its window expires? Deferred to Phase 7; this change keeps the query
  read-only, and the wave-store write is trivial to add to whichever owner wins.
