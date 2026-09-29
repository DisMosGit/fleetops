# Proposal

## Why

Heartbeats already reach durable storage and the `waves` collection exists, but nothing turns that
data into the decision a wave gate needs: how much of a wave is actually healthy right now, and on
how much evidence. Without one shared evaluation, the rollout workflow (Phase 7) would reimplement
the formula, the operator UI would invent its own window, and a rollback would depend on whichever
version of "success rate" happened to run — the one number a canary rollout must not disagree with
itself about. This change lands that single evaluation and the gRPC method that exposes it, before
the workflow that consumes it.

## What Changes

- **Wave records carry the two facts a wave's health needs.** A wave gains the device set it
  targeted (`device_ids`) and the time its health window opened (`started_at`), so a wave is
  evaluated over exactly the devices it targeted and the period since it started — not over
  whatever the rollout's region+model happens to select at query time, and not over heartbeats
  from before the wave opened.
- **One aggregation component evaluates a (rollout, wave) pair over a sliding window.** It resolves
  the wave through its rollout, reads the window's heartbeat samples for that wave's devices, and
  reports the success ratio (share of samples whose reported health is at or above the per-sample
  health threshold), the sample size, the effective window it read, and a verdict.
- **Partial and empty windows are first-class, not edge cases.** The effective window never reaches
  back before the wave started; a window whose sample count is below the configured minimum is
  *undecided* rather than healthy, so thin evidence cannot promote a wave; an empty window reports
  sample size zero and no ratio.
- **A new gRPC contract exposes the aggregation.** `fleetops.rollout.v1.RolloutService.GetWaveHealth`
  returns the success ratio and sample size for a given rollout and wave, plus the verdict, window,
  and the thresholds the verdict was computed against. Unknown rollouts and waves map to
  `NOT_FOUND`, malformed requests to `INVALID_ARGUMENT`, with operator-safe messages.
- **Configuration grows a `rollout` section** — sliding window width, per-sample health threshold,
  minimum success ratio for a healthy verdict, and minimum sample count — validated like every
  other field and documented in `deploy/config.yaml`.
- **Unit tests cover the four ways this formula goes wrong**: an empty window, a partial window
  (wave younger than the window, or fewer samples than the minimum), transient failures that must
  not read as a regression, and the exact boundary where the ratio flips from healthy to unhealthy.

## Capabilities

### New Capabilities

- `wave-health-aggregation`: the sliding-window computation for one rollout and wave — wave
  scoping (target devices plus the wave's start), sample counting against the per-sample health
  threshold, the success ratio and sample size, empty/partial window semantics, the minimum-sample
  gate, and the healthy/unhealthy boundary.
- `rollout-health-api`: the `fleetops.rollout.v1` contract and its serving behavior — the
  `RolloutService.GetWaveHealth` RPC, its request/response fields (ratio, sample size, verdict,
  window, thresholds), validation and status-code mapping, and the additive-evolution rules the
  new package freezes under.

### Modified Capabilities

- `mongo-data-model`: the wave record gains its target device set (`device_ids`) and the time its
  health window opened (`started_at`), enforced by the `waves` validator and documented in the
  collection reference.
- `runtime-config`: a new `rollout` section (`health_window`, `sample_health_threshold`,
  `min_success_ratio`, `min_samples`) with defaults and validation.

## Impact

- **Code**: `internal/wavehealth` (new — the aggregation over a telemetry window, a Mongo-backed
  wave/rollout source, and the thin gRPC service; both storage seams are consumed interfaces so
  the component unit-tests without a database), `api/proto/rollout/v1/{rollout.proto,rollout.pb.go,rollout_grpc.pb.go}`
  (new, regenerated with `make proto`), `cmd/controlplane` (register `RolloutService` on the
  existing gRPC listener and wire the new config), `internal/config` + `deploy/config.yaml` (new
  section), `deploy/mongo/init.js`, `deploy/mongo/verify.sh`, and `deploy/README.md` (the wave
  fields). `internal/telemetry`, `internal/agentserver`, and the frozen `fleetops.agent.v1`
  contract are untouched.
- **Dependencies**: none — the Mongo driver v2 and gRPC already in `go.mod` cover this.
- **Contracts**: one new proto package, `fleetops.rollout.v1`, starting its own v1; `fleetops.agent.v1`
  is not extended, because an operator health query is a different bounded context from the agent
  transport and messages are never shared across services.
- **Storage**: the `waves` validator and collection reference gain `device_ids` and `started_at`;
  no new collection, no new index (the existing `idx_rollout_percent` already serves the lookup),
  no data migration — nothing writes wave documents yet.
- **Ops**: the aggregation is a read-only query over `rollouts`, `waves`, and `telemetry`; it adds
  no listener, no broker traffic, and no background job. A live stack returns empty-window reports
  until the rollout workflow starts writing wave membership, which is Phase 7 scope.
