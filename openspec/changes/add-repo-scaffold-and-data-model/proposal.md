# Proposal

## Why

FleetOps has its documentation (the previous change) but no repository structure and no data
contract. Before stage 1 code can land, the project needs an agreed repository layout (control
plane, workers, agent emulator, internal packages, proto contracts, deploy manifests, dashboards,
frontend) and an agreed MongoDB data model (devices, firmware metadata, rollouts, waves, heartbeat
time-series) with required fields, indexing, and telemetry retention. Writing these down now stops
every future change from re-deciding where code lives and how fleet state is stored.

## What Changes

- **Define the repository layout** as real directories: thin `cmd/` entrypoints for the control
  plane, workers, and agent emulator; `internal/` packages (`agent`, `temporal`, `telemetry`);
  `api/proto/` for Protobuf contracts and generated Go; `deploy/` for k3d/compose manifests;
  `dashboards/` for Grafana dashboards; and `web/` as a **git submodule placeholder** pointing at
  the future frontend repository (per user decision — the README map's in-repo `web/` becomes a
  submodule).
- **Add a `Makefile`** (per user decision — not mage or Taskfile) with basic targets: build, vet,
  lint, test, proto generation, and local-stack up/down — each wrapping, not replacing, the
  Definition-of-Done commands from `AGENTS.md`.
- **Define the MongoDB collections**: `devices` (heterogeneous device state), firmware metadata
  (with binaries in GridFS), `rollouts`, `waves`, and `telemetry` (time-series heartbeats). For
  each: required fields and types, the indexing strategy (device identity, region+model, status),
  and a retention policy for telemetry. Ingestion idempotency (`_id = event_id`) and change-stream
  semantics already mandated by `AGENTS.md` are reflected in the data contract.
- **Keep scope stage-consistent**: directories and build targets are structural only — no
  application code, workflows, or services are implemented in this change; empty packages carry
  placeholder documentation, and stage-gated content follows the README delivery plan.

## Capabilities

### New Capabilities

- `repo-scaffold`: The repository layout contract and build tool surface — which directories
  exist and what lives in each, the `web/` submodule placeholder, and the Makefile targets every
  contributor and CI step can rely on.
- `mongo-data-model`: The MongoDB persistence contract — the `devices`, firmware metadata,
  `rollouts`, `waves`, and `telemetry` collections, their required fields, the indexing strategy
  (device identity, region+model, status), and the telemetry retention policy.

### Modified Capabilities

None (no existing specs).

## Impact

- **Files created:** top-level `Makefile`; directories `cmd/`, `internal/`, `api/proto/`, `deploy/`,
  `dashboards/` (with placeholder content where a stage has not landed); `web/` converted to a git
  submodule placeholder plus its registration (`.gitmodules`).
- **Docs touched:** `README.md` repository map is corrected where the user's decisions differ from
  it (`web/` as submodule, new `dashboards/` entry) — a small consistency edit, not a rewrite.
- **Audience:** contributors and AI agents (layout + build contract), and every future storage-
  touching change (data contract).
- **No impact on:** runtime behavior (there is none yet), `go.mod` dependencies, or the staged
  delivery plan. Nothing breaks because nothing runs yet.
