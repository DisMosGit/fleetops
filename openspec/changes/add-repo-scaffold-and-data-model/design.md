# Design

## Context

Greenfield Go module `github.com/DisMosGit/fleetops` with `go.mod` and docs only — no `cmd/`,
`internal/`, `api/`, `deploy/`, `dashboards/`, or build tooling yet. The idea source
(`.nda/idea_description.md`, private) fixes several things this design must respect: the ER
sketch in §6 (DEVICE/TELEMETRY/FIRMWARE/ROLLOUT/WAVE), the file paths named in §7
(`internal/temporal/rollout_workflow.go`, `internal/agent/stream.go`,
`internal/telemetry/consumer.go`), telemetry ingest idempotent via `_id = event_id`, and open
question #2 — heartbeat retention of all devices in a time-series collection: unbounded or
7 days. Engineering rules live in `AGENTS.md`; delivery is staged 1–5 per `README.md`. Two user
decisions shape the layout: the build tool is a **Makefile**, and `web/` is a **git submodule
placeholder** (the README map's in-repo `web/` is corrected accordingly).

## Goals / Non-Goals

**Goals:**
- Land the directory skeleton the staged plan fills in, with honest placeholders for
  not-yet-implemented stages.
- One obvious command surface (`make`) wrapping the AGENTS.md Definition of Done.
- A concrete MongoDB contract — field names matching the idea source's ER sketch — that later
  stages code against without renegotiation.

**Non-Goals:**
- Any application code: no handlers, workflows, activities, consumers, or proto messages.
- Schema migration tooling: there is nothing to migrate; migrations arrive only if a released
  schema must change.
- Collection-creation code (`internal/telemetry` etc. is stage 3): the deploy stack may create
  collections/indexes eagerly, but the runtime enforcement code belongs to the stage that first
  writes to each collection.
- Resolving the idea source's other open questions (emulation scale, `FirmwareWorkflow` vs
  activity, downgrade firmware as separate GridFS object) — none of them change this layout or
  schema.

## Decisions

1. **Makefile as the only build tool.** Targets wrap the exact DoD commands from AGENTS.md
   (`goimports -w .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...`) plus
   `proto`, `up`, `down`. Alternative: mage (rejected — a new tool dependency for what is a
   dozen shell lines) or Taskfile (rejected — external binary install for the same). Rationale:
   zero dependencies, universally known, and CI can call the same targets humans do.

2. **`web/` as a git submodule placeholder, registered in `.gitmodules`.** The frontend lives in a
   separate repository; this repo registers it and carries only a placeholder note (stack: React
   + Vite, delivered at stage 5). Alternative: in-repo `web/` per the current README map
   (superseded by explicit user decision). The Makefile Go targets ignore `web/` entirely so a
   clone without submodules stays buildable.

3. **Directory skeleton matches the planned map exactly, with one addition: `dashboards/`.**
   Grafana dashboard JSON is neither deploy manifest nor Go code nor proto, so it gets its own
   top-level home instead of being buried in `deploy/`. Entrypoints are
   `cmd/controlplane`, `cmd/worker`, `cmd/agent` (one per binary named in AGENTS.md). Each new
   directory carries a placeholder `README.md` naming its owning stage — the "never present a
   planned path as already working" rule made mechanical. `.gitkeep` is not used where a README
   can carry the explanation.

4. **MongoDB collections and field names follow the idea source's ER sketch verbatim** where it
   names fields (`id`/`model`/`region`/`current_fw`/`status`/`last_heartbeat` for devices;
   `version`/`gridfs_id`/`checksum` for firmware; `firmware_id`/`status`/`temporal_wf_id` for
   rollouts; `percent`/`status`/`success_rate` for waves; `ts`/`device_id`/`cpu`/`mem`/`health`
   for telemetry), mapping `id` → Mongo `_id`. This kills a rename churn later and keeps the
   public ER story and the code aligned. Added fields the sketch omits but the flows need:
   `waves.rollout_id` (the sketch's WAVE→ROLLOUT relation has no back-reference), `rollouts.region`
   + `rollouts.model` (the user flow's "target group = region + model" selector).

5. **`telemetry` is a time-series collection with `ts` as timeField and `meta` carrying
   `device_id`, `region`, `model`; the metric fields `cpu`, `mem`, `health` are top-level.**
   Idempotency via `_id = event_id` is kept exactly as AGENTS.md mandates — on a redelivered
   event the `_id` collision is swallowed as a no-op (duplicate-key error mapped to success), not
   bubbled to the consumer. Alternative: dedupe via a separate `processed_events` collection —
   rejected, double write for no benefit.

   One known tension to resolve during implementation (does not change the spec): MongoDB
   time-series collections have historically restricted custom `_id` values and unique secondary
   indexes. The implementer verifies behavior on the pinned MongoDB version; if custom `_id` on a
   TS collection is rejected, fall back to a plain collection with a TTL index on `ts` (still
   time-series-shaped data, still automatic retention) and record the deviation in the change
   notes. The spec's observable contract (one row per event id, automatic expiry) holds either
   way.

6. **Retention: 7 days for raw heartbeat telemetry, TTL on `ts`.** This resolves idea-source open
   question #2 in favor of retention over "keep forever": at the target scale (~5k events/sec)
   unbounded growth would dominate storage within days, and the demo's value is the live canary
   window, not historical telemetry. The period lives in one named config constant used both by
   the index bootstrap and any code that reasons about window age. Domain collections
   (`devices`, firmware metadata, `rollouts`, `waves`) carry no TTL; GridFS binaries live exactly
   as long as their metadata references them. Alternatives: unbounded (rejected per above);
   rollup aggregates before expiry — deferred, it is not needed by any current consumer.

7. **Index list is derived from named access patterns only** — device identity lookup,
   region+model pool/target-group queries, status filtering, rollout→waves listing, and
   device+time / region+model+time telemetry reads. Nothing else gets an index now (writes at
   heartbeat ingest rate pay per index). Notably `devices.status` and `rollouts.status` get
   single-field indexes because the UI's device list and rollout list filter by status;
   `telemetry` gets `{meta.device_id: 1, ts: -1}` and `{meta.region: 1, meta.model: 1, ts: -1}`.
   Uniqueness is only on `devices._id`, `firmware.version`, and event `_id` — waves deliberately
   have no unique (rollout_id, percent) constraint since a rollback may re-run a wave size.

## Risks / Trade-offs

- [Time-series collection quirks (custom `_id`, TTL semantics on the time field) may not match the
  mental model on the pinned MongoDB version] → Verify empirically at implementation time;
  decision 5 pre-authorizes the plain-collection fallback so the spec contract never breaks.
- [Field-name fidelity to the idea source's sketch locks in short names (`ts`, `current_fw`) that
  some reviewers find terse] → Accepted: consistency with the published ER diagram beats local
  naming taste; the spec spells every field out so there is no ambiguity.
- [Placeholder READMEs can rot into false claims as stages land] → Each placeholder names its
  stage; the stage's own Definition of Done includes deleting the placeholder it replaces (added
  to tasks as a cross-check).
- [Makefile drift from AGENTS.md's DoD block] → `make check` literally runs the four commands in
  order; if AGENTS.md changes, the Makefile is the single place to mirror, and `make help` shows
  the documented command for every target.
- [Submodule pinning: `web/` will point at whatever the frontend repo's default branch is] →
  Pin to a commit like any submodule; frontend churn never blocks the Go toolchain because Go
  targets never read `web/`.

## Migration Plan

None — greenfield. The change creates directories, the Makefile, `.gitmodules`, and placeholder
content; nothing to roll back beyond deleting those files. MongoDB schema is introduced when each
stage's code first creates collections (deploy stack may pre-create with indexes); no existing
data exists to migrate.

## Open Questions

- The frontend repository URL for `.gitmodules` — resolved at implementation time with the user
  if none exists yet; the placeholder records the intended URL and can be updated without
  touching the spec.
- Whether the deploy stack (stage 1) eagerly creates collections/indexes or the owning stage's
  code does it lazily — decided when stage 1 or 3 lands; both satisfy the spec as long as the
  indexes exist before traffic.