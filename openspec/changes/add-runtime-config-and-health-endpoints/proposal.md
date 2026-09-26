# Proposal

## Why

Every FleetOps binary today is a stub whose only configuration is a handful of hardcoded
`flag` defaults (`:9090`, `:8080`, `localhost:7233`, `fleetops`, `100`) with no way to point the
stack at real MongoDB, RabbitMQ, or Temporal endpoints — and no way to tell whether a running
control plane or worker can actually reach those dependencies. As the stages land (agent
heartbeats first, then Temporal workers and telemetry), both gaps become blocking: services need
one coherent place where simulation scale, ports, and endpoints live, and operators need HTTP
health/readiness probes that answer "is this process alive" and "can it reach its dependencies".

## What Changes

- **Introduce a single YAML configuration file** loaded by all three entrypoints
  (`cmd/controlplane`, `cmd/worker`, `cmd/agent`) through a shared `-config` flag. It covers the
  configuration surface the services need: simulation scale (emulated fleet size), gRPC listen
  and target addresses, MongoDB endpoint and database, RabbitMQ endpoint, Temporal address /
  namespace / task queue, and observability endpoints (OTel collector, Prometheus metrics
  address, health-probe listen address).
- **Validate the file on load and fail fast**: unknown keys, unparseable addresses/URIs,
  non-positive scale, and empty required strings are startup errors wrapped with the offending
  field — never a silently ignored typo. Missing fields take documented **sensible defaults**
  (today's flag values), so an empty file yields a working local-stack configuration.
- **Expose HTTP health and readiness endpoints on the control plane and the worker**:
  `/healthz` (liveness — the process is serving) and `/readyz` (readiness — connectivity to
  MongoDB, RabbitMQ, and Temporal is confirmed). Readiness reports per-dependency status and
  answers 503 until every dependency check passes.
- **Wire the entrypoints to the config**: the stub `flag` defaults in `cmd/` are replaced by
  values loaded from the configuration file; each binary uses the sections it needs (the agent
  emulator reads simulation scale and the control-plane gRPC target; the control plane and worker
  read their endpoints and the health listen address).

## Capabilities

### New Capabilities

- `runtime-config`: The single YAML configuration file — its sections and fields (simulation
  scale, gRPC, MongoDB, RabbitMQ, Temporal, observability), the defaults applied to absent
  fields, the validation contract that rejects malformed or unknown input at startup, and how
  the three entrypoints load and consume it.
- `health-readiness`: The HTTP liveness and readiness surface of the control plane and the
  worker — endpoint paths and status semantics, the readiness dependency-connectivity contract
  for MongoDB, RabbitMQ, and Temporal, and the per-dependency reporting shape.

### Modified Capabilities

None. No main specs exist yet (`openspec list --specs` is empty), and this change does not alter
the contracts defined by earlier changes: the `mongo-data-model` retention knob
(`FLEETOPS_TELEMETRY_RETENTION_DAYS`) keeps its existing meaning and is out of scope here.

## Impact

- **Files created:** an `internal/config` package (typed config structs, YAML loading,
  defaults, validation, plus tests) and an `internal/health` package (HTTP handlers, dependency
  connectivity checkers, plus tests); a sample configuration file (e.g. `deploy/config.yaml`)
  documenting every field and default.
- **Files touched:** `cmd/controlplane/main.go`, `cmd/worker/main.go`, `cmd/agent/main.go` —
  replace inline `flag` defaults with `-config` loading and wire the health servers;
  `go.mod` gains the two dependencies named below; `cmd/README.md`, `deploy/README.md`, and
  `internal/README.md` note the config file, the probe endpoints, and the new packages.
- **Dependencies:** `go.yaml.in/yaml/v3` (decoding the configuration file with strict
  unknown-key rejection) and `golang.org/x/sync/errgroup` (goroutine lifecycle for the probe
  server, per the concurrency rules) become direct `go.mod` requirements. MongoDB, RabbitMQ,
  and Temporal client libraries are *not* added by this change — readiness checks use dial-level
  connectivity (TCP/protocol handshake with timeout) behind small interfaces, so drivers land
  with the stages that actually use them.
- **No impact on:** the frozen v1 proto contracts, the MongoDB data model, workflow or telemetry
  behavior (nothing of them exists yet), or the staged delivery plan — config loading and health
  probes are stage-1 infrastructure that later stages consume.
