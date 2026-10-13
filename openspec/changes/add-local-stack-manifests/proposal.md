# Proposal

## Why

Every component of the stack runs, and every component is started by hand. `deploy/` carries the
MongoDB bootstrap, the configuration sample, and a README stating that the stack manifests "arrive
at stage 1"; there is no `Dockerfile` in the repository, no manifest, and no namespace, so `make up`
runs `kubectl apply -f deploy/` against a directory with no Kubernetes YAML — it does nothing, and
`make down` does the same. The workflow engine, the broker, the document database, the metrics
endpoint, and the three binaries exist only as things a developer assembles from a terminal.

This is stage 1's remaining deliverable (README delivery status: "compose → k3d manifests"). It
lands now because the components it deploys have landed: the proto contracts are frozen, the agent
emulator registers and heartbeats, the control plane serves gRPC and HTTP, and the worker hosts the
workflows. The deployment is the last thing standing between the repository and a running stack,
and it is also what two recorded tasks are waiting on —
`add-device-search-attributes-snapshot-and-worker` defers tasks 5.3 and 6.2 to "the stage that
delivers the local stack", and four capability scenarios in `temporal-worker` and
`device-search-attributes` have no automated test because none of them is reachable without one.

## What Changes

- **A cluster script that targets the Kubernetes cluster the operator already has.** k3s is
  installed and single-node on the development machine, so `deploy/cluster.sh` does not create a
  cluster; it verifies one is reachable, that it has a default StorageClass to bind volumes
  against, and that the FleetOps namespaces exist — creating them idempotently and reporting what
  it found. It is re-runnable and is the first step `make up` performs.
- **Container images for the three binaries, built and published into the cluster's own image
  store.** One multi-stage `Dockerfile` parameterized by binary builds `controlplane`, `worker`,
  and `agent` from the module at a pinned Go version, and the bring-up script imports the results
  into the node's containerd image store. The cluster has no registry, so a manifest that only
  pushes to one would not run.
- **Manifests and overlays under `deploy/k8s/`.** A `base/` every environment can run, and
  `overlays/demo/` holding the local demo's knobs — replica counts, simulated fleet size, and
  resource sizes — applied with `kubectl apply -k`, which needs no tool beyond the `kubectl`
  already required. The base is stage-shaped, not demo-shaped: it is what a real environment would
  extend, and the overlay is the only place a demo value lives.
- **Application manifests.** `controlplane` with its gRPC, HTTP, metrics, and health ports, a
  `Service` for gRPC traffic, readiness and liveness probes against the documented endpoints, and
  resource requests and limits. `worker` as a `Deployment` with no replica-local state, so its
  replica count is a knob the demo raises to show horizontal processing. `agent` emulating a
  configurable number of devices from the configuration file, sized by the demo overlay.
- **The emulated fleet runs in one process, and the stack says why.** Device identities are
  `device-<index>` per process, fixed at the emulator's construction and exposed by no configuration
  field, so two emulator processes simulate the same identities and the control plane routes each
  device id to whichever stream registered last — a dispatched command would reach the wrong
  process. The stack therefore runs one emulator, sized by the configured fleet size, and the spec's
  requirement on non-overlapping identities states the constraint that a second process would have
  to satisfy. Splitting the fleet across several processes needs the emulator to parameterize its
  identities, which is a configuration-contract change and belongs in its own change, not here.
- **Data and platform manifests in their own namespaces.** MongoDB as a single-node replica set
  with a persistent volume — required, not preferred: the worker establishes a change stream over
  `devices` before it polls and fails startup without one — and RabbitMQ with a persistent volume,
  both in a data namespace separated from application services. Temporal and its UI, Prometheus,
  Grafana with a provisioned Prometheus data source, and an OpenTelemetry collector in a platform
  namespace, wired to the addresses the applications are configured with.
- **One command up, one command down.** `make up` builds images, applies the base and demo overlay,
  waits for the workloads to become ready, and prints the endpoints an operator reaches them at.
  `make down` removes the workloads and, when asked for it explicitly, the persistent volumes —
  volume deletion is never implicit, because a stray `make down` must not be how a demo's data
  disappears.
- **Documentation that says what the stack does and does not do yet.** `deploy/README.md` loses its
  "Not implemented yet" line and documents bring-up, the namespace layout, and the follower
  ordering (an emulator must register devices before a rollout has a fleet to target). The
  README's delivery-status table stops claiming stages 1–5 are "Not started".

## Capabilities

### New Capabilities

- `local-cluster-bootstrap`: the cluster script — reachability, the StorageClass a volume needs,
  namespace creation, and what it reports and refuses.
- `fleetops-stack-lifecycle`: the single command up and the single command down, the ordering they
  respect, readiness gating, the endpoints bring-up reports, and explicit-only volume cleanup.
- `control-plane-deployment`: the control plane's workload — ports, gRPC `Service`, probes against
  the documented endpoints, resource requests and limits, and the configuration it mounts.
- `worker-deployment`: the worker's workload — replica count as a knob, no replica-local state,
  the change-stream dependency MongoDB's topology has to satisfy, and per-replica ports.
- `agent-emulator-deployment`: the emulator's workload — target address from configuration, the
  device count that spreads over a small pod count, and the registration ordering a demo follows.
- `data-services-deployment`: MongoDB and RabbitMQ — the data namespace, persistent volumes, the
  replica set the change stream needs, and the readiness that gates the services depending on them.
- `platform-services-deployment`: Temporal and its UI, Prometheus, Grafana, and the OTel collector —
  where they live, what they are wired to, and which of their inputs do not exist yet.

### Modified Capabilities

None. No existing requirement's behavior changes: this change adds deployment, and the requirements
it must satisfy about probes (`health-readiness`), the change stream (`temporal-worker`), and the
telemetry endpoints (`telemetry-ingest`) are already specified. It configures those components and
does not alter them.

## Impact

- **New files**: `deploy/Dockerfile`, `deploy/cluster.sh`, `deploy/up.sh`, `deploy/down.sh`,
  `deploy/k8s/base/**`, `deploy/k8s/overlays/demo/**`. Nothing under `internal/`, `api/`, or `cmd/`
  changes: the stack configures the binaries that exist, and every value it needs is already a field
  of the configuration file.
- **New dependency**: a container image builder and a reachable Kubernetes cluster. Both are
  already present on the development machine (Docker, k3s with `local-path` storage); no new Go
  dependency, and no chart repository.
- **`Makefile`**: `up` and `down` stop being stubs and gain sibling targets. `check`, `proto`, and
  the Definition of Done are untouched.
- **Configuration stays a mounted file.** Every binary reads `-config` and no binary reads the
  environment, so the stack mounts one `ConfigMap` per environment and the demo overlay's values
  (in-cluster service addresses, `simulation.fleet_size`) live there. Changing that is not in this
  change.
- **Two limits this change records rather than hides.** Tracing does not flow: `observability.otel_endpoint`
  is declared and validated but consumed by no binary, so the collector will receive nothing until
  the instrumentation stage lands — the collector is deployed ready for it and the spec says so.
  And `worker` serves no `/metrics` endpoint, so Prometheus scrapes what exists (`controlplane`
  today) rather than a target that answers nothing.
- **Not in scope**: the Grafana dashboards their panels would read (`dashboards/` stays a
  placeholder; the metrics they need do not all have sources), the web UI (`web/` is an empty
  submodule, stage 5), and a compose path (the request asked for the cluster; the README's
  "compose first" ordering is superseded here and the divergence is recorded).
