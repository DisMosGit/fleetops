# FleetOps

FleetOps is a control plane and device-fleet simulator for over-the-air firmware updates: it rolls
new firmware out to an emulated IoT fleet in canary waves, gates each wave on real health
telemetry, and automatically rolls back the whole wave when health regresses.

It is a portfolio/demo project built to show what production-grade Go services look like when the
hard parts — long-lived orchestration, streaming transports, and failure handling — are done
deliberately: durable per-device entity workflows in Temporal, a rollback saga for canary
rollouts, bidirectional gRPC streams for agents, and a telemetry fan-out pipeline with idempotent
consumption and a dead-letter queue.

**Audience:** engineers evaluating Go/Temporal/distributed-systems craft, and contributors who
want a realistic playground for these patterns without production stakes.

## Highlights

- **Canary rollout saga** — waves 1% → 5% → 25% → 100%, each held open by a durable health
  window; on regression, compensating actions run in reverse (downgrade, inventory update,
  operator alert).
- **Entity workflows per device** — one long-lived Temporal workflow per device carries version,
  config, and last-heartbeat state through pod restarts, updated via signals.
- **Bidi agent streams** — a single long-lived gRPC stream per agent for heartbeats and commands,
  with backpressure and graceful reconnect.
- **Telemetry fan-out** — heartbeats fan out through RabbitMQ to analytics, alerting, and rollout
  health consumers; poison messages land in a DLQ; MongoDB time-series + change streams keep the
  eligible-device pool fresh without polling.

## Architecture

```
React UI ──HTTP/SSE──▶ API gateway ──▶ Temporal (workflows, workers)
                                            │
Device agents ◀─────gRPC bidi────────── gRPC server
     │                                     │
     └──heartbeats──▶ RabbitMQ ──▶ consumers ──▶ MongoDB (devices, telemetry, GridFS)
                                                             │
                              Prometheus ◀──metrics──  OTel traces
                                  └──▶ Grafana   (Temporal UI as second dashboard)
```

For the planned architecture document and the rest of the documentation plan, see the
[docs backlog](docs/README.md).

## Tech stack

| Layer | Technology | Why it is here |
|-------|-----------|----------------|
| Backend | Go | Idiomatic, concurrent services: control plane, Temporal workers, agent emulator |
| Orchestration | Temporal.io | Entity workflows, saga rollback, durable timers, signals |
| Database | MongoDB | Heterogeneous `devices`, time-series `telemetry`, GridFS firmware binaries, change streams |
| Broker | RabbitMQ | Heartbeat fan-out, rollout work queue, DLQ |
| Transport | gRPC + Protobuf | Bidirectional streaming for agents, versioned contracts |
| Frontend | React + Vite + Tailwind | Minimal operator UI over the HTTP/SSE gateway |
| Infra | Docker + k3d | The whole stack — control plane, agents, Temporal, Mongo, RabbitMQ — on one local cluster |
| Observability | OpenTelemetry + Prometheus + Grafana | Trace from workflow to individual heartbeat; wave/health metrics |

**Deliberately not used:** Kafka (RabbitMQ is enough and is the point of the demo), Redis (no
caching problem that justifies it), Helm (plain manifest files applied to k3d), Kubernetes in
production (out of scope — this is a local demo).

## Repository map

Planned layout — paths appear as their delivery stage lands (see status below):

```
cmd/<binary>/      thin entrypoints: control plane, workers, agent emulator
internal/agent/    device-agent emulator: heartbeat, stream client, firmware apply
internal/temporal/ DeviceWorkflow, RolloutWorkflow, FirmwareWorkflow + activities
internal/telemetry/ RabbitMQ publisher/consumers, idempotent ingestion, DLQ
api/proto/         Protobuf contracts + generated Go
deploy/            k3d/compose manifests
web/               React + Vite frontend
```

Engineering rules for this layout live in [AGENTS.md](AGENTS.md).

## Delivery status

| Stage | Scope | Status |
|-------|-------|--------|
| 1 | Protobuf contracts, service skeletons, agent emulator with heartbeats (no Temporal), compose → k3d manifests | Not started |
| 2 | Temporal: `DeviceWorkflow` entity, `RolloutWorkflow` waves + health window + saga rollback, workflow tests | Not started |
| 3 | gRPC bidi streams, RabbitMQ telemetry fan-out, idempotent consumer, DLQ | Not started |
| 4 | OTel instrumentation, Prometheus metrics, Grafana dashboard, Temporal UI in the stack | Not started |
| 5 | React UI (devices list, rollout page, approve/pause), README refresh + demo GIF | Not started |

## Getting Started

Prerequisites (as the stages land, the ones marked **planned** become required):

- **Go toolchain** (module `github.com/DisMosGit/fleetops`) — required from stage 1
- **Docker** — required from stage 1
- **k3d / kubectl** — **planned**, stage 1
- **Temporal CLI + Temporal UI** — **planned**, stage 2
- **Node.js + npm** — **planned**, stage 5 (frontend only)

> **Planned run instructions** — each is documented in this README when the stage that makes it
> work lands, not before:
> - local stack bring-up (compose/k3d) — **planned**, stage 1
> - starting the agent emulator fleet — **planned**, stage 1
> - running a canary rollout end-to-end — **planned**, stage 2
> - streaming telemetry and watching Grafana — **planned**, stage 3–4
> - launching the web UI — **planned**, stage 5

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to contribute: setup, PR workflow, Definition of Done
- [AGENTS.md](AGENTS.md) — engineering rules for AI agents and humans
- [docs/README.md](docs/README.md) — docs index and the plan for future documents
- [LICENSE.md](LICENSE.md) — MIT

## License

MIT — see [LICENSE.md](LICENSE.md).
