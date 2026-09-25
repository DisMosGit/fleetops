# Documentation

This directory holds FleetOps' deep-dive documents. Most of them don't exist yet — this file is
the index and the plan.

## Where content belongs

Four surfaces, one owner each. Put new content where it belongs instead of appending to whichever
file is open:

| Surface | Belongs here | Does not belong here |
|---------|-------------|----------------------|
| [README.md](../README.md) | What FleetOps is and why, architecture summary, tech stack, repo map, delivery status, entry-level getting started | Deep-dive technical documents, engineering rules, contribution process |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Contribution process: setup, PR workflow, review expectations, pointers to the rules | Engineering rules themselves (link to AGENTS.md), product/feature documentation |
| [AGENTS.md](../AGENTS.md) | Engineering rules for AI agents and humans: Go, Temporal, gRPC, storage, testing, Definition of Done | Process (PRs, reviews), architecture explanations, tutorials |
| `docs/` (this directory) | Deep-dive technical documents listed in the backlog below | Repeated copies of what the three root files already say |

## Future documents (backlog)

Each document is written when its trigger milestone lands — as part of that stage's work, not
speculatively before it (stages match the delivery-status table in
[README.md](../README.md#delivery-status)).

| Document | Purpose | Audience | Trigger milestone |
|----------|---------|----------|-------------------|
| `docs/architecture.md` | Full system architecture: components, data flow, deployment topology beyond the README summary | Contributors, evaluators | Stage 3 (streaming + telemetry paths in place) |
| `docs/api.md` | API contracts: gRPC services and HTTP gateway endpoints, idempotency semantics | Contributors, UI authors | Stage 1 (v1 proto frozen) |
| `docs/workflows.md` | Temporal deep-dive: `DeviceWorkflow` entity, `RolloutWorkflow` saga, signals, versioning, ContinueAsNew strategy | Contributors working on orchestration | Stage 2 |
| `docs/telemetry.md` | Telemetry pipeline: RabbitMQ topology, consumer semantics, DLQ, Mongo time-series + change streams | Contributors working on ingestion | Stage 3 |
| `docs/observability.md` | OTel trace paths, Prometheus metrics catalogue, Grafana dashboards | Operators, contributors | Stage 4 |
| `docs/demo.md` | Portfolio walkthrough: running a canary rollout, reading the rollback in Temporal UI, the demo GIF script | Evaluators, demo audience | Stage 5 |
