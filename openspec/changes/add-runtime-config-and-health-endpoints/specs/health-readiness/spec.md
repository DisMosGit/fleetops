# Spec Delta

## Purpose

Defines the HTTP liveness and readiness surface of the control plane and the worker: endpoint
paths and status semantics, and the readiness contract that confirms connectivity to MongoDB,
RabbitMQ, and Temporal before a service reports itself ready.

## ADDED Requirements

### Requirement: Liveness endpoint

The control plane and the worker SHALL each serve `GET /healthz` on the listener configured by
`observability.health_addr`. The endpoint reports only that the process is up and serving: it
performs no dependency checks and returns `200 OK` with body `{"status":"ok"}` for as long as
the HTTP listener responds. The agent emulator has no health surface.

#### Scenario: Process is alive

- **WHEN** `GET /healthz` is sent to a running control plane or worker
- **THEN** the response is `200 OK` with body `{"status":"ok"}`

#### Scenario: Liveness stays green while dependencies are down

- **WHEN** MongoDB, RabbitMQ, and Temporal are all unreachable and `GET /healthz` is sent
- **THEN** the response is still `200 OK`, because liveness does not consult dependencies

### Requirement: Readiness endpoint confirms dependency connectivity

The control plane and the worker SHALL each serve `GET /readyz` on the listener configured by
`observability.health_addr`. Each request SHALL confirm current connectivity to MongoDB,
RabbitMQ, and Temporal — the configured endpoints accepting connections within a short timeout
— and return `200 OK` only when all three checks pass, otherwise `503 Service Unavailable`. The
response body SHALL report each dependency separately with status `"ok"` or
`"error"` and, on error, a short summary — for example
`{"status":"ready","checks":{"mongodb":{"status":"ok"},"rabbitmq":{"status":"ok"},"temporal":{"status":"ok"}}}`.
The summary MUST NOT contain credentials or other secrets from the configured URIs.

#### Scenario: All dependencies reachable

- **WHEN** `GET /readyz` is sent while MongoDB, RabbitMQ, and Temporal all accept connections
- **THEN** the response is `200 OK` and every entry in `checks` has status `"ok"`

#### Scenario: One dependency down

- **WHEN** `GET /readyz` is sent while RabbitMQ is unreachable and the other two are reachable
- **THEN** the response is `503 Service Unavailable`, `checks.rabbitmq.status` is `"error"`, and
  the other two checks report `"ok"`

#### Scenario: Readiness recovers without restart

- **WHEN** a dependency was unreachable, becomes reachable again, and `GET /readyz` is sent afterwards
- **THEN** the response reflects the current state (`200 OK` once all three are reachable) without
  restarting the process

#### Scenario: Readiness answers promptly when everything is down

- **WHEN** `GET /readyz` is sent while every dependency is unreachable
- **THEN** the response is `503 Service Unavailable` within a bounded time (each check has its own
  short timeout; the request does not hang)

#### Scenario: Error summaries never leak credentials

- **WHEN** `GET /readyz` reports a failing check for a configured URI containing credentials
- **THEN** the response body contains neither the URI nor its credentials
