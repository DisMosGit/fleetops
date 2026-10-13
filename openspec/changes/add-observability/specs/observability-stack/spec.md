# Spec Delta

## Purpose

Provides the local observability stack the instrumentation needs somewhere to go: a collector that
receives traces and forwards them to a backend, Prometheus that scrapes the binaries, and Grafana
that renders the provisioned dashboards — runnable and verifiable without bringing up the
application stack.

## ADDED Requirements

### Requirement: The observability stack runs from the repository

`deploy/` SHALL carry compose manifests that start an OpenTelemetry collector, Prometheus, and
Grafana together, with every service's configuration mounted from a file in the repository rather
than from an inline or hand-edited definition. Bringing the stack up SHALL require no manual
configuration step beyond the documented environment values, and tearing it down SHALL remove the
services it started.

#### Scenario: The stack starts from a clean checkout
- **WHEN** an operator brings the observability stack up from the repository with no prior local state
- **THEN** the collector, Prometheus, and Grafana are all running and their ports answer

#### Scenario: Configuration is repository-owned
- **WHEN** the stack's services start
- **THEN** each one's configuration comes from a file tracked in `deploy/`, not from an inline compose definition or a manual edit inside a container

#### Scenario: The stack tears down cleanly
- **WHEN** an operator tears the stack down
- **THEN** the services it started stop and their containers are removed

### Requirement: The collector receives traces and forwards them to a backend

The collector SHALL accept OTLP traces on the endpoint the binaries are configured with and SHALL
forward them to the configured backend. The backend SHALL be selectable by configuration, including
a local option that requires no external account, and the collector SHALL keep receiving traces
when the backend is temporarily unavailable rather than failing its own listener.

#### Scenario: Traces reach the backend
- **WHEN** a binary exports a span to the collector and the backend is reachable
- **THEN** the span is forwarded to the backend and is retrievable there

#### Scenario: The backend is selected by configuration
- **WHEN** the backend setting is changed to a different exporter
- **THEN** the collector forwards traces to the new backend without any binary being rebuilt or reconfigured

#### Scenario: A backend outage does not stop collection
- **WHEN** the backend becomes unreachable while the collector is running
- **THEN** the collector keeps accepting traces on its listener and retries forwarding

#### Scenario: The collector's own health is observable
- **WHEN** the collector's health endpoint is queried
- **THEN** it reports the collector's status, so its liveness is checkable without reading logs

### Requirement: Prometheus scrapes every binary that exposes metrics

The stack SHALL carry a Prometheus configuration that scrapes the control plane, the worker, and
the agent on their configured metrics addresses, so every metric those processes expose is
queryable in the stack without manual target registration.

#### Scenario: All three binaries are scraped
- **WHEN** the control plane, the worker, and the agent are all running with the stack
- **THEN** Prometheus reports all three as targets and their metrics are queryable

#### Scenario: A binary that is down is visible as a target failure
- **WHEN** one binary stops while the stack runs
- **THEN** Prometheus reports that target as down rather than reporting the last value as current

#### Scenario: The scrape targets come from the repository
- **WHEN** the stack is started from a clean checkout
- **THEN** the scrape configuration is the file tracked in `deploy/`, and no target is added by hand

### Requirement: Grafana provisions its datasource and dashboards from files

Grafana SHALL start with its Prometheus datasource and the dashboards tracked in `dashboards/`
provisioned from files, so the dashboards are present on first start with no import step. A change
to a dashboard file SHALL be reflected on restart without editing anything through Grafana's UI.

#### Scenario: Dashboards are present on first start
- **WHEN** Grafana starts for the first time from the repository
- **THEN** its datasource resolves and the tracked dashboards are listed

#### Scenario: Dashboard edits are file-driven
- **WHEN** a tracked dashboard file changes and the stack is restarted
- **THEN** Grafana serves the changed dashboard without a manual import

#### Scenario: A dashboard that cannot be provisioned is reported
- **WHEN** a dashboard file is malformed
- **THEN** Grafana reports the provisioning failure rather than starting silently without that dashboard

### Requirement: The stack is driven by named targets

Bringing the observability stack up and down SHALL be available as named targets in the repository's
build tool, documented with the rest, so the stack is started the same way as every other gate and
its entry points are discoverable without reading the compose files.

#### Scenario: The stack has a documented up target
- **WHEN** the build tool's help is listed
- **THEN** the observability stack's up and down targets appear with the command each runs

#### Scenario: The target reflects a failure
- **WHEN** the stack cannot start — for example the container runtime is unavailable
- **THEN** the target exits non-zero with an error naming the cause

### Requirement: The stack does not require the application stack

The observability stack SHALL run independently of Temporal, MongoDB, RabbitMQ, and the k3d
cluster. Starting it SHALL NOT require the application stack to be up, and its failure SHALL NOT be
presented as an application failure.

#### Scenario: The stack runs alone
- **WHEN** the observability stack is started with no application services running
- **THEN** the collector, Prometheus, and Grafana run, and Prometheus reports the application targets as down rather than failing to start

#### Scenario: Application bring-up is unaffected
- **WHEN** the application stack is brought up without the observability stack
- **THEN** it starts as it does today and the absence of the collector only disables trace export
