# Spec Delta

## Purpose

Defines the platform services the applications are configured to talk to — the workflow engine and
its inspection UI, the metrics store, the dashboard, and the trace collector — where they live, what
they are wired to, and which of their inputs the applications do not yet produce.

## ADDED Requirements

### Requirement: The workflow engine serves the frontend the applications dial

The workflow engine's workload SHALL serve the frontend address the control plane and the worker are
configured with, in the namespace the workflows run in, and the stack SHALL make that address
reachable inside the cluster before any application workload starts polling. Workflow history SHALL
survive the engine's pod being restarted while the stack's volumes are preserved.

#### Scenario: The binaries reach the engine

- **WHEN** the control plane and the worker start with the engine's in-cluster address in their
  mounted configuration
- **THEN** both connect, register their workflows and activities, and begin polling without a
  connection error, so the namespace holds the task queues the binaries poll

#### Scenario: Workflow history survives a restart

- **WHEN** the workflow engine's pod restarts while the stack's volumes are preserved
- **THEN** the workflows that were running are still present with their histories, so a rollout
  interrupted by the restart resumes rather than being lost

### Requirement: The workflow engine's UI is exposed for operator inspection

The stack SHALL run the workflow engine's web UI as an in-cluster service configured to reach the
engine's frontend and to open the namespace the workflows run in, and bring-up SHALL make it
reachable from the operator's machine so a rollout can be inspected while it runs.

#### Scenario: The UI lists the namespace's workflows

- **WHEN** an operator forwards the UI's port and opens it while devices and rollouts are running
- **THEN** the UI reaches the engine's frontend, opens the configured namespace, and lists the
  running workflow executions

#### Scenario: The UI is not required for the stack to be up

- **WHEN** the UI's workload is not ready but the engine and the applications are
- **THEN** the applications continue to execute workflows, because no part of the application path
  depends on the UI

### Requirement: Metrics are collected from the endpoints that exist

The stack SHALL run a metrics store configured to scrape the components that serve a metrics
endpoint, and SHALL NOT configure a scrape target for a component that serves none. The store SHALL
retain scraped samples across its own pod restart while the stack's volumes are preserved.

#### Scenario: The control plane's metrics reach the store

- **WHEN** the metrics store scrapes the control plane's metrics port and a query is run against the
  collected series
- **THEN** the series the control plane exposes are present, so the store is reading the same
  registry the application serves

#### Scenario: No target answers nothing

- **WHEN** an operator inspects the metrics store's configured targets and their health
- **THEN** every configured target is a component that serves a metrics endpoint, and no target is
  configured for a component that serves none

### Requirement: The dashboard is wired to the metrics store

The stack SHALL run a dashboard configured to read the metrics store as its data source, with the
data source provisioned rather than added by hand, so a fresh stack is ready to query.

#### Scenario: The dashboard queries the store on a fresh stack

- **WHEN** an operator opens the dashboard on a freshly brought-up stack and queries its provisioned
  data source
- **THEN** the query returns the metrics the store has collected, without the operator having to add
  the data source first

### Requirement: The trace collector accepts what the applications will export

The stack SHALL run a trace collector that accepts the trace-export protocol on the endpoint the
applications are configured with, and SHALL record that no application currently exports to it, so
the collector is ready for the instrumentation that lands later rather than appearing wired when it
is not.

#### Scenario: The collector listens on the configured endpoint

- **WHEN** the collector is running and a client connects to the configured trace-export port
- **THEN** the connection is accepted and the collector reports receiving telemetry

#### Scenario: The absence of exported traces is documented, not implied away

- **WHEN** the stack's documentation describes the observability components
- **THEN** it states that the applications do not yet export traces — the configuration value that
  names the collector's endpoint is not consumed by any binary — so an operator does not read an
  empty trace view as a broken pipeline
