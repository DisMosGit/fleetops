# Spec Delta

## Purpose

Defines the one command that brings the whole FleetOps stack up and the one command that takes it
down: the ordering they respect, what bring-up waits for and reports, and how a teardown removes
persisted data only when the operator asks for it by name.

## ADDED Requirements

### Requirement: One command brings the whole stack up

A single documented command SHALL build the application images, verify the cluster, apply the
manifests for every component, and wait for the stack to become ready. It SHALL be idempotent: run
again against a stack that is already up, it converges on the same state without restarting
workloads that are already running the same image.

#### Scenario: A cold cluster reaches a running stack

- **WHEN** the operator runs the bring-up command on a cluster with the stack absent
- **THEN** the images are built, every component's manifests are applied, bring-up waits until the
  workloads report ready, and the command exits successfully

#### Scenario: A second bring-up changes nothing

- **WHEN** the operator runs the bring-up command again with no changes to the repository
- **THEN** it exits successfully, no running pod is restarted, and the stack reaches the same ready
  state it already had

#### Scenario: A failure names the component that did not become ready

- **WHEN** a workload never reports ready within the wait period
- **THEN** bring-up exits non-zero and names that workload, so the operator is not left to guess
  which component failed

### Requirement: Bring-up respects component dependencies

Bring-up SHALL apply the data services before the platform services and the application services,
and SHALL NOT declare an application workload ready before the services it depends on are
available. The document database SHALL be a replica set before the worker starts, because the
worker establishes a change stream on startup and fails when it cannot.

#### Scenario: Data services precede the worker

- **WHEN** the stack is applied from scratch
- **THEN** the document database accepts connections on a replica set, the repository's data-model
  bootstrap has been applied to it, and the message broker accepts connections, before any
  application workload begins polling for work

#### Scenario: The worker's readiness follows its dependencies

- **WHEN** the document database or the message broker is not yet reachable
- **THEN** the worker does not report ready, and bring-up does not report the stack as up

### Requirement: Bring-up reports the endpoints an operator reaches

When the stack is ready, bring-up SHALL print the addresses an operator inspects the demo through —
the operator HTTP surface, the gRPC surface the emulator connects to, the workflow engine's UI, and
the dashboard — together with the command that forwards each to the operator's machine, because the
cluster's services are not exposed outside it.

#### Scenario: Endpoints are printed once the stack is ready

- **WHEN** bring-up completes successfully
- **THEN** it prints one line per operator surface naming the service, its namespace, and how to
  reach it

#### Scenario: The printed surface works

- **WHEN** the operator runs a printed forwarding command for the workflow engine's UI and opens
  the address it names
- **THEN** the workflow engine's UI answers and lists the namespace the workflows run in

### Requirement: One command takes the stack down

A single documented command SHALL remove every workload the stack declares, in an order that stops
the application services before the services they depend on. It SHALL be idempotent and SHALL exit
successfully when the stack is already down.

#### Scenario: A running stack is removed

- **WHEN** the operator runs the teardown command against a running stack
- **THEN** every workload, service, and configuration object the stack declares is removed, and the
  command exits successfully

#### Scenario: Teardown of a stack that is already down succeeds

- **WHEN** the operator runs the teardown command twice
- **THEN** the second run removes nothing, reports nothing to remove, and exits successfully

### Requirement: Persisted data is removed only when asked for by name

Teardown SHALL preserve every persistent volume claim by default, and SHALL remove them only when
the operator explicitly requests volume cleanup. Volume cleanup SHALL remove the claims and the
data they hold, and SHALL report what it removed.

#### Scenario: A default teardown keeps the data

- **WHEN** the operator runs the teardown command without requesting volume cleanup
- **THEN** the stack's workloads are gone, its persistent volume claims and the data in them remain,
  and a later bring-up starts with the same data

#### Scenario: Volume cleanup removes the data and says so

- **WHEN** the operator runs the teardown command requesting volume cleanup
- **THEN** the persistent volume claims are removed, the volumes they bound are deleted, and the
  command reports each claim it removed
