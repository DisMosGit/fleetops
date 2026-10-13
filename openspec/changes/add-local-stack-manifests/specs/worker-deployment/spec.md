# Spec Delta

## Purpose

Defines the worker's presence in the local stack: how its replica count is changed to demonstrate
horizontal processing, how each replica's ports and probes are kept independent, and the
dependencies a replica must not start without.

## ADDED Requirements

### Requirement: The replica count is a knob the operator changes at runtime

The worker's workload SHALL declare a replica count that the operator changes without rebuilding
images or editing the workload's template, and the stack SHALL keep working across that change: a
replica added picks up work, and a replica removed leaves no work unfinished. The demo overlay SHALL
declare the replica count the demo starts with.

#### Scenario: Scaling up adds processing capacity

- **WHEN** the operator raises the worker's replica count from one to several with the stack running
- **THEN** the added replicas become ready, poll the same task queue, and execute tasks, so workflows
  continue to make progress while more than one replica is serving

#### Scenario: Scaling down loses no work

- **WHEN** the operator lowers the worker's replica count while workflows are running
- **THEN** the removed replicas stop cleanly, and the remaining replicas continue executing the
  workflows those replicas had been handling, because no replica holds state the work depends on

#### Scenario: A removed replica is not restarted by bring-up

- **WHEN** the operator lowers the replica count and then runs bring-up again without changing the
  overlay
- **THEN** the count stays where the operator set it, and bring-up does not restore the higher count

### Requirement: Replicas do not contend for ports or state

Each worker replica SHALL serve its health endpoint on a port that does not clash with another
replica's, so several replicas run on the same node, and no worker replica SHALL hold durable state
that the stack loses when that replica is removed. Both SHALL hold: any replica executes any task,
and removing a replica loses no workflow or activity progress.

#### Scenario: Two replicas coexist on one node

- **WHEN** the worker runs more than one replica and the cluster schedules two of them on the same
  node
- **THEN** both report ready and both poll the task queue, because neither depends on a port or a
  host path the other holds

#### Scenario: Progress survives losing every replica at once

- **WHEN** every worker replica is removed and a replacement is started
- **THEN** workflows that were mid-execution resume where they left off, because workflow state lives
  in the workflow engine and device state in the document database, not in a replica

### Requirement: The worker does not start without the document topology it requires

The stack SHALL provide the document database as a replica set before the worker starts, because the
worker establishes a change stream over the device collection before it polls and fails its startup
when it cannot. The worker's readiness SHALL reflect that dependency, and bring-up SHALL NOT report
the stack up while the worker is not ready.

#### Scenario: A standalone database keeps the worker out of service

- **WHEN** the document service is reachable but is not a replica set
- **THEN** the worker does not report ready, so no wave resolution reads an unmaintained eligible
  device view

#### Scenario: A replica set releases the worker to poll

- **WHEN** the document service is an initiated replica set and the bootstrap has been applied
- **THEN** the worker establishes its change stream before polling, reports ready, and begins
  executing tasks
