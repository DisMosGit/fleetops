# Spec Delta

## Purpose

Hosts the device entity workflows and their database-bound activities on the shared task queue
through a stateless worker binary that can be scaled horizontally by running replicas.

## ADDED Requirements

### Requirement: Worker hosts the device workflow and its activities
The worker binary SHALL register the device workflow and the device workflow's worker-hosted
activities on the configured task queue under their explicit registered names — the device
workflow and the device-state snapshot activity — and shall poll that queue until shutdown. An
activity whose side effect lives in another process (command dispatch to the agent stream) SHALL
NOT be registered here: activities run beside the side effect they own.

#### Scenario: Startup registration
- **WHEN** the worker starts with a valid configuration
- **THEN** the device workflow and the snapshot activity are registered on
  `temporal.task_queue` under their explicit names and the worker begins polling

#### Scenario: Registration names are explicit
- **WHEN** the registered workflow and activities are inspected (for example in the Temporal UI
  or on a workflow execution)
- **THEN** they appear under their explicit registered names, not under function-reflection
  names

### Requirement: Worker startup bootstraps the namespace's search attributes
Before polling tasks, the worker SHALL ensure the custom search attributes its workflows upsert
exist in the configured namespace. The bootstrap SHALL be idempotent and safe when several
replicas perform it at once: an attribute that already exists is left unchanged, and a
concurrent registration attempt does not fail the worker.

#### Scenario: Bootstrap precedes task polling
- **WHEN** the worker starts against a namespace without the custom search attributes
- **THEN** the attributes exist in the namespace before the worker executes its first workflow
  task

#### Scenario: Concurrent replica bootstrap is harmless
- **WHEN** several worker replicas start at once against the same namespace
- **THEN** all of them start successfully and the attribute definitions are unchanged

### Requirement: Worker replicas share work without coordination
Running several worker replicas of the same binary against the same task queue SHALL be safe
and effective: replicas hold no replica-local state that workflow or activity correctness
depends on, any replica may execute any workflow or activity task, adding or removing replicas
SHALL neither lose nor duplicate work, and a replica starting or stopping SHALL NOT require
coordination with its peers.

#### Scenario: Two replicas share the load
- **WHEN** two worker replicas poll the same task queue while device workflows process signals
  and snapshot their state
- **THEN** every task is executed exactly once per scheduling (per Temporal's task semantics)
  and the observable device state is the same as with one replica

#### Scenario: Replica loss changes nothing observable
- **WHEN** one of several replicas stops while device workflows are active
- **THEN** the remaining replicas keep the workflows running and no snapshot or state change is
  lost

### Requirement: Worker lifecycle and probes
The worker SHALL run until it receives a shutdown signal, then stop accepting new work, drain
what it is executing within a bounded timeout, and exit cleanly. Each replica SHALL serve the
liveness and readiness probes on the configured health address so a replica that cannot reach
its dependencies is visible to the platform.

#### Scenario: Graceful shutdown
- **WHEN** the worker receives SIGTERM while executing tasks
- **THEN** it stops polling, finishes in-flight work within the shutdown timeout, and exits
  without error

#### Scenario: Probes answer per replica
- **WHEN** a replica is running with its dependencies reachable
- **THEN** its liveness and readiness probes answer on `observability.health_addr`
