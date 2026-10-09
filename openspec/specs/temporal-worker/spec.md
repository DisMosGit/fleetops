# temporal-worker Specification

## Purpose
Hosts the device entity workflows and their database-bound activities on the shared task queue
through a stateless worker binary that can be scaled horizontally by running replicas.

## Requirements

### Requirement: Worker hosts the device workflow and its activities

The worker binary SHALL register the device workflow, the rollout workflow, and their
worker-hosted activities on the configured task queue under their explicit registered names — the
device workflow, the device-state snapshot activity, and the rollout workflow's fleet-database,
per-device update, health-evaluation, and rollback activities, the last of which are the per-device
downgrade, the per-device inventory reconciliation, and the announcement that publishes a rollback's
events — and shall poll that queue until shutdown. An activity whose side effect lives in another
process (command dispatch to the agent stream) SHALL NOT be registered here: activities run beside
the side effect they own.

The worker SHALL also host the publisher a rollback's announcements are published through, since the
announcing step runs as one of its activities: the publisher SHALL be started with the worker, SHALL
keep a broker connection alive for the worker's lifetime, and SHALL reconnect after a broker outage
without a worker restart, so a rollback's announcement is published by the process that runs the
rollback rather than by a second one holding the same connections.

#### Scenario: Startup registration

- **WHEN** the worker starts with a valid configuration
- **THEN** the device workflow, the rollout workflow, and their worker-hosted activities are
  registered on `temporal.task_queue` under their explicit names and the worker begins polling

#### Scenario: Registration names are explicit

- **WHEN** the registered workflows and activities are inspected (for example in the Temporal UI
  or on a workflow execution)
- **THEN** they appear under their explicit registered names, not under function-reflection
  names

#### Scenario: The per-device update activity is registered

- **WHEN** the worker registers its rollout activities
- **THEN** the activity that commands one device and waits for its reported result is among them
  under its explicit name

#### Scenario: The rollback activities are registered

- **WHEN** the worker registers its rollout activities
- **THEN** the activity that restores one device to the firmware it ran before, the activity that
  reconciles one device's recorded firmware version against that device, and the activity that
  publishes a rollback's announcement are among them under their explicit names

#### Scenario: The announcement publisher is hosted by the worker

- **WHEN** the worker starts
- **THEN** the publisher a rollback's announcements use is running in the worker process before it
  polls for its first task

#### Scenario: A broker outage does not stop an announcement

- **WHEN** the worker's broker connection drops and the broker becomes reachable again
- **THEN** the publisher reconnects without a worker restart and a later announcement is published
  through the reconnected session

#### Scenario: Agent transport stays in the control plane

- **WHEN** the worker registers its workflows and activities
- **THEN** the activity that dispatches a command to a device's live agent stream is not among
  them, because its side effect lives in the control-plane process that holds the agent
  connections

#### Scenario: Rollout work is shared by worker replicas

- **WHEN** a rollout is executing while the worker replica that started it stops and another
  replica is polling the same task queue
- **THEN** the remaining replica continues the rollout from its recorded wave state, without a
  wave being dispatched twice and without the rollout needing replica-local state

### Requirement: Worker startup bootstraps the namespace's search attributes

Before polling tasks, the worker SHALL ensure the custom search attributes its workflows upsert —
the device attributes (`DeviceRegion`, `DeviceModel`, `DeviceFirmware`, `DeviceOnline`) and the
rollout attributes (`RolloutFirmware`, `RolloutRegion`, `RolloutStatus`) — exist in the configured
namespace. The bootstrap SHALL be idempotent and safe when several replicas perform it at once: an
attribute that already exists is left unchanged, and a concurrent registration attempt does not
fail the worker.

#### Scenario: Bootstrap precedes task polling

- **WHEN** the worker starts against a namespace without the custom search attributes
- **THEN** the device and rollout attributes exist in the namespace before the worker executes its
  first workflow task

#### Scenario: The rollout attributes are bootstrapped too

- **WHEN** the worker starts against a namespace that carries the device attributes but not the
  rollout ones
- **THEN** it registers `RolloutFirmware`, `RolloutRegion`, and `RolloutStatus` as Keyword
  attributes before it polls, and leaves the device attributes unchanged

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

### Requirement: Task queues are polled only by processes that host every task type they can deliver

Temporal routes a task to any poller of its queue, not to a poller that registered that task type.
Every process that polls a task queue SHALL therefore register every workflow and activity type
that queue can deliver, and a process SHALL NOT poll a queue that can deliver a task type it does
not host. The control plane SHALL NOT poll the work queue the worker hosts, so no process is ever
handed a task type it would fail as unknown.

#### Scenario: The control plane never polls the work queue

- **WHEN** the control plane starts with a valid configuration
- **THEN** it polls only the control-plane task queue and not `temporal.task_queue`, so a device,
  snapshot, or rollout task is never delivered to it

#### Scenario: The worker never polls the control-plane queue

- **WHEN** one or more worker replicas start with a valid configuration
- **THEN** they poll only `temporal.task_queue`, and the control-plane task queue is left for the
  control plane alone

#### Scenario: A workflow added to the work queue cannot be bounced

- **WHEN** a workflow or activity type is added to the work queue in future
- **THEN** it is delivered only to processes that poll that queue and host its type, because the
  control plane — the one process that does not host them — is not among its pollers

### Requirement: The dispatch activity runs on the control-plane queue

The activity that dispatches a command to a device's live agent stream SHALL be scheduled on the
control-plane task queue and registered there under its explicit name, because its side effect —
the agent connection — lives in the control-plane process. It SHALL NOT be scheduled on the work
queue, so its delivery cannot depend on which process happens to poll.

#### Scenario: The control plane registers the dispatch activity

- **WHEN** the control plane starts with a valid configuration
- **THEN** it registers the command-dispatch activity under its explicit name on the
  control-plane task queue and begins polling that queue

#### Scenario: Device commands are scheduled on the control-plane queue

- **WHEN** a device workflow dispatches a pending command to its device
- **THEN** the activity task is placed on the control-plane task queue, where the process holding
  the agent connections executes it

#### Scenario: A device command still reaches its device

- **WHEN** a device workflow holds a pending command and the control plane is running
- **THEN** the command is delivered to the device's live agent stream and the pending command is
  marked dispatched, exactly as before the queue separation
