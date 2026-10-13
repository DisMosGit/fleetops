# Spec Delta

## Purpose

Hosts the device entity workflows and their database-bound activities on the shared task queue
through a stateless worker binary that can be scaled horizontally by running replicas, and hosts
the watcher that keeps the eligible-device view a wave resolution reads fresh.

## MODIFIED Requirements

### Requirement: Worker hosts the device workflow and its activities

The worker binary SHALL register the device workflow, the rollout workflow, and their
worker-hosted activities on the configured task queue under their explicit registered names — the
device workflow, the device-state snapshot activity, and the rollout workflow's fleet-database,
per-device update, health-evaluation, and rollback activities, the last of which are the per-device
downgrade, the per-device inventory reconciliation, and the announcement that publishes a rollback's
events — and shall poll that queue until shutdown. An activity whose side effect lives in another
process (command dispatch to the agent stream, or a firmware upload's storage) SHALL NOT be
registered here: activities run beside the side effect they own.

The worker SHALL also host the publisher a rollback's announcements are published through, since the
announcing step runs as one of its activities: the publisher SHALL be started with the worker, SHALL
keep a broker connection alive for the worker's lifetime, and SHALL reconnect after a broker outage
without a worker restart, so a rollback's announcement is published by the process that runs the
rollback rather than by a second one holding the same connections.

The worker SHALL also host the watcher that keeps the eligible-device view fresh from the `devices`
collection's change stream, because a wave resolution reads that view: the watcher SHALL be started
with the worker, SHALL run for the worker's lifetime, SHALL stop when the worker's context ends
without leaving a goroutine watching the collection, and SHALL be safe to run in every replica, so
that a rollout executed by any replica resolves against a view that is current.

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

#### Scenario: The eligible-device watcher runs in the worker

- **WHEN** the worker starts with a reachable database and a configuration selecting the region and
  model the fleet covers
- **THEN** the watcher is watching the `devices` collection before the worker resolves its first
  wave, and it stops when the worker's context ends

### Requirement: The dispatch activity runs on the control-plane queue

The activity that dispatches a command to a device's live agent stream SHALL be scheduled on the
control-plane task queue and registered there under its explicit name, because its side effect —
the agent connection — lives in the control-plane process. It SHALL NOT be scheduled on the work
queue, so its delivery cannot depend on which process happens to poll.

The firmware workflow and its upload activities SHALL run in the control-plane process on the
control-plane task queue, for the same rule in the other direction: the binary arrives in that
process as an HTTP request body, so the storage step's side effect — the stream — is there, and the
process that owns the stream is the one that owns the upload. The firmware workflow and its
activities SHALL NOT be scheduled on the work queue, and the worker SHALL register neither the
workflow nor its activities, because the worker neither receives the binary nor holds the store
binding the upload's steps write through.

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

#### Scenario: The control plane registers the firmware workflow and its upload activities

- **WHEN** the control plane starts with a valid configuration
- **THEN** the firmware workflow and the activities that validate a firmware's target models,
  store its binary, and write its metadata record are registered under their explicit names on the
  control-plane task queue, and the worker registers none of them

#### Scenario: Firmware upload tasks stay on the control-plane queue

- **WHEN** a firmware workflow schedules one of its upload activities
- **THEN** the activity task is placed on the control-plane task queue and not on the worker's task
  queue, so the storage step runs in the process that received the binary and holds its store
  binding
