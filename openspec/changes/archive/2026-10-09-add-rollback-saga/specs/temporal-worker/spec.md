# Spec Delta

## MODIFIED Requirements

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
