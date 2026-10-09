# Spec Delta

## MODIFIED Requirements

### Requirement: Worker hosts the device workflow and its activities

The worker binary SHALL register the device workflow, the rollout workflow, and their
worker-hosted activities on the configured task queue under their explicit registered names — the
device workflow, the device-state snapshot activity, and the rollout workflow's fleet-database,
device-signal, and health-evaluation activities — and shall poll that queue until shutdown. An
activity whose side effect lives in another process (command dispatch to the agent stream) SHALL
NOT be registered here: activities run beside the side effect they own.

#### Scenario: Startup registration

- **WHEN** the worker starts with a valid configuration
- **THEN** the device workflow, the rollout workflow, and their worker-hosted activities are
  registered on `temporal.task_queue` under their explicit names and the worker begins polling

#### Scenario: Registration names are explicit

- **WHEN** the registered workflows and activities are inspected (for example in the Temporal UI
  or on a workflow execution)
- **THEN** they appear under their explicit registered names, not under function-reflection
  names

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
