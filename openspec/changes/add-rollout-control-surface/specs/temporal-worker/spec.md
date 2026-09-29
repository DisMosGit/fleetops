# Spec Delta

## MODIFIED Requirements

### Requirement: Worker hosts the device workflow and its activities

The worker binary SHALL register the device workflow, the rollout workflow, and their
worker-hosted activities on the configured task queue under their explicit registered names — the
device workflow, the device-state snapshot activity, and the rollout workflow's fleet-database,
per-device update, and health-evaluation activities — and shall poll that queue until shutdown. An
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

#### Scenario: The per-device update activity is registered

- **WHEN** the worker registers its rollout activities
- **THEN** the activity that commands one device and waits for its reported result is among them
  under its explicit name

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
