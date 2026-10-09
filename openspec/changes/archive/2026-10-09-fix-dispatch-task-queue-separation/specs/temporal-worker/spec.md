# Spec Delta

## ADDED Requirements

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
