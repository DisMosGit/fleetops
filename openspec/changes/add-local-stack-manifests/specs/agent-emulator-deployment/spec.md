# Spec Delta

## Purpose

Defines how the simulated device fleet is deployed: how many devices run, how many pods run them,
what address the emulator reaches the control plane at, and the identity constraint a second
emulator process has to respect.

## ADDED Requirements

### Requirement: The emulated fleet's size is a configuration knob

The number of devices the stack emulates SHALL come from the emulator's configuration file, and the
stack's demo overlay SHALL declare the fleet size the demo starts with, so changing the simulated
fleet is a configuration change rather than an image or manifest rewrite. Every device SHALL be
served by a running emulator process, and no device SHALL be simulated by two processes at once.

#### Scenario: The configured size reaches the emulator

- **WHEN** the emulator's container starts with a mounted configuration naming a fleet size
- **THEN** it logs that fleet size and emits heartbeats for exactly that many devices, so the fleet
  an operator sees is the fleet the configuration names

#### Scenario: Changing the size is a configuration change

- **WHEN** the operator raises the configured fleet size and restarts the emulator's workload
- **THEN** the larger fleet registers and heartbeats without a new image being built

### Requirement: Emulator processes declare non-overlapping device identities

The emulator SHALL connect to the control plane at the in-cluster address the configuration names,
not a loopback address, and the stack SHALL be configured so that two emulator processes never
simulate the same device identity: a device identity registered by two live streams is routed by
whichever registered last, so a command meant for one process would be dispatched to the other. The
stack's default posture SHALL be a single emulator process; a deployment with more than one SHALL
mount configurations whose device ranges do not overlap.

#### Scenario: A single process runs the whole fleet

- **WHEN** the stack runs one emulator process with the demo's fleet size
- **THEN** every simulated device registers exactly once and heartbeat routing reaches it

#### Scenario: Distinct processes carry distinct identities

- **WHEN** an operator runs a second emulator process whose configured device range does not overlap
  the first process's range
- **THEN** both fleets register, and the control plane holds one live stream per device id

#### Scenario: The emulator reaches the control plane through the service

- **WHEN** the emulator's configuration names the control plane's in-cluster service address
- **THEN** the stream establishes and devices register, rather than the process retrying a
  connection to an address with no listener

### Requirement: The emulator's pod status reflects whether it is streaming

The emulator's workload SHALL declare resource requests and limits, SHALL stop its devices promptly
on termination so a replacement process starts cleanly, and SHALL leave running and failing
distinguishable: a pod whose process has exited SHALL not be reported as a running emulator.

#### Scenario: Termination stops the fleet

- **WHEN** the emulator's pod is deleted as part of scaling or teardown
- **THEN** the process shuts down within the pod's termination grace period, so device goroutines do
  not outlive the pod and no restart loop is reported

#### Scenario: A crashing emulator is visible as failing

- **WHEN** the emulator's process exits because it cannot reach the control plane or cannot load its
  configuration
- **THEN** the pod's status reports the failure rather than a running emulator, and its restart
  count reflects the restarts
