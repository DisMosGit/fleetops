# Spec Delta

## Purpose

Defines the FleetOps device-agent emulator: the simulated device fleet one emulator process
runs, the identity and simulated condition of each device, and the periodic heartbeat emission
that feeds the control plane's telemetry path.

## ADDED Requirements

### Requirement: Simulated device fleet
One agent emulator process SHALL run exactly the configured number of simulated devices, each as
an independent concurrent unit with its own stop condition tied to the process context.
Cancelling that context SHALL stop every device promptly and completely: no device goroutine may
outlive shutdown, whatever the fleet size.

#### Scenario: Fleet size comes from configuration
- **WHEN** the emulator starts with a configured fleet size of N
- **THEN** exactly N simulated devices run, each with its own device id

#### Scenario: Shutdown stops the whole fleet
- **WHEN** the emulator's context is cancelled
- **THEN** every device stops emitting and the emulator exits without leaking device goroutines

### Requirement: Simulated device identity
Each simulated device SHALL have a stable identity for its whole run: a device id unique within
the fleet, a model and a region drawn from the small fixed sets the emulator defines (so rollout
target groups of model + region are meaningful), the firmware version it currently runs
(`current_fw`), and a reported status. The identity fields SHALL be exactly those the device
registration exchange carries.

#### Scenario: Identity is stable across heartbeats
- **WHEN** one device emits several heartbeats
- **THEN** every heartbeat names the same device id, model, region, and firmware version

#### Scenario: Fleet covers rollout target groups
- **WHEN** the emulator builds a fleet of more than one device
- **THEN** the devices span more than one model and more than one region

### Requirement: Periodic heartbeat emission
Each simulated device SHALL emit one heartbeat per configured period for as long as it runs.
Every heartbeat SHALL carry: a unique `event_id`, the device id, the `current_fw` firmware
version, the reported `status`, the sample timestamp `ts`, and the `cpu`, `mem`, and `health`
samples. `cpu` and `mem` SHALL be utilisation values in [0.0, 1.0] and `health` a score in
[0.0, 1.0] where 1.0 is fully healthy. A heartbeat resubmitted after a stream reconnect SHALL
carry its original `event_id`, never a fresh one.

#### Scenario: Heartbeat carries the telemetry fields
- **WHEN** a device emits a heartbeat
- **THEN** the message carries event id, device id, current firmware, status, timestamp, and
  `cpu`, `mem`, and `health` samples within their documented ranges

#### Scenario: Emission is periodic
- **WHEN** a device runs for several heartbeat periods
- **THEN** it emits one heartbeat per period, at the configured period apart

#### Scenario: Event ids are unique per emission and stable per redelivery
- **WHEN** a device emits two distinct heartbeats
- **THEN** their event ids differ
- **WHEN** one heartbeat is redelivered after a reconnect
- **THEN** the redelivery carries the same event id as the original

### Requirement: Simulated health condition
A device's simulated condition SHALL drive its reported metrics and status: while the device is
normally healthy it reports status `online`, and during a simulated degradation episode it
reports status `degraded` with a visibly lower `health` score than in its healthy periods. The
simulation source SHALL be injectable so tests observe deterministic values without sleeping on
wall-clock randomness.

#### Scenario: Degradation lowers health and changes status
- **WHEN** a device enters a simulated degradation episode
- **THEN** its heartbeats report status `degraded` with a lower `health` score than before the
  episode
- **WHEN** the episode ends
- **THEN** its heartbeats report status `online` again

#### Scenario: Simulation is deterministic under an injected source
- **WHEN** tests drive the emulator with a scripted simulation source
- **THEN** the emitted heartbeats are exactly the scripted series, with no randomness of their own
