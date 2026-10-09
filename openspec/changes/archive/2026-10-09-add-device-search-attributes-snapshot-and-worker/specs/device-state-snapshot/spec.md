# Spec Delta

## Purpose

Projects the device workflow's authoritative state into MongoDB — one snapshot document per
device — at a bounded cadence and immediately whenever something meaningful changes, so the
database reflects the entity without reading workflow histories.

## ADDED Requirements

### Requirement: Periodic state snapshots
Each device workflow SHALL persist a snapshot of its state at the configured snapshot interval,
regardless of how much the device reports in, so a stored snapshot of a device is never older
than the interval plus one write attempt.

#### Scenario: Idle device keeps getting snapshots
- **WHEN** a device sends no signals for several snapshot intervals
- **THEN** its snapshot document keeps being refreshed at the configured cadence

#### Scenario: Cadence follows configuration
- **WHEN** the snapshot interval is configured to `30s`
- **THEN** each device workflow persists a snapshot roughly every 30 seconds

### Requirement: Snapshots on meaningful transitions
The device workflow SHALL persist a snapshot immediately whenever its state changes
meaningfully: a firmware version is adopted (by heartbeat or by a concluded command), the
pending command is set, superseded, or concluded, a configuration snapshot is applied, or the
liveness status flips. A heartbeat that only refreshes the last-heartbeat timestamp SHALL NOT
trigger an immediate snapshot — the periodic snapshot bounds how stale that field is.

#### Scenario: Firmware adoption snapshots immediately
- **WHEN** a device workflow adopts a new firmware version
- **THEN** a snapshot carrying that version is persisted without waiting for the next periodic
  snapshot

#### Scenario: Command lifecycle snapshots immediately
- **WHEN** a command becomes pending, is superseded, or is concluded
- **THEN** a snapshot reflecting the resulting pending-command state is persisted

#### Scenario: Routine heartbeat defers to the periodic snapshot
- **WHEN** an applied heartbeat only moves the last-heartbeat timestamp forward
- **THEN** no immediate snapshot is persisted for that heartbeat

### Requirement: Snapshot content records the full state
Each snapshot SHALL record the device's state at snapshot time: the device identity, region,
model, current firmware version, liveness status, last heartbeat timestamp, the pending command
or its absence, and the configuration snapshot with its version — together with the time the
snapshot was taken.

#### Scenario: Snapshot reflects the complete state
- **WHEN** a snapshot is taken for a device with region `eu-west`, model `sensor-2`, firmware
  `2.0.3`, online status, a pending update command, and configuration version 7
- **THEN** the stored snapshot carries all of those values and a snapshot time

### Requirement: Snapshot persistence is idempotent and failure-isolated
Snapshots SHALL persist as exactly one document per device identity, updated in place: writing
the same state twice (an activity retry or replay) MUST NOT create a second document or leave
divergent content. A snapshot write that fails SHALL NOT terminate the device workflow — the
next snapshot attempt persists the then-current state.

#### Scenario: Retried write converges
- **WHEN** a snapshot write is retried for unchanged state
- **THEN** exactly one document exists for the device and it matches the state

#### Scenario: Failed snapshot leaves the entity running
- **WHEN** a snapshot write fails
- **THEN** the device workflow keeps processing signals and a later snapshot persists the
  current state
