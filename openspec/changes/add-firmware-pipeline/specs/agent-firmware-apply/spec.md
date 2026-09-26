# Spec Delta

## Purpose

Defines the agent-side firmware application step: a deterministic simulation of flashing a
downloaded binary — configurable duration and success rate — that stands in for real hardware so
rollout behavior can be exercised.

## ADDED Requirements

### Requirement: Deterministic simulated application
Applying a downloaded firmware SHALL be simulated: it takes the configured simulated apply delay
and then succeeds or fails according to the configured success rate. The outcome SHALL be drawn
from the emulator's seeded random source, so the same seed replays the same sequence of apply
outcomes across runs. The simulated delay SHALL be interruptible: cancelling the agent's context
stops an in-flight application promptly.

#### Scenario: Same seed replays the same outcomes
- **WHEN** two emulator runs with the same seed apply firmware in the same order
- **THEN** both runs produce the same sequence of apply successes and failures

#### Scenario: Success rate bounds outcomes
- **WHEN** the configured success rate is `1.0`, or `0.0`
- **THEN** every simulated application succeeds, or every one fails, respectively

#### Scenario: Shutdown interrupts an in-flight application
- **WHEN** the agent's context is cancelled during the simulated apply delay
- **THEN** the application stops promptly and no further update steps run

### Requirement: Firmware adoption on success
A successful application SHALL adopt the applied firmware: the emulated device reports the new
firmware version in subsequent heartbeats and the command concludes as succeeded. A failed
application SHALL keep the device's previous firmware version and conclude the command as failed
with operator-safe detail, and the agent SHALL report the applying and terminal phases —
completed on success, failed with detail on failure — as update status.

#### Scenario: Applied version appears in heartbeats
- **WHEN** a device applies firmware version X successfully
- **THEN** its subsequent heartbeats report `current_fw` X and the command is concluded as succeeded

#### Scenario: Failed apply keeps the previous version
- **WHEN** a simulated application fails
- **THEN** the device keeps its previous firmware version, reports a failed update status with
  detail, and concludes the command as failed
