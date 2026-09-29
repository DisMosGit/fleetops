# Spec Delta

## Purpose

Defines the operator- and workflow-facing contract that answers one question over gRPC: how healthy
is a given wave of a given rollout right now, and on how much evidence.

## ADDED Requirements

### Requirement: Wave health query contract

The contract SHALL define a new proto package `fleetops.rollout.v1` with a `RolloutService` whose
unary `GetWaveHealth` RPC takes a rollout id and a wave id and returns the wave's health. The
response SHALL carry the requested rollout and wave ids, the success ratio, the sample size, a
verdict of healthy, unhealthy, or undecided, the effective window's start and end, and the values
the verdict was computed against (the per-sample health threshold, the minimum success ratio, and
the minimum sample count). A served response MUST carry a decided verdict or the undecided verdict,
never an unset one, and MUST echo the ids it was asked about so concurrent callers can match
results to requests.

#### Scenario: A wave's health is returned for a rollout and wave

- **WHEN** a client calls `GetWaveHealth` with a known rollout id and a wave of that rollout
- **THEN** it receives that wave's success ratio and sample size, the verdict, the window the
  samples were read from, and the thresholds the verdict used

#### Scenario: Identifiers are echoed

- **WHEN** a client queries health for a rollout and wave
- **THEN** the response repeats both ids, so a client with several queries in flight can match each
  result to its request

#### Scenario: The verdict set is closed

- **WHEN** any query is served
- **THEN** its verdict is healthy, unhealthy, or undecided, and a client never receives an
  unspecified verdict

### Requirement: Wave health query validation

`GetWaveHealth` SHALL validate its request before reading any state: a missing rollout id or wave
id MUST be rejected with `INVALID_ARGUMENT` and MUST NOT run an evaluation, and validation MUST NOT
distinguish a malformed request from any other malformed request in a way that leaks storage or
topology details.

#### Scenario: Empty wave id is rejected

- **WHEN** a client calls `GetWaveHealth` with an empty wave id
- **THEN** the call fails with `INVALID_ARGUMENT` and no evaluation runs

#### Scenario: Empty rollout id is rejected

- **WHEN** a client calls `GetWaveHealth` with an empty rollout id
- **THEN** the call fails with `INVALID_ARGUMENT` and no evaluation runs

### Requirement: Wave health failure mapping

Failures SHALL surface as gRPC status codes with operator-safe messages. An unknown rollout, an
unknown wave, and a wave that does not belong to the requested rollout MUST map to `NOT_FOUND`. A
storage failure MUST map to `INTERNAL` with a message that names no driver error, collection, or
infrastructure detail. An empty window is not a failure: it MUST be served as an undecided result
with sample size zero.

#### Scenario: Unknown rollout is not found

- **WHEN** a client queries health for a rollout id that does not exist
- **THEN** the call fails with `NOT_FOUND` and a message free of internal details

#### Scenario: Wave of another rollout is not found

- **WHEN** a client queries a wave id that belongs to a different rollout
- **THEN** the call fails with `NOT_FOUND` rather than answering for the wrong wave

#### Scenario: Storage failure leaks nothing

- **WHEN** the telemetry or wave lookup fails
- **THEN** the call fails with `INTERNAL` and the message carries no driver, collection, or
  topology detail

### Requirement: Frozen rollout v1 evolution

`fleetops.rollout.v1` SHALL freeze when this change lands: existing fields MUST keep their numbers
and types, and evolution is by adding fields (and messages, and RPCs) only. The agent contract
SHALL NOT be extended by this capability: an operator health query is not an agent exchange, and
messages are never shared across the two packages.

#### Scenario: Adding a field keeps old clients working

- **WHEN** a field is later added to the wave health response
- **THEN** clients generated before the addition keep working against the updated server without
  regeneration failures or wire incompatibility

#### Scenario: Renumbering is rejected

- **WHEN** a proposed change to this package reuses or reorders an existing field number
- **THEN** it is rejected as a v1 violation regardless of whether current code compiles

#### Scenario: The agent contract stays untouched

- **WHEN** the wave health query is added
- **THEN** `fleetops.agent.v1` gains no service, RPC, or message for it
