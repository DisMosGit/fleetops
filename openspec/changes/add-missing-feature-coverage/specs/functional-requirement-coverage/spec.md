# Spec Delta

## Purpose

Establishes that the project's own capability scenarios are backed by tests: what counts as backing
a scenario, how the mapping from scenario to test is recorded, and which scenarios are legitimately
exempt because their verification needs a running local stack the delivery plan has not reached.

## ADDED Requirements

### Requirement: Every capability scenario is backed by a test

Every scenario under `openspec/specs/` SHALL be backed by at least one test that produces the
scenario's trigger and asserts its outcome, unless the scenario is named in the recorded list of
exemptions. A test SHALL back a scenario only when it fails if the behavior the scenario describes
is removed or reversed. A test that requires the behavior without checking it — calling a code path
and asserting nothing about the outcome, or asserting a value the test itself constructed — SHALL
NOT count as backing. The suite SHALL be runnable as one command that tells a reader which
scenarios are backed and which are not.

#### Scenario: A removed behavior fails its backing test

- **WHEN** the behavior a scenario describes is removed or reversed in the implementation
- **THEN** at least one test fails, and that test is the one recorded as backing the scenario

#### Scenario: The coverage check reports a newly unbacked scenario

- **WHEN** a scenario exists under `openspec/specs/` with no test producing its trigger and
  asserting its outcome, and it is not named as an exemption
- **THEN** the coverage check reports the scenario and its capability as unbacked, and fails

#### Scenario: An exemption is named rather than inferred

- **WHEN** a scenario's verification needs an environment the delivery plan has not reached
- **THEN** it is listed as an exemption with the reason and what would verify it, and the coverage
  check passes while reporting it as exempt rather than silently omitting it

### Requirement: The scenario-to-test mapping is recorded in the repository

The mapping from each capability scenario to the test that backs it, and the list of exemptions
with their reasons, SHALL be recorded in the repository rather than derived from discussion or
memory. The record SHALL name the scenario, the test that backs it, and the file it lives in, so a
reader follows it to the test without searching. The record SHALL be checked by a command that
fails when a scenario is unbacked and unexempted, so a scenario added without a test is caught
rather than drifting.

#### Scenario: A reader follows a scenario to its test

- **WHEN** a reader looks up a scenario in the record
- **THEN** it names the test and the test file that back it, and that test exists in that file

#### Scenario: An added scenario without a test is caught

- **WHEN** a capability gains a scenario whose behavior is implemented but has no backing test
- **THEN** running the coverage check fails and names the scenario, instead of the gap being
  discovered later by reading

### Requirement: The scenarios the suite cannot back are exempt and named

A scenario whose verification requires a running local stack — a live worker binary, several
replicas, or the Temporal UI — SHALL be exempt from the backing requirement while that stack does
not exist, and the exemption SHALL name what would verify it and what unblocks it. An exemption
SHALL NOT be used for a scenario a test could verify without that stack: this requirement exists to
record the delivery plan's gap, not to relabel an untested requirement as untestable.

#### Scenario: A stack-dependent scenario is exempt with its blocker named

- **WHEN** a scenario can only be verified against a running local stack
- **THEN** the exemption names the missing stack, what would verify the scenario once it exists,
  and the delivery work that provides it

#### Scenario: An exemption is refused for a scenario a test could verify

- **WHEN** a scenario is proposed as exempt although a test could produce its trigger and assert
  its outcome without a running stack
- **THEN** it is not exempt: it is backed by a test, and the coverage check fails until it is
