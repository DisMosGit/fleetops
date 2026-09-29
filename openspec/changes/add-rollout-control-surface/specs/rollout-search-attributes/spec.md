# Spec Delta

## Purpose

Makes rollout runs filterable in the Temporal UI by the three dimensions an operator reasons about
a canary release with — the firmware version being deployed, the target region, and the rollout's
current status — through custom search attributes that mirror the rollout workflow's state.

## ADDED Requirements

### Requirement: Rollout runs carry release search attributes

Every rollout workflow run SHALL carry exactly these custom search attributes: `RolloutFirmware`
(Keyword) holding the version of the firmware the rollout deploys, `RolloutRegion` (Keyword)
holding the target selector's region, and `RolloutStatus` (Keyword) holding the rollout's current
lifecycle status. The attribute values SHALL be the corresponding rollout state values; an
attribute whose state value is not yet known SHALL hold its empty value until the rollout knows it
— in particular `RolloutFirmware` is empty until the firmware's metadata has been loaded. A Temporal
UI (or CLI) list query over the workflow's namespace SHALL be able to select rollout runs by
firmware version, region, and status alone or in combination.

#### Scenario: Filtering rollouts in the Temporal UI

- **WHEN** an operator lists workflows with a query combining firmware version, region, and status
  (for example firmware `2.0.0`, region `eu-west`, status `running`)
- **THEN** exactly the rollout runs whose current state matches all three conditions are returned

#### Scenario: Attribute values mirror rollout state

- **WHEN** a rollout deploys firmware version `2.0.0` to region `eu-west` and is driving its waves
- **THEN** its run carries `RolloutFirmware` `2.0.0`, `RolloutRegion` `eu-west`, and
  `RolloutStatus` `running`

#### Scenario: An unknown firmware version stays empty

- **WHEN** a rollout run has not yet loaded the metadata of the firmware it deploys
- **THEN** `RolloutFirmware` holds an empty value and the run is still listed

### Requirement: Search attributes track rollout state changes

The workflow SHALL upsert the search attributes whenever a mirrored state value changes — the
firmware version becoming known, and every status the rollout moves through (running, awaiting
approval, paused, and the terminal statuses) — so the attributes at every point reflect the
rollout's current state. A state change that does not touch a mirrored value SHALL NOT upsert
search attributes.

#### Scenario: Loading the firmware fills the firmware attribute

- **WHEN** a rollout loads the metadata of the firmware it deploys
- **THEN** `RolloutFirmware` becomes that firmware's version

#### Scenario: Waiting for approval changes the status attribute

- **WHEN** a rollout holds at a wave that requires approval
- **THEN** `RolloutStatus` becomes `awaiting_approval`

#### Scenario: Pausing changes the status attribute

- **WHEN** a rollout is paused
- **THEN** `RolloutStatus` becomes `paused` and becomes `running` again when it is resumed

#### Scenario: Concluding changes the status attribute

- **WHEN** a rollout completes, rolls back, or fails
- **THEN** `RolloutStatus` becomes that terminal status and does not change again

### Requirement: Search attributes are registered before use

The three custom rollout search attributes SHALL exist in the FleetOps namespace before any rollout
workflow upserts them. Registration SHALL be idempotent: registering an attribute that already
exists changes nothing, and concurrent registration SHALL be safe for the registering processes.

#### Scenario: Fresh namespace gets the attributes

- **WHEN** the system starts against a namespace without custom search attributes
- **THEN** `RolloutFirmware`, `RolloutRegion`, and `RolloutStatus` (Keyword) exist in the namespace
  before any rollout workflow upserts them

#### Scenario: Re-registration is a no-op

- **WHEN** registration runs against a namespace that already carries the attributes
- **THEN** startup proceeds and the attribute definitions are unchanged
