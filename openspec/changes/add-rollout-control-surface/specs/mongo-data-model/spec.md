# Spec Delta

## MODIFIED Requirements

### Requirement: Rollout records

The system SHALL store one document per rollout in the `rollouts` collection, identified by
`_id` = rollout id. Every rollout document MUST contain `firmware_id`, `status`, and
`temporal_wf_id` (the id of the workflow execution driving the rollout), and MUST record the
target group as `region` plus `model`. The document SHALL be written when the rollout workflow
starts and updated by that workflow as the rollout progresses; `status` SHALL be one of the
lifecycle values `running`, `paused`, `awaiting_approval`, `rolled_back`, `completed`, or `failed`,
where `rolled_back`, `completed`, and `failed` are terminal and are never left once recorded, and
`paused` is never terminal. Every write SHALL be idempotent: a repeated write of a state the
document already holds MUST leave it unchanged.

#### Scenario: Rollout record content

- **WHEN** a rollout workflow starts for a firmware and a target group
- **THEN** its `rollouts` document is retrievable by `_id` and carries `firmware_id`, `status`,
  `temporal_wf_id`, `region`, and `model`

#### Scenario: Rollout status transitions are recorded

- **WHEN** the rollout workflow advances a wave, waits for approval, is paused, is resumed, rolls
  back, completes, or fails
- **THEN** the `status` field of the existing rollout document reflects the new state

#### Scenario: A paused rollout is recorded as paused

- **WHEN** a running rollout is paused and later resumed
- **THEN** its document carries `paused` while it is paused and a non-terminal status again once it
  is resumed

#### Scenario: A rollout that could not start is recorded as failed

- **WHEN** a rollout workflow cannot resolve the firmware it was given, or the firmware does not
  target the selector's model
- **THEN** its rollout document carries the terminal status `failed`

#### Scenario: A terminal status is not overwritten

- **WHEN** a write attempts to move a rollout document out of `rolled_back`, `completed`, or
  `failed`
- **THEN** the stored status is unchanged

### Requirement: Wave records

The system SHALL store one document per wave in the `waves` collection, identified by `_id` =
wave id. Every wave document MUST contain its parent `rollout_id`, `percent` (the canary share of
the target group), `status`, `success_rate` (health measured over the wave's health window), the
target device set the wave was dispatched to (`device_ids`, an array of device identities that MAY
be empty), and `started_at` (the time the wave started, when its health window opened), and waves
of one rollout SHALL be retrievable in rollout order. Heartbeats recorded before a wave's
`started_at` are outside that wave's health window.

The document SHALL also record the wave's device outcomes once they have been collected: the
devices that reported a failed update (`failed_device_ids`) and the devices that never reported a
result before the wave stopped waiting (`unreported_device_ids`), each an array of device
identities that MAY be empty and that is written by every wave write. A document stored before
these fields existed MAY omit them.

The wave id SHALL be determined by the rollout and the wave's position in the configured sequence,
so that a re-resolved or retried wave write addresses the same document instead of creating a
second one. `status` SHALL be one of `dispatching` (membership recorded, update commands being
delivered), `evaluating` (every target has settled or run out of time, and the wave is inside its
health window or being re-measured), `skipped` (a share that added no device), `healthy`,
`unhealthy`, or `failed` (its update commands could not be delivered). `success_rate` SHALL be `0`
until the wave's health has been evaluated and SHALL then carry the evaluated success ratio; a
skipped wave is never evaluated and keeps `0`.

#### Scenario: Wave record content

- **WHEN** a rollout starts one of its canary waves (1%, 5%, 25%, or 100%)
- **THEN** a `waves` document exists with `rollout_id`, `percent`, `status`, a `success_rate` that
  is filled in once the health window has been evaluated, the target `device_ids`, `started_at`,
  and the `failed_device_ids` and `unreported_device_ids` sets

#### Scenario: Wave membership is the record of who was targeted

- **WHEN** a wave's target devices are resolved as the wave starts
- **THEN** the wave document records exactly those device identities, so the wave's health is
  evaluated over the devices it dispatched to rather than over a later re-resolution of the target
  group

#### Scenario: A wave records the devices that did not take the update

- **WHEN** a wave's devices have settled with failures and unreported devices among them
- **THEN** its document names those devices in `failed_device_ids` and `unreported_device_ids`, and
  both arrays are empty for a wave whose devices all succeeded

#### Scenario: A wave that targets no devices is still a record

- **WHEN** a wave resolves an empty target set (for example a canary share smaller than one device)
- **THEN** its document is stored with an empty `device_ids` array and a `started_at`, and its
  health evaluation reports an empty window rather than a healthy wave

#### Scenario: Wave status reflects the wave's life

- **WHEN** a wave is resolved, dispatched, evaluated, and decided
- **THEN** its document's status moves from `dispatching` to `evaluating` to `healthy`,
  `unhealthy`, or `failed`, or is recorded as `skipped` without being dispatched

#### Scenario: A retried wave write converges

- **WHEN** a wave write is retried after the wave's membership or status was already recorded
- **THEN** exactly one document exists for that wave id and its stored fields are the recorded ones

#### Scenario: Waves roll up to their rollout

- **WHEN** a consumer lists the waves of a rollout
- **THEN** exactly the waves belonging to that `rollout_id` are returned, in wave order

#### Scenario: Required fields enforced

- **WHEN** a write attempts to store a wave document missing `rollout_id`, `percent`, `status`,
  `success_rate`, `device_ids`, or `started_at`
- **THEN** the write is rejected and no partial document is stored
