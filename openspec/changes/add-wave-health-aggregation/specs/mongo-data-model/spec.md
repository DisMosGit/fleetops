# Spec Delta

## MODIFIED Requirements

### Requirement: Wave records
The system SHALL store one document per wave in the `waves` collection, identified by `_id` =
wave id. Every wave document MUST contain its parent `rollout_id`, `percent` (the canary share of
the target group), `status`, `success_rate` (health measured over the wave's health window), the
target device set the wave was dispatched to (`device_ids`, an array of device identities that MAY
be empty), and `started_at` (the time the wave started, when its health window opened), and waves
of one rollout SHALL be retrievable in rollout order. Heartbeats recorded before a wave's
`started_at` are outside that wave's health window.

#### Scenario: Wave record content
- **WHEN** a rollout starts one of its canary waves (1%, 5%, 25%, or 100%)
- **THEN** a `waves` document exists with `rollout_id`, `percent`, `status`, a `success_rate` that
  is filled in once the health window has been evaluated, the target `device_ids`, and `started_at`

#### Scenario: Wave membership is the record of who was targeted
- **WHEN** a wave's target devices are resolved as the wave starts
- **THEN** the wave document records exactly those device identities, so the wave's health is
  evaluated over the devices it dispatched to rather than over a later re-resolution of the target
  group

#### Scenario: A wave that targets no devices is still a record
- **WHEN** a wave resolves an empty target set (for example a canary share smaller than one device)
- **THEN** its document is stored with an empty `device_ids` array and a `started_at`, and its
  health evaluation reports an empty window rather than a healthy wave

#### Scenario: Waves roll up to their rollout
- **WHEN** a consumer lists the waves of a rollout
- **THEN** exactly the waves belonging to that `rollout_id` are returned, in wave order

#### Scenario: Required fields enforced
- **WHEN** a write attempts to store a wave document missing `rollout_id`, `percent`, `status`,
  `success_rate`, `device_ids`, or `started_at`
- **THEN** the write is rejected and no partial document is stored
