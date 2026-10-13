# Spec Delta

## Purpose

Keeps the fleet's eligible-device view fresh from MongoDB change streams: what a device change does
to the view, how the view is derived from the `devices` collection that remains its source of
truth, how a lost stream is resumed or rebuilt, and what a rollout's wave resolution reads instead
of recomputing.

## ADDED Requirements

### Requirement: Device changes update the eligible-device view without polling

The system SHALL maintain a view of the eligible devices per selector — a selector being a region
and a device model — that reflects the `devices` collection as it changes: a device that becomes
eligible appears in its selector's view, a device whose region or model changes leaves the view it
was in and enters the one it now belongs to, and a device that stops satisfying a selector's
matching rule leaves that selector's view. The view SHALL be kept current from MongoDB change
streams over the `devices` collection, so that freshness comes from the fleet's own writes and
MUST NOT require repeated queries over the collection to discover what changed. The `devices`
collection SHALL remain the source of truth: the view is derived, carries no state of its own that
cannot be recomputed from `devices`, and losing it SHALL NOT lose fleet information.

#### Scenario: A new device enters its selector's view

- **WHEN** a device that matches a selector becomes present in the `devices` collection
- **THEN** that device is part of that selector's eligible view without any query over the
  `devices` collection being made to discover the change

#### Scenario: A device that changes model moves between views

- **WHEN** a device's recorded model changes from one to another
- **THEN** it is part of the view of the selector it now matches and no longer part of the view of
  the selector it matched before

#### Scenario: A device that leaves the fleet leaves the view

- **WHEN** a device's record is removed from the `devices` collection
- **THEN** it is no longer part of any selector's eligible view

#### Scenario: A change in a later wave does not move an earlier wave's membership

- **WHEN** a device becomes eligible for a selector while a rollout over that selector has a wave
  whose membership was already resolved and recorded
- **THEN** the view reflects the new device and that wave's recorded target set is unchanged

### Requirement: The view is derived at startup and converges on the collection

The view SHALL be derived from the `devices` collection when the component that maintains it starts,
and every change stream update SHALL move the view toward the state the collection describes, so
that the view after any sequence of updates is the view a full derivation would produce. Updating
the view SHALL be idempotent: applying the same change more than once SHALL leave the view
unchanged rather than duplicating an entry or reverting a later change. A stream event that can no
longer be applied meaningfully — an update for a device the view has already moved past, or a
removal for a device the view does not hold — SHALL be a no-op rather than an error.

#### Scenario: Startup derivation matches a full read

- **WHEN** the view is established at startup over a collection holding devices of several regions
  and models
- **THEN** each selector's view holds exactly the devices a full read of the collection would
  return for it

#### Scenario: A redelivered change does not duplicate an entry

- **WHEN** the same device change is applied to the view twice
- **THEN** the device appears once in its selector's view and the view is otherwise unchanged

### Requirement: A lost change stream resumes, and rebuilds when it cannot

The system SHALL resume a lost change stream from the position it had reached rather than from the
beginning, and when the stream cannot be resumed — its position is no longer held by the server, or
the stream cannot be re-established at all — it SHALL rebuild the view from the `devices`
collection and then continue from the new stream. A stream that was interrupted SHALL NOT leave the
view serving data older than the collection without the rebuild having been attempted, and a
watcher SHALL stop with the process it runs in: its lifetime is bounded by the context it is
started with, and it SHALL release its connection and its cursor when that context ends.

#### Scenario: Reconnection resumes from the last position

- **WHEN** a change stream is interrupted and re-established
- **THEN** it resumes from the position it had reached, and changes made while it was down are not
  lost from the view

#### Scenario: An unrecoverable position triggers a rebuild

- **WHEN** a change stream cannot be resumed because its position is no longer available
- **THEN** the view is rebuilt from the `devices` collection and the stream continues from a fresh
  position, and the rebuilt view holds exactly what a full read would return

#### Scenario: Shutdown releases the stream

- **WHEN** the context the watcher was started with is cancelled while a stream is open
- **THEN** the watcher returns, closing its cursor and its connection, and does not leave a
  goroutine watching the collection

### Requirement: Wave resolution reads the maintained view

A rollout's wave resolution SHALL take the eligible pool for its selector from the maintained view
rather than recomputing it by a query over the `devices` collection on every resolution. The
resolution's existing guarantees SHALL be unaffected: the pool SHALL still be ordered by device
identity so the same pool and shares resolve to the same target sets, a wave's membership SHALL
still be resolved once and recorded before dispatch, and a retried resolution SHALL still converge
on the recorded membership. A selector the view holds no entry for SHALL resolve to an empty pool
rather than to an error, matching how a selector no device matches resolves today.

#### Scenario: A wave resolves against the maintained view

- **WHEN** a wave of a rollout is resolved for a selector the view holds devices for
- **THEN** the wave's target set is the same set the ordered pool and the wave's share imply,
  derived from the view rather than from a fresh query over `devices`

#### Scenario: An empty selector still records an empty wave

- **WHEN** a wave's selector matches no device in the view
- **THEN** the wave is recorded with an empty target set, is not dispatched, and the rollout
  advances exactly as it does today
