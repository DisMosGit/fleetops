# Spec Delta

## Purpose

Owns one firmware upload end to end: a workflow execution per firmware version drives the upload's
steps, records the upload's state as it progresses, and answers a state query, so a retried upload
converges on one firmware record and an operator learns the outcome from a queryable record rather
than from a request that has to stay open.

## ADDED Requirements

### Requirement: One firmware workflow owns an upload from acceptance to availability

Each firmware upload SHALL be driven by exactly one workflow execution, addressed by a stable
workflow identity derived from the firmware version it stores, which owns the upload's whole life:
the steps it runs, the state it records, and its terminal outcome. Two uploads naming the same
version SHALL converge on that one identity — the second SHALL reach the existing execution rather
than start a second one — so a client that retries an upload after a lost response does not create
a second upload. The workflow SHALL conclude in exactly one terminal state: available (the binary
and its metadata record exist and agree) or failed (the upload could not be completed), and SHALL
perform no further step after reaching one.

#### Scenario: Starting an upload records it as accepted

- **WHEN** a firmware upload is accepted for a version and a binary
- **THEN** exactly one workflow execution exists for that version, and the firmware record carries
  the state the upload has reached

#### Scenario: A retried upload reaches the same execution

- **WHEN** an upload for a version whose execution already exists is submitted again
- **THEN** no second execution is started for that version and the existing execution's outcome is
  authoritative

#### Scenario: A concluded upload is terminal

- **WHEN** an upload has reached available or failed
- **THEN** it runs no further step and its recorded state does not change again

### Requirement: The upload's steps run as activities

The work an upload performs SHALL run as activities, each taking a context, being idempotent under
retry, and receiving its retry policy from the workflow that schedules it: validating that the
named models are models the device registry knows, computing the binary's checksum as it streams
into storage, storing the binary in GridFS, and writing the metadata record that references it. The
workflow SHALL NOT perform I/O of its own, and every activity SHALL be individually retryable
without repeating another activity's side effect: a retried storage step for a binary already
stored SHALL NOT leave a second GridFS object, and a retried metadata write SHALL NOT create a
second firmware record.

#### Scenario: Validation precedes storage

- **WHEN** an upload names a model no registered device runs
- **THEN** the upload concludes as failed naming that model, no binary is stored in GridFS, and no
  firmware record exists

#### Scenario: A retried storage step stores one binary

- **WHEN** the activity that stores the binary is retried after its first attempt succeeded
- **THEN** exactly one GridFS object exists for that upload

#### Scenario: A version conflict fails the upload without touching the existing record

- **WHEN** an upload names a version that already identifies a firmware record written by another
  upload
- **THEN** the upload concludes as failed with the conflict as its reason, and the existing record
  and its binary are unchanged

#### Scenario: A failed metadata write leaves no orphaned binary

- **WHEN** the binary was stored but the metadata record cannot be written within the upload's
  retry budget
- **THEN** the stored binary is removed, no firmware record exists, and the upload concludes as
  failed

### Requirement: The upload's state is recorded on the firmware document

The firmware document SHALL carry the upload's state as it progresses, one of accepted, storing,
available, or failed, together with the id of the workflow execution that owns it and, once the
upload has concluded without availability, an operator-safe reason. A state transition SHALL be
written idempotently — writing the same transition twice SHALL leave the stored record unchanged —
and a record whose upload is still in progress SHALL NOT be indistinguishable from one whose upload
has concluded.

#### Scenario: State transitions are recorded

- **WHEN** an upload is accepted, stores its binary, and becomes available
- **THEN** the firmware document carries each state as the upload reaches it, and finally available

#### Scenario: A failed upload carries its reason

- **WHEN** an upload concludes as failed
- **THEN** its firmware document carries the failed state and an operator-safe reason that names
  what could not be done, without exposing internals

#### Scenario: A retried transition does not regress the record

- **WHEN** a state transition already recorded is written again
- **THEN** the stored firmware document is unchanged

### Requirement: An upload's outcome is available as a state query

The workflow SHALL serve a state query that reports, without blocking the upload: the firmware
version, the upload's current state, whether it has concluded, the firmware id and checksum once
they are known, and the reason when it failed. A caller SHALL be able to learn an upload's outcome
after the request that started it has ended, and a query against an upload still in progress SHALL
answer with the state reached so far rather than waiting for it to conclude.

#### Scenario: A running upload reports its progress

- **WHEN** the state query is evaluated while the upload is storing its binary
- **THEN** it reports the storing state and no firmware id or checksum yet, without blocking

#### Scenario: A concluded upload reports its outcome

- **WHEN** the state query is evaluated after the upload became available
- **THEN** it reports the available state with the firmware id and the checksum of the stored
  binary

#### Scenario: A failed upload reports why

- **WHEN** the state query is evaluated after an upload failed its model validation
- **THEN** it reports the failed state and the model that was refused
