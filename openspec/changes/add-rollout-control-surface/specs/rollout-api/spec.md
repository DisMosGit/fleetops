# Spec Delta

## Purpose

The operator's HTTP surface over a rollout: starting one, reading its current state, and sending
the approve, pause, and resume commands — request and response shapes, status codes, and the
idempotency rule that keeps a retried start from creating a second rollout.

## ADDED Requirements

### Requirement: A rollout can be started over HTTP

`POST /api/rollouts` SHALL start one rollout from a JSON body naming the rollout id, the firmware
id, and the target selector (region and model). The rollout id SHALL be the request's idempotency
key: it determines the rollout's workflow identity, so a repeated start for a rollout that already
has a workflow execution SHALL be refused rather than starting a second run chain beside the first
one's records. The endpoint SHALL answer `202 Accepted` with the rollout id, the workflow id driving
it, and its status, and SHALL answer `400 Bad Request` for a body that is not valid JSON or leaves
the rollout id, firmware id, region, or model empty, and `409 Conflict` when that rollout id is
already in use. It SHALL NOT verify the firmware itself: a firmware that is unknown or does not
target the selector's model is the rollout's own recorded failure, not a start refusal.

#### Scenario: A start request begins a rollout

- **WHEN** a client posts a rollout id, firmware id, region, and model
- **THEN** the rollout's workflow execution is started under the derived workflow id and the
  response is `202` naming the rollout and workflow ids

#### Scenario: A repeated start is refused

- **WHEN** a client posts a start request for a rollout id that already has a workflow execution
- **THEN** the response is `409` and no second workflow execution exists for that rollout

#### Scenario: An incomplete request is rejected

- **WHEN** a start request omits the firmware id, leaves the region or model empty, or carries a
  body that is not valid JSON
- **THEN** the response is `400` and no workflow is started

#### Scenario: A rollout that cannot start is still accepted

- **WHEN** a start request names a firmware the registry does not hold, or one that does not target
  the selector's model
- **THEN** the request is accepted and the rollout records its own terminal failure

### Requirement: A rollout's current state can be read over HTTP

`GET /api/rollouts/{id}` SHALL answer `200 OK` with the rollout's authoritative state as its
workflow reports it: the rollout id, its status, the firmware it deploys, its target selector, the
configured waves with each wave's percentage, status, success rate, target count and device outcome
counts, which wave is current, whether an approval is outstanding, and — once it concluded — its
outcome with the wave that ended it and that wave's decision. The state SHALL be read from the
rollout's workflow execution, not from a stored projection, so a wave in flight is reported as it
stands. An id with no workflow execution SHALL answer `404 Not Found`; a state read that fails SHALL
answer `500 Internal Server Error` without echoing internal detail.

#### Scenario: A running rollout reports where it stands

- **WHEN** a client reads the state of a rollout whose second wave is inside its health window
- **THEN** the response names the first wave's recorded outcome and reports the second wave as the
  current one

#### Scenario: A concluded rollout reports why it ended

- **WHEN** a client reads the state of a rolled-back rollout
- **THEN** the response carries the terminal outcome, the wave that ended it, and that wave's
  measured decision

#### Scenario: An unknown rollout is not found

- **WHEN** a client reads the state of a rollout id with no workflow execution
- **THEN** the response is `404` and its body names no internal detail

### Requirement: Approve, pause and resume can be sent over HTTP

`POST /api/rollouts/{id}/approve`, `POST /api/rollouts/{id}/pause`, and
`POST /api/rollouts/{id}/resume` SHALL deliver the corresponding operator signal
(`approve_next_wave`, `pause_rollout`, `resume_rollout`) to the rollout's workflow execution and
answer `202 Accepted` naming the rollout and the signal delivered. An id with no workflow execution
SHALL answer `404 Not Found`, and a rollout that has already concluded SHALL answer `409 Conflict`
rather than accepting a command that could no longer change anything. A signal that cannot be
delivered SHALL answer `500 Internal Server Error` without echoing internal detail.

#### Scenario: An approval is delivered

- **WHEN** a client posts approve for a rollout awaiting approval
- **THEN** the response is `202` naming `approve_next_wave` and the rollout's next gated wave starts

#### Scenario: A pause and a resume are delivered

- **WHEN** a client posts pause for a running rollout and then resume for the same rollout
- **THEN** each response is `202` naming the signal delivered, the rollout reports `paused` after
  the pause, and it continues from where it stopped after the resume

#### Scenario: Commanding a concluded rollout is refused

- **WHEN** a client posts approve, pause, or resume for a rollout that has completed, rolled back,
  or failed
- **THEN** the response is `409` and the rollout's recorded state is unchanged

#### Scenario: Commanding an unknown rollout is not found

- **WHEN** a client posts approve, pause, or resume for a rollout id with no workflow execution
- **THEN** the response is `404`

### Requirement: The rollout surface shares the gateway listener

The rollout routes SHALL be served on the same HTTP listener as the firmware upload API, under the
`/api/rollouts` path, and SHALL reject a request whose method does not match the route (`405 Method
Not Allowed`) and a path that matches no route (`404 Not Found`). Every rejection SHALL be answered
with an operator-safe JSON body, and a failing request SHALL be logged at the boundary rather than
answered with driver or server internals.

#### Scenario: Both APIs answer on one listener

- **WHEN** the gateway serves a firmware upload and then a rollout start on the same address
- **THEN** both requests are handled by their own API and neither route shadows the other

#### Scenario: An unsupported method is rejected

- **WHEN** a client sends `GET /api/rollouts` or `DELETE /api/rollouts/{id}`
- **THEN** the response is `405` and no rollout is started, read, or commanded by it

#### Scenario: An unknown path is rejected

- **WHEN** a client sends a request under an unregistered path
- **THEN** the response is `404` with an operator-safe body
