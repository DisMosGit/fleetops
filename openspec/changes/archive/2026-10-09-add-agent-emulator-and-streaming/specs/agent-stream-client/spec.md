# Spec Delta

## Purpose

Defines the agent side of the `AgentService.Connect` bidirectional stream: how the emulator
opens and registers on the stream, sends heartbeats under backpressure, keeps the stream alive,
reconnects with capped exponential backoff after failure, and preserves every message across a
reconnect.

## ADDED Requirements

### Requirement: Stream establishment and registration
The agent SHALL open one `AgentService.Connect` stream to the control plane and register every
simulated device on it before that device's heartbeats flow. Each registration request SHALL
carry a correlation id the response is matched on. When a stream is lost and re-established, the
agent SHALL register every device again on the new stream before resuming heartbeat delivery.

#### Scenario: Devices register before heartbeating
- **WHEN** the agent opens its stream
- **THEN** every simulated device is registered on it, each request matched to its response by
  correlation id, before any heartbeat of that device is sent

#### Scenario: Reconnect re-registers before resuming
- **WHEN** the stream is re-established after a failure
- **THEN** every device is registered on the new stream and only then do queued heartbeats flow

### Requirement: Heartbeat delivery with backpressure
Heartbeats SHALL travel to the stream through a bounded outbound queue. When the queue is full
the producing device SHALL be slowed rather than the queue growing: unbounded buffering is
forbidden. A message SHALL leave the queue only after its stream send attempt has succeeded.

#### Scenario: A full queue slows producers instead of growing
- **WHEN** heartbeats are produced faster than the stream accepts them and the queue reaches its
  bound
- **THEN** producers wait for queue space and the queue never exceeds its bound

#### Scenario: Only sent messages leave the queue
- **WHEN** a stream send attempt fails
- **THEN** the message remains queued for delivery on the next stream

### Requirement: Stream keepalive
The agent SHALL keep its stream detectably alive with gRPC keepalive, so a silently dead
connection is discovered and treated as a stream failure instead of stalling heartbeat delivery
invisibly.

#### Scenario: A dead connection becomes a reconnect
- **WHEN** the connection dies without a clean stream error (keepalive timeout)
- **THEN** the agent treats the stream as failed and starts its reconnect cycle

### Requirement: Reconnect with capped exponential backoff
On stream failure the agent SHALL retry the connection with exponential backoff and jitter,
starting from an initial delay and doubling up to a fixed maximum delay, until the connection is
re-established or the agent context is cancelled. A successful connection SHALL reset the
backoff. The agent SHALL never hot-loop reconnect attempts.

#### Scenario: Backoff grows and is capped
- **WHEN** consecutive connection attempts fail
- **THEN** successive delays grow exponentially with jitter and never exceed the configured
  maximum

#### Scenario: Success resets the backoff
- **WHEN** a connection attempt succeeds after several failures
- **THEN** the next reconnect cycle starts again from the initial delay

#### Scenario: Reconnect attempts stop at shutdown
- **WHEN** the agent context is cancelled while the stream is down
- **THEN** no further connection attempts are made

### Requirement: No message loss across reconnects
A stream failure SHALL lose no message the agent has accepted for delivery: unsent messages stay
in the outbound queue and are delivered on the new stream after re-registration, and a request
that was sent but never answered is retried on the new stream under its original correlation id,
so its response still matches. Redelivery SHALL be safe: heartbeats keep their original
`event_id`, and every duplicate the control plane receives is identifiable as such.

#### Scenario: Unsent heartbeats are delivered after reconnect
- **WHEN** heartbeats are queued while the stream is down
- **THEN** after reconnection and re-registration they are sent on the new stream with their
  original event ids

#### Scenario: Unanswered requests are retried under their correlation id
- **WHEN** a request was sent on a stream that broke before its response arrived
- **THEN** it is retried on the new stream with the same correlation id and matched to the
  response that answers it

#### Scenario: Duplicates are always identifiable
- **WHEN** the reconnect cycle causes a message to be delivered twice
- **THEN** both deliveries carry the same event id or correlation id, so the receiving side can
  treat the second as a no-op
