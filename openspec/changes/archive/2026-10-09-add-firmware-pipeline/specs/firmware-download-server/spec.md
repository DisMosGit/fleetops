# Spec Delta

## Purpose

Defines how the control plane delivers a stored firmware binary to an agent: the server-streaming
download method, its bounded chunking, and its error surface.

## ADDED Requirements

### Requirement: Server-streaming firmware delivery
The control plane SHALL deliver a stored firmware binary through the server-streaming
`AgentService.DownloadFirmware` method, whose request names the device id and the firmware id.
The stream SHALL open with a metadata message carrying the firmware id, version, checksum, and
total size with no chunk; follow with the binary as ordered chunks whose offsets are contiguous
from zero and whose size never exceeds a fixed bound; and end with a message whose
end-of-transfer marker is set. Serving a download MUST NOT require holding the whole binary in
memory, whatever its size.

#### Scenario: Metadata precedes the chunk stream
- **WHEN** an agent streams a firmware id the control plane has
- **THEN** the first message carries the version, checksum, and total size before any chunk, and
  the last message sets the end-of-transfer marker

#### Scenario: Chunks are bounded and contiguous
- **WHEN** a binary of several chunks is streamed
- **THEN** every chunk is at most the fixed bound in size and chunk offsets form a contiguous
  range from zero to the total size

### Requirement: Download error surface
A download request naming a firmware id the control plane does not have SHALL fail with
`NOT_FOUND`, and a request missing the device id or firmware id SHALL fail with
`INVALID_ARGUMENT`; both messages MUST be operator-safe and free of implementation internals. A
failure after streaming began SHALL terminate the stream with a gRPC status error rather than a
truncated, silent end.

#### Scenario: Unknown firmware is refused safely
- **WHEN** an agent requests a firmware id the control plane does not have
- **THEN** the method fails with a `NOT_FOUND` status whose message carries no internal details

#### Scenario: Incomplete request is rejected
- **WHEN** a download request carries an empty device id or empty firmware id
- **THEN** the method fails with `INVALID_ARGUMENT`
