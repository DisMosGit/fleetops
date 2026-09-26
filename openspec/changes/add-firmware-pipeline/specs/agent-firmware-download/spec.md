# Spec Delta

## Purpose

Defines the agent side of firmware delivery: receiving the chunk stream, assembling the binary on
disk, verifying its integrity, and reporting the download outcome back to the control plane.

## ADDED Requirements

### Requirement: Chunked download to disk
On a `StartUpdate` command the agent SHALL fetch the named firmware through the download stream
and write the arriving chunks, in order, to a disk file as they arrive — the binary MUST NOT be
assembled in memory. When the transfer ends and verification passes, the file SHALL take its
final name for the device to apply; a partial transfer file MUST NOT survive a failed download.

#### Scenario: Verified download lands on disk
- **WHEN** a firmware download completes and its content verifies
- **THEN** the binary exists on disk under its final name with exactly the transferred content

#### Scenario: Failed download leaves no partial file
- **WHEN** a download fails at any point — stream error, early end, or failed verification
- **THEN** no partial or final binary file for that firmware remains on disk

### Requirement: Download integrity verification
The agent SHALL verify the assembled binary before applying it: the checksum computed
incrementally while writing MUST match both the checksum on the download stream and the checksum
carried by the `StartUpdate` command, the received chunk offsets MUST form a contiguous range from
zero, and the transfer MUST end with its end-of-transfer marker. Any mismatch — wrong checksum,
disagreeing checksums, missing marker, or truncated stream — SHALL fail the download with an
operator-safe detail naming the reason.

#### Scenario: Checksum mismatch fails the download
- **WHEN** the assembled binary's SHA-256 digest differs from the checksum on the stream
- **THEN** the download fails with a detail naming the checksum mismatch, and nothing is applied

#### Scenario: Stream and command checksums must agree
- **WHEN** the checksum on the download stream differs from the checksum the `StartUpdate`
  command carried
- **THEN** the download fails and nothing is applied

#### Scenario: Truncated stream fails the download
- **WHEN** the stream ends without its end-of-transfer marker
- **THEN** the download fails and nothing is applied

### Requirement: Download outcome reporting
The agent SHALL report download progress and outcome back to the control plane as update status
for the device and firmware: the downloading phase while transfer proceeds, then the outcome —
continuing into the applying phase on success, or the failed phase with operator-safe detail on
failure. A failed download SHALL also conclude its command with a terminal failed report. Every
terminal report SHALL carry an idempotency key derived deterministically from the command id, so
a redelivery of the same result reuses the same key and never produces a second effect.

#### Scenario: Successful download reports progress and proceeds
- **WHEN** a download completes and verifies
- **THEN** the control plane has received downloading-phase status reports for the device and the
  update proceeds to application

#### Scenario: Failed download reports why and concludes the command
- **WHEN** a download fails verification
- **THEN** the control plane receives a failed-phase status report carrying the failure detail,
  and a terminal report concluding the command as failed

#### Scenario: Retried report reuses the idempotency key
- **WHEN** the same terminal result is reported again
- **THEN** both reports carry the same idempotency key derived from the command id
