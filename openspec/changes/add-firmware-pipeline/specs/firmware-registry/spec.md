# Spec Delta

## Purpose

Defines the firmware catalog and its upload path: how an operator's firmware binary and metadata
become a stored firmware record — checksummed, validated for target-model compatibility and
version conflicts, kept in GridFS with a metadata-only document.

## ADDED Requirements

### Requirement: Firmware upload endpoint
The control plane SHALL serve `POST /api/firmwares`, accepting `multipart/form-data` with exactly
one binary part named `binary` and the metadata fields `version` (the firmware version string) and
`models` (the target device models, as a comma-separated list). A successful upload answers
`201 Created` with a JSON body carrying the firmware id, version, models, checksum, and size. A
request without the binary part, without `version`, or without `models` SHALL be answered
`400 Bad Request` and MUST store neither a binary nor a metadata record.

#### Scenario: Successful upload returns the firmware record
- **WHEN** an operator posts a firmware binary with `version` and `models` naming known device models
- **THEN** the endpoint answers `201 Created` with the firmware id, version, models, checksum,
  and size of the stored binary

#### Scenario: Malformed upload is rejected without side effects
- **WHEN** a request carries no binary part, an empty `version`, or no `models` field
- **THEN** the endpoint answers `400 Bad Request`, and no GridFS object and no firmware record exist

### Requirement: Checksum computed while streaming
The upload endpoint SHALL compute the checksum of the binary as the payload streams into storage
and record it on the firmware document. The checksum SHALL be the lowercase hexadecimal SHA-256
digest of the complete binary, and computing it MUST NOT require holding the whole binary in
memory at any point.

#### Scenario: Recorded checksum matches the stored binary
- **WHEN** a binary is uploaded and later read back from GridFS
- **THEN** the recorded checksum equals the SHA-256 digest of the reassembled content

#### Scenario: Uploads larger than any transfer buffer succeed
- **WHEN** a binary larger than the endpoint's transfer buffer is uploaded
- **THEN** it is stored in full and its checksum covers the complete content

### Requirement: Firmware binaries live in GridFS
The binary SHALL be stored in GridFS and referenced from the firmware document by its GridFS
object id; the metadata document MUST NOT carry the binary payload. If the metadata write fails
after the binary was stored, the stored binary SHALL be deleted so no orphaned GridFS object
outlives a rejected upload.

#### Scenario: Metadata references the GridFS object
- **WHEN** an upload completes
- **THEN** the firmware document carries the checksum and the GridFS object id of the stored binary,
  and the document itself contains no binary content

#### Scenario: Failed metadata write leaves no orphan
- **WHEN** the binary is stored but the firmware document cannot be written
- **THEN** the stored binary is deleted and no firmware document exists

### Requirement: Target model compatibility validation
An upload SHALL be accepted only when its `models` list is non-empty, contains no duplicates, and
names only device models the device registry knows — a model at least one registered device runs.
An upload failing this check SHALL be answered `422 Unprocessable Entity` with operator-safe
detail naming the offending model, and MUST store nothing.

#### Scenario: Firmware for known models is accepted
- **WHEN** every model in `models` is a model at least one registered device runs
- **THEN** the upload proceeds to storage

#### Scenario: Unknown target model is refused
- **WHEN** `models` names a model no registered device runs
- **THEN** the endpoint answers `422 Unprocessable Entity` naming that model, and neither a
  binary nor a firmware record is stored

#### Scenario: Empty model list is refused
- **WHEN** `models` is empty or lists the same model twice
- **THEN** the endpoint answers `422 Unprocessable Entity` and stores nothing

### Requirement: Version conflict rejection
An upload SHALL be accepted only when its `version` does not already identify a firmware record.
A conflicting version SHALL be answered `409 Conflict` and MUST store nothing, leaving the
existing record and its binary untouched. When two uploads of the same version race, at most one
firmware record SHALL exist afterwards.

#### Scenario: Re-uploading a version is refused
- **WHEN** an upload names a version that already exists in the firmware records
- **THEN** the endpoint answers `409 Conflict` and the existing record is unchanged

#### Scenario: Concurrent duplicate versions collapse to one record
- **WHEN** two uploads carrying the same version are processed concurrently
- **THEN** exactly one of them stores a firmware record and the other is answered `409 Conflict`
