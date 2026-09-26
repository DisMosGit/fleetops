# Spec Delta

## MODIFIED Requirements

### Requirement: Firmware metadata records
The system SHALL store one document per firmware in a firmware metadata collection, identified by
`_id` = firmware id. Every firmware document MUST contain `version`, a non-empty `models` list
naming the device models the firmware targets, `checksum`, `size`, `created_at`, and `gridfs_id`
referencing the binary stored in GridFS, and MUST NOT contain the binary payload itself. A
firmware document whose `gridfs_id` does not resolve to a GridFS object MUST be rejected at write
time.

#### Scenario: Firmware metadata with binary reference
- **WHEN** an operator uploads a firmware binary
- **THEN** the binary lands in GridFS and the metadata document records `version`, `models`,
  `checksum`, `size`, `created_at`, and the `gridfs_id` of the stored object

#### Scenario: Metadata never holds payloads
- **WHEN** a firmware document is read
- **THEN** it exposes `version`, `models`, `checksum`, `size`, `created_at`, and `gridfs_id` but no
  inline binary content

#### Scenario: Target models are recorded
- **WHEN** a firmware record is read after an upload declaring its target device models
- **THEN** the record's `models` list is exactly the declared target models
