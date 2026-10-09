# Spec Delta

## ADDED Requirements

### Requirement: Firmware metadata is addressable by version

The firmware registry SHALL resolve a firmware version to the metadata record that carries it, so a
caller that knows only a version — a device reporting the firmware it ran, or a rollback restoring a
device to it — can obtain the firmware's id, checksum, and target models. The lookup SHALL resolve to
the single record whose `version` matches, because version strings are unique across the registry, and
SHALL report a version the registry does not hold as not found rather than as an empty record.

#### Scenario: A claimed version resolves to its record

- **WHEN** the registry is asked for the metadata of a version it holds
- **THEN** it returns that record's firmware id, version, target models, and checksum

#### Scenario: An unclaimed version is not found

- **WHEN** the registry is asked for a version no firmware record carries
- **THEN** it reports the version as not found and returns no record

#### Scenario: Resolving a version reads metadata, not payloads

- **WHEN** a firmware version is resolved
- **THEN** the lookup returns the metadata record and reads no stored binary, which stays in GridFS
