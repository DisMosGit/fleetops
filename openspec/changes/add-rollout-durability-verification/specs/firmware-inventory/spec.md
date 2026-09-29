# Spec Delta

## ADDED Requirements

### Requirement: After a rollback the fleet's records match what the devices run

Once a rollback has run, the fleet's recorded firmware version for every device the rollback touched
SHALL be the version that device's workflow — the authority on its own firmware — holds at that
moment: a device whose restore concluded successfully SHALL be recorded on the version it was
restored to, and a device whose restore failed, was never reported, or could not be attempted SHALL
be recorded on the version it still runs rather than on the version the rollback intended for it. A
device whose version could not be established SHALL be reported as unverified with the reason rather
than recorded on either version. The reconciled inventory SHALL account for every device the
rollback touched exactly once, and SHALL be readable from the rollout's record and from the
completion announcement once the rollback has concluded.

#### Scenario: A restored device is recorded on the version it runs

- **WHEN** a device's restore concluded successfully
- **THEN** the fleet records that device on the version it was restored to, which is the version the
  device's workflow holds

#### Scenario: A device the rollback could not restore is recorded on what it still runs

- **WHEN** a device's restore failed, was never reported, or could not be attempted
- **THEN** the fleet records that device on the firmware it still runs, and the rollout's record
  counts it among the devices the rollback did not restore

#### Scenario: The records agree with the devices when a rollback concludes

- **WHEN** a rollback has concluded
- **THEN** every device it touched is either recorded on the version its workflow holds or reported
  as unverified with the reason, and no device is recorded on a version it does not run
