# Spec Delta

## Purpose

Makes device workflow runs filterable in the Temporal UI by fleet dimensions — region, model,
firmware version, and online status — through custom search attributes that mirror the device
entity's state.

## ADDED Requirements

### Requirement: Device workflow runs carry fleet search attributes
Every device workflow run SHALL carry exactly these custom search attributes: `DeviceRegion`
(Keyword) holding the device's region, `DeviceModel` (Keyword) holding the device's model,
`DeviceFirmware` (Keyword) holding the firmware version the device runs, and `DeviceOnline`
(Bool) holding the device's liveness status. The attribute values SHALL be the corresponding
device workflow state values; an attribute whose state value is not yet known SHALL hold its
empty value until a signal supplies it. A Temporal UI (or CLI) list query over the workflow's
namespace SHALL be able to select device runs by region, model, firmware version, and online
status alone or in combination.

#### Scenario: Filtering devices in the Temporal UI
- **WHEN** an operator lists workflows with a query combining region, model, firmware version,
  and online status (for example region `eu-west`, model `sensor-2`, firmware `2.0.3`, online)
- **THEN** exactly the device runs whose current state matches all four conditions are returned

#### Scenario: Attribute values mirror device state
- **WHEN** a device workflow's state has region `eu-west`, model `sensor-2`, firmware `2.0.3`,
  and liveness status online
- **THEN** its run carries `DeviceRegion` `eu-west`, `DeviceModel` `sensor-2`, `DeviceFirmware`
  `2.0.3`, and `DeviceOnline` true

#### Scenario: Unknown attributes stay empty
- **WHEN** a device workflow exists whose signals have never supplied a region or model
- **THEN** `DeviceRegion` and `DeviceModel` hold empty values and the run is still listed

### Requirement: Search attributes track state changes
The workflow SHALL upsert the search attributes whenever a mirrored state value changes — the
adoption of region or model, a firmware version change, or a liveness status flip — so the
attributes at every point reflect the device's current state. A state change that does not touch
a mirrored value (for example a heartbeat that only refreshes the last-heartbeat timestamp)
SHALL NOT upsert search attributes.

#### Scenario: Firmware change updates the firmware attribute
- **WHEN** a device workflow adopts a new firmware version
- **THEN** `DeviceFirmware` becomes that version

#### Scenario: Going offline flips the online attribute
- **WHEN** a device workflow's liveness status flips to offline
- **THEN** `DeviceOnline` becomes false

#### Scenario: Returning to online flips the online attribute back
- **WHEN** a device workflow that is offline applies a heartbeat
- **THEN** `DeviceOnline` becomes true

### Requirement: Search attributes survive rolling continuations
A device workflow's search attributes SHALL reflect its current state across rolling
continuations: after the run chain rolls over to a fresh run, the same queries keep returning the
same devices.

#### Scenario: Filtering works after a rollover
- **WHEN** a device workflow rolls over to a fresh run and an operator filters by the device's
  region and model
- **THEN** the continuing run chain is still returned

### Requirement: Search attributes are registered before use
The four custom search attributes SHALL exist in the FleetOps namespace before any workflow
upserts them. Registration SHALL be idempotent: registering an attribute that already exists
changes nothing, and concurrent registration SHALL be safe for the registering processes.

#### Scenario: Fresh namespace gets the attributes
- **WHEN** the system starts against a namespace without custom search attributes
- **THEN** `DeviceRegion`, `DeviceModel`, `DeviceFirmware` (Keyword) and `DeviceOnline` (Bool)
  exist in the namespace before any device workflow upserts them

#### Scenario: Re-registration is a no-op
- **WHEN** registration runs against a namespace that already carries the attributes
- **THEN** startup proceeds and the attribute definitions are unchanged
