# Spec Delta

## MODIFIED Requirements

### Requirement: Firmware download exchange
The contract SHALL define `FirmwareDownloadRequest` and `FirmwareDownloadResponse` messages and
expose them as `AgentService.DownloadFirmware`, a server-streaming RPC the agent issues after a
`StartUpdate` command. The request SHALL name the device id and the firmware id; the response
SHALL carry the firmware id, `version`, `checksum`, the total size of the binary, and the binary
as bounded-size chunks with an offset and end-of-transfer marker, so a single download never
requires unbounded buffering. The checksum and total size MUST be present independently of the
chunk stream — on the stream's opening message — so the agent can verify the assembled binary.
This evolution SHALL stay additive: `total_size` takes a new field number on
`FirmwareDownloadResponse`, `DownloadFirmware` takes no field number, and every existing field
keeps its number and type.

#### Scenario: Firmware is fetched in bounded chunks
- **WHEN** an agent streams a firmware id over `DownloadFirmware`
- **THEN** it receives the firmware version, checksum, and total size first, then chunked binary
  data with offsets, ending with an explicit end-of-transfer marker

#### Scenario: Unknown firmware is refused safely
- **WHEN** an agent requests a firmware id the control plane does not have
- **THEN** the exchange fails with a `NOT_FOUND` gRPC status whose message carries no internal
  details

#### Scenario: Adding the field and RPC keeps old clients working
- **WHEN** a client generated before this evolution calls the updated server
- **THEN** its existing exchanges keep working unchanged on the wire
