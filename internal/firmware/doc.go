// Package firmware is the firmware registry: firmware binaries in GridFS and their
// metadata-only records in the firmware collection, plus the upload validation and HTTP
// upload endpoint an operator uses to add a firmware.
//
// The registry computes the checksum as the binary streams into storage, validates an
// upload's target device models against the device registry and its version against the
// existing records before anything is stored, and never leaves an orphaned GridFS object
// behind a rejected or partially failed upload.
//
// The upload's steps are also activity-callable here — validating the target models, storing the
// spooled binary addressed by version, writing each transition of the upload's record, and
// discarding what an upload that cannot complete its record stored — and the package owns the
// spool directory a request body waits in and the registry a workflow-backed upload endpoint
// records through.
package firmware
