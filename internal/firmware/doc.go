// Package firmware is the firmware registry: firmware binaries in GridFS and their
// metadata-only records in the firmware collection, plus the upload validation and HTTP
// upload endpoint an operator uses to add a firmware.
//
// The registry computes the checksum as the binary streams into storage, validates an
// upload's target device models against the device registry and its version against the
// existing records before anything is stored, and never leaves an orphaned GridFS object
// behind a rejected or partially failed upload.
package firmware
