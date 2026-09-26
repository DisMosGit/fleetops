// Package temporal holds the FleetOps Temporal workflows and their activities. Today that is
// the long-lived per-device DeviceWorkflow entity — the authority for a device's firmware,
// heartbeat liveness, pending command, and configuration snapshot — its signal contracts, the
// dispatch-command activity, and the SignalWithStart producers feeding the entity. The
// RolloutWorkflow canary saga (waves with durable health windows and compensating rollback)
// and the FirmwareWorkflow join at later stages.
package temporal
