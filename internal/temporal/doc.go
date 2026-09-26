// Package temporal holds the FleetOps Temporal workflows and their activities. Today that is
// the long-lived per-device DeviceWorkflow entity — the authority for a device's firmware,
// identity (region, model), heartbeat liveness, pending command, and configuration snapshot —
// its signal contracts, the SignalWithStart producers feeding the entity, and the two
// activities run beside their side effects: dispatch-command (in the control plane, beside
// the agent hub) and snapshot-device-state (in the worker, projecting entity state into the
// fleet database). The entity keeps the outside world in step with its state: custom search
// attributes mirror it for the Temporal UI's filters, and the snapshot activity persists it
// periodically and on every meaningful transition. The RolloutWorkflow canary saga (waves
// with durable health windows and compensating rollback) and the FirmwareWorkflow join at
// later stages.
package temporal
