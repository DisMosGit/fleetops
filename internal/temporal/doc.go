// Package temporal holds the FleetOps Temporal workflows and their activities. Today that is
// the long-lived per-device DeviceWorkflow entity — the authority for a device's firmware,
// identity (region, model), heartbeat liveness, pending command, and configuration snapshot —
// its signal contracts, the SignalWithStart producers feeding the entity, and the RolloutWorkflow
// that owns a canary rollout from start to finish: it drives the configured wave sequence,
// resolves and records what each wave targets, dispatches the firmware update through the device
// command seam, holds every wave open for its durable health window, promotes a healthy wave,
// transitions a regressing rollout into rollback, and waits for an operator's approval at a wave
// configured to require one. Rollback here is the decision and the recorded transition;
// its compensating steps (downgrade, inventory, notification) are the saga's later stage, as are
// the FirmwareWorkflow and the rollout HTTP surface.
//
// The activities run beside their side effects: dispatch-command (in the control plane, beside
// the agent hub) and snapshot-device-state plus the rollout's fleet-database, device-signal, and
// health-evaluation activities (in the worker). Both workflows keep the outside world in step with
// their state: custom search attributes mirror a device for the Temporal UI's filters, and the
// snapshot activity persists device state periodically and on every meaningful transition, while
// the rollout projects its progress into the rollouts and waves collections.
package temporal
