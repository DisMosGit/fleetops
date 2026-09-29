// Package temporal holds the FleetOps Temporal workflows and their activities. Today that is
// the long-lived per-device DeviceWorkflow entity — the authority for a device's firmware (the
// version it runs and the version it ran before it, which is what a rollback restores it to),
// its identity (region, model), heartbeat liveness, pending command, the last command it
// concluded, and configuration snapshot — its signal contracts, the SignalWithStart producers
// feeding the entity, and the RolloutWorkflow that owns a canary rollout from start to finish:
// it drives the configured wave sequence, resolves and records what each wave targets, commands
// every target device through one update-device activity per device and waits for that device's
// reported result within the configured result timeout, holds every wave open for its durable
// health window, promotes a healthy wave, and folds the operator's signals — approve_next_wave at
// a wave configured to require one, and pause_rollout and resume_rollout to hold a rollout and to
// continue it — at every wait.
//
// A regressing rollout rolls back as a saga. Entering rollback derives the plan of compensations
// from the rollout's own recorded progress, walking it in reverse, and reports the non-terminal
// status `rolling_back` while it runs: first the announcement that the rollback started, then one
// downgrade step per dispatched wave with the most recently dispatched compensated first — each
// restoring its devices through the per-device downgrade-device activity — then the
// reconcile-device-inventory activity, which brings the fleet's recorded firmware versions back in
// line with the devices, then the announcement that the compensations completed. No step aborts
// the plan: a step that fails permanently is recorded with what it did and did not achieve, and the
// terminal `rolled_back` status is written only once the plan has run, so a concluded rollback
// means the fleet is back rather than that someone intended it. Control signals change nothing once
// a rollout has begun compensating. Every step is idempotent — a restore is addressed by a command
// id derived from the rollback and the device, the reconciliation writes only a version a device
// reported and only where the record disagrees, and an announcement repeats its event identity — so
// an interrupted rollback is safe to continue. The FirmwareWorkflow is the stage still deferred.
//
// The activities run beside their side effects: dispatch-command (in the control plane, beside
// the agent hub) and snapshot-device-state plus the rollout's fleet-database, per-device update,
// downgrade, inventory-reconciliation, announcement, and health-evaluation activities (in the
// worker). Both workflows keep the outside world in step with their state: custom search
// attributes mirror a device for the Temporal UI's device filters and a rollout for its release
// filters, and the snapshot activity persists device state periodically and on every meaningful
// transition, while the rollout projects its progress — the rollback's plan and its steps
// included — into the rollouts and waves collections.
package temporal
