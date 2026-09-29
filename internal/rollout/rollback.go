package rollout

// RollbackStepKind names what one compensating step of a rollback does. The vocabulary is the
// workflow's own: the plan it derives is a sequence of these, and the rollout document records them
// in plan order.
type RollbackStepKind string

// The kinds a rollback's plan is built from.
const (
	// RollbackNotifyStarted announces the rollback before any compensation runs.
	RollbackNotifyStarted RollbackStepKind = "notify_started"
	// RollbackDowngrade restores one dispatched wave's devices to the firmware they ran before,
	// compensating that wave's update.
	RollbackDowngrade RollbackStepKind = "downgrade"
	// RollbackReconcileInventory reconciles the fleet's recorded firmware versions against the
	// devices the rollback touched, compensating the bookkeeping the rollout advanced.
	RollbackReconcileInventory RollbackStepKind = "reconcile_inventory"
	// RollbackNotifyCompleted announces that the compensations have run.
	RollbackNotifyCompleted RollbackStepKind = "notify_completed"
)

// RollbackStepStatus is where one compensating step stands.
type RollbackStepStatus string

// The statuses a rollback step moves through. It is never left once it is completed or failed: a
// step is recorded once and the plan moves on.
const (
	// RollbackStepPending is a step the plan has not reached yet.
	RollbackStepPending RollbackStepStatus = "pending"
	// RollbackStepRunning is the step the plan is executing.
	RollbackStepRunning RollbackStepStatus = "running"
	// RollbackStepCompleted is a step that did its own work. What it achieved is in its counts:
	// a downgrade step that could not restore every device is completed, with those devices
	// recorded as unrestored and listed on the rollback record.
	RollbackStepCompleted RollbackStepStatus = "completed"
	// RollbackStepFailed is a step whose own work failed and could not be retried away — an
	// announcement the broker never took. It does not abort the plan: the step is recorded with
	// its failure and the remaining steps still run.
	RollbackStepFailed RollbackStepStatus = "failed"
)

// RollbackStepRecord is one compensating step as the rollout document carries it: what it
// compensates and what it achieved.
type RollbackStepRecord struct {
	// Kind is what the step does.
	Kind RollbackStepKind `bson:"kind"`
	// WaveID is the wave a compensating step compensates; empty for a step that compensates the
	// fleet's bookkeeping or announces the rollback.
	WaveID string `bson:"wave_id,omitempty"`
	// Status is where the step stands.
	Status RollbackStepStatus `bson:"status"`
	// Devices is how many devices the step targeted; zero for a step that targets none.
	Devices int `bson:"devices"`
	// Restored, Failed, Unreported, Skipped, and Unavailable are the outcomes of a downgrade
	// step's devices.
	Restored    int `bson:"restored"`
	Failed      int `bson:"failed"`
	Unreported  int `bson:"unreported"`
	Skipped     int `bson:"skipped"`
	Unavailable int `bson:"unavailable"`
	// Agreed, Corrected, and Unverified are the outcomes of a reconciliation step's devices.
	Agreed     int `bson:"agreed"`
	Corrected  int `bson:"corrected"`
	Unverified int `bson:"unverified"`
	// Detail is why a step failed; empty otherwise.
	Detail string `bson:"detail,omitempty"`
}

// FirmwareInventoryRecord is one firmware version's share of the inventory a rollback reconciled:
// how many of the devices it touched were found on it.
type FirmwareInventoryRecord struct {
	// Version is the firmware version devices were found on.
	Version string `bson:"version"`
	// Devices is how many devices were found on it.
	Devices int `bson:"devices"`
}

// document returns the record as it is stored: its arrays are written as arrays rather than null, so
// a record whose slices are nil still satisfies the collection's validator and a reader never has to
// tell "nothing to report" apart from "not collected".
func (r RollbackRecord) document() RollbackRecord {
	out := r
	if out.Steps == nil {
		out.Steps = []RollbackStepRecord{}
	}
	if out.Inventory == nil {
		out.Inventory = []FirmwareInventoryRecord{}
	}
	if out.UnrestoredDeviceIDs == nil {
		out.UnrestoredDeviceIDs = []string{}
	}
	return out
}

// RollbackRecord is the rollback a rollout document carries: why it happened, the plan's steps with
// what each achieved, the inventory the reconciliation established, and the devices the rollback
// could not restore.
type RollbackRecord struct {
	// Outcome is the outcome that ended the rollout — why the compensations ran. It carries the
	// workflow's outcome vocabulary (unhealthy_wave, decision_timeout, dispatch_failed), which is
	// recorded as a plain string so the document does not depend on the workflow's types.
	Outcome string `bson:"outcome"`
	// Steps are the plan's steps in plan order.
	Steps []RollbackStepRecord `bson:"steps"`
	// Inventory is the reconciled inventory, ordered by version.
	Inventory []FirmwareInventoryRecord `bson:"inventory"`
	// UnrestoredDeviceIDs are the devices the rollback could not restore: the ones that reported
	// a failed restore, never reported, or could not be restored at all. A device that had
	// nothing to restore is not listed — nothing was left to put back.
	UnrestoredDeviceIDs []string `bson:"unrestored_device_ids"`
}
