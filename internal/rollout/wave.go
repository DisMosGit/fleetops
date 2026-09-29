package rollout

import (
	"fmt"
	"time"
)

// RolloutStatus is where a rollout is in its life. The three terminal statuses are never left
// once recorded.
type RolloutStatus string

// The rollout lifecycle statuses a rollout document records.
const (
	// RolloutRunning is a rollout driving its sequence: waiting on a gate, dispatching, or
	// measuring a wave.
	RolloutRunning RolloutStatus = "running"
	// RolloutPaused is a rollout an operator held: it starts no further wave until it is
	// resumed. A wave already in flight is still driven to its decision and can still roll the
	// rollout back, so a pause holds promotion and never safety. It is never terminal.
	RolloutPaused RolloutStatus = "paused"
	// RolloutAwaitingApproval is a rollout holding at a wave that requires an operator approval.
	RolloutAwaitingApproval RolloutStatus = "awaiting_approval"
	// RolloutRollingBack is a rollout that has stopped deciding and is running the compensations
	// its rollback derived from the wave that failed it. It is never terminal: the terminal
	// status `rolled_back` is recorded only once every compensating step has run, so "rolled
	// back" means the fleet is back rather than that someone intended it.
	RolloutRollingBack RolloutStatus = "rolling_back"
	// RolloutRolledBack is a concluded rollout that stopped on a wave which failed its gate.
	RolloutRolledBack RolloutStatus = "rolled_back"
	// RolloutCompleted is a concluded rollout whose whole sequence was promoted.
	RolloutCompleted RolloutStatus = "completed"
	// RolloutFailed is a concluded rollout that could not start.
	RolloutFailed RolloutStatus = "failed"
)

// Terminal reports whether the status concludes a rollout. A terminal status is never left, so a
// write that would move a document out of one is refused.
func (s RolloutStatus) Terminal() bool {
	switch s {
	case RolloutRolledBack, RolloutCompleted, RolloutFailed:
		return true
	default:
		return false
	}
}

// WaveStatus is where a wave is in its life.
type WaveStatus string

// The wave statuses a wave document records, plus the view-only status of a wave that has no
// document yet.
const (
	// WavePending is the status of a wave that has not started: it is what a rollout's state
	// query reports for a sequence entry with no recorded wave yet. It is never stored — the
	// fleet database only ever records the statuses below.
	WavePending WaveStatus = "pending"
	// WaveDispatching is a wave whose membership is recorded and whose update commands are
	// being delivered.
	WaveDispatching WaveStatus = "dispatching"
	// WaveEvaluating is a dispatched wave inside its health window, or one being re-measured.
	WaveEvaluating WaveStatus = "evaluating"
	// WaveSkipped is a wave whose share added no device: it was recorded, not dispatched, and
	// not gated on health.
	WaveSkipped WaveStatus = "skipped"
	// WaveHealthy is a wave its gate promoted.
	WaveHealthy WaveStatus = "healthy"
	// WaveUnhealthy is a wave its gate failed.
	WaveUnhealthy WaveStatus = "unhealthy"
	// WaveFailed is a wave whose update commands could not be delivered.
	WaveFailed WaveStatus = "failed"
)

// RolloutRecord is one rollouts document: the rollout's identity — the firmware it deploys, the
// driving workflow execution, and the target selector — and the status the workflow records as
// it progresses.
type RolloutRecord struct {
	// ID is the rollout id; the _id of the rollout document.
	ID string `bson:"_id"`
	// FirmwareID is the firmware the rollout deploys.
	FirmwareID string `bson:"firmware_id"`
	// Status is the rollout's lifecycle status.
	Status RolloutStatus `bson:"status"`
	// WorkflowID is the id of the workflow execution driving the rollout.
	WorkflowID string `bson:"temporal_wf_id"`
	// Region is the target selector's region.
	Region string `bson:"region"`
	// Model is the target selector's device model.
	Model string `bson:"model"`
	// Rollback is the rollback the rollout ran, written as it compensates: its steps with what
	// each achieved, the inventory the reconciliation established, and the devices it could not
	// restore. It is nil for a rollout that never entered rollback, and a write leaves the stored
	// record alone when it is nil.
	Rollback *RollbackRecord `bson:"rollback,omitempty"`
}

// WaveRecord is one waves document: which devices the wave targets over the share of the pool it
// owns, when its health window opened, the status and success rate its gate recorded, and how its
// devices' updates ended.
type WaveRecord struct {
	// ID is the wave id; the _id of the wave document.
	ID string `bson:"_id"`
	// RolloutID is the rollout the wave belongs to.
	RolloutID string `bson:"rollout_id"`
	// Percent is the wave's cumulative share of the rollout's eligible pool.
	Percent int `bson:"percent"`
	// Status is the wave's lifecycle status.
	Status WaveStatus `bson:"status"`
	// SuccessRate is the success ratio measured over the wave's health window; it is zero
	// until the wave has been evaluated.
	SuccessRate float64 `bson:"success_rate"`
	// DeviceIDs are the devices the wave targets. It is empty — never absent — for a wave whose
	// share added no device.
	DeviceIDs []string `bson:"device_ids"`
	// StartedAt is when the wave started: the earliest time its health window may read from.
	StartedAt time.Time `bson:"started_at"`
	// FailedDeviceIDs are the devices that reported a failed update. It is empty — never
	// absent — for a wave whose devices all succeeded.
	FailedDeviceIDs []string `bson:"failed_device_ids"`
	// UnreportedDeviceIDs are the devices that never reported a result before the wave stopped
	// waiting on them. It is empty — never absent — for a wave whose devices all reported.
	UnreportedDeviceIDs []string `bson:"unreported_device_ids"`
}

// WaveID returns the id of the wave at the given zero-based position of a rollout's sequence.
// The id is derived from the rollout, the position, and the share, so a retried resolution or
// record write addresses the same document instead of creating a second one.
func WaveID(rolloutID string, position, percent int) string {
	return fmt.Sprintf("%s-w%d-%d", rolloutID, position, percent)
}

// WaveTargets returns the devices a wave of the given cumulative share targets: the first share
// of the ordered pool, minus the devices the rollout's earlier waves already target, so no
// device is commanded twice by one rollout.
//
// The result is never nil: a share that adds no device — a percentage smaller than one device of
// the pool, or a share earlier waves already covered — resolves to an empty target set, which is
// a recorded membership rather than a missing one.
func WaveTargets(pool, targeted []string, percent int) []string {
	targets := []string{}
	share := Share(len(pool), percent)
	if share > len(pool) {
		share = len(pool)
	}
	if share <= 0 {
		return targets
	}
	already := make(map[string]struct{}, len(targeted))
	for _, id := range targeted {
		already[id] = struct{}{}
	}
	for _, id := range pool[:share] {
		if _, ok := already[id]; ok {
			continue
		}
		targets = append(targets, id)
	}
	return targets
}

// Share returns how many devices of a pool of poolSize a cumulative percentage covers, rounding
// down: a share smaller than one device covers none, and 100% covers the whole pool.
func Share(poolSize, percent int) int {
	if poolSize <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return poolSize
	}
	return poolSize * percent / 100
}
