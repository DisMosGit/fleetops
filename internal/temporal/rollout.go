package temporal

import (
	"time"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// Registered names and the signal/query types of the rollout workflow. Workflows and activities
// register under explicit names — never function-name reflection — and an operator addresses the
// approval signal and the state query by these constants.
const (
	// RolloutWorkflowName is the registered name of RolloutWorkflow.
	RolloutWorkflowName = "rollout-workflow"
	// GetRolloutStateQueryType is the state query returning the rollout's authoritative state.
	GetRolloutStateQueryType = "get-rollout-state"
	// ApproveNextWaveSignalName carries an ApproveNextWaveSignal.
	ApproveNextWaveSignalName = "approve_next_wave"
	// LoadFirmwareActivityName is the registered name of the load-firmware activity.
	LoadFirmwareActivityName = "load-firmware"
	// ResolveWaveTargetsActivityName is the registered name of the resolve-wave-targets activity.
	ResolveWaveTargetsActivityName = "resolve-wave-targets"
	// RecordRolloutStateActivityName is the registered name of the record-rollout-state activity.
	RecordRolloutStateActivityName = "record-rollout-state"
	// RecordWaveStateActivityName is the registered name of the record-wave-state activity.
	RecordWaveStateActivityName = "record-wave-state"
	// DispatchWaveUpdateActivityName is the registered name of the dispatch-wave-update activity.
	DispatchWaveUpdateActivityName = "dispatch-wave-update"
	// EvaluateWaveHealthActivityName is the registered name of the evaluate-wave-health activity.
	EvaluateWaveHealthActivityName = "evaluate-wave-health"
)

// The Temporal error types of a rollout that cannot start. The load-firmware caller's retry policy
// lists them as non-retryable, so a decision the firmware registry has already made fails the
// rollout visibly instead of retrying forever.
const (
	firmwareUnknownErrorType  = "firmware_unknown"
	firmwareMismatchErrorType = "firmware_mismatch"
)

// RolloutWorkflowID returns the stable workflow id of a rollout. One rollout has exactly one
// workflow execution under this id, and the id is the handle the rollout document carries.
func RolloutWorkflowID(rolloutID string) string {
	return "rollout-" + rolloutID
}

// CommandID returns the id of the update command one wave issues to one device. It is derived
// from the wave and the device, so a redelivered command repeats the same id and is a no-op for
// the device that already accepted it.
func CommandID(waveID, deviceID string) string {
	return waveID + "-" + deviceID
}

// RolloutRequest is a caller's request to start a rollout: which rollout, the firmware to deploy,
// and the target selector. The canary policy is deliberately not part of it — the worker's
// configuration supplies that, so every rollout of a deployment drives the same sequence.
type RolloutRequest struct {
	// RolloutID identifies the rollout; the workflow id is derived from it.
	RolloutID string `json:"rollout_id"`
	// FirmwareID is the firmware to deploy.
	FirmwareID string `json:"firmware_id"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
}

// RolloutInput is the rollout workflow's start input: the request plus the policy the rollout
// drives under. It is built by the starter from the worker's configuration, so the sequence and
// the windows a run chain drives are the values it started with.
type RolloutInput struct {
	RolloutRequest
	// Settings are the configured canary policy this rollout drives under.
	Settings RolloutSettings `json:"settings"`
}

// RolloutSettings are the configured canary policy one rollout decides under.
type RolloutSettings struct {
	// HealthWindow is the width of the durable window each wave's health is measured over.
	HealthWindow time.Duration `json:"health_window"`
	// DecisionTimeout is the longest a wave may stay undecided before its gate treats it as
	// unhealthy, measured from the wave's recorded start.
	DecisionTimeout time.Duration `json:"decision_timeout"`
	// Waves is the canary sequence the rollout drives, in sequence order.
	Waves []RolloutWave `json:"waves"`
}

// RolloutWave is one entry of the configured canary sequence.
type RolloutWave struct {
	// Percent is the wave's cumulative share of the rollout's eligible pool.
	Percent int `json:"percent"`
	// RequireApproval reports whether the wave waits for an operator's approval before it starts.
	RequireApproval bool `json:"require_approval"`
}

// ApproveNextWaveSignal is the approve_next_wave signal payload: an operator letting the rollout's
// next approval-requiring wave start. It carries nothing — the fleet has a single operator, so
// there is no identity to carry and the signal's arrival is the decision — and an operator sends
// it with the Temporal CLI or UI.
type ApproveNextWaveSignal struct{}

// RolloutOutcome names why a rollout reached a terminal state.
type RolloutOutcome string

// The terminal outcomes a rollout reports.
const (
	// OutcomeCompleted is a rollout whose whole sequence was promoted.
	OutcomeCompleted RolloutOutcome = "completed"
	// OutcomeUnhealthyWave is a rollout whose wave failed its gate on a decided verdict.
	OutcomeUnhealthyWave RolloutOutcome = "unhealthy_wave"
	// OutcomeDecisionTimeout is a rollout whose wave never gathered enough evidence to decide.
	OutcomeDecisionTimeout RolloutOutcome = "decision_timeout"
	// OutcomeDispatchFailed is a rollout whose wave's update commands could not be delivered.
	OutcomeDispatchFailed RolloutOutcome = "dispatch_failed"
	// OutcomeFirmwareUnknown is a rollout naming a firmware the registry does not hold.
	OutcomeFirmwareUnknown RolloutOutcome = "firmware_unknown"
	// OutcomeFirmwareMismatch is a rollout whose firmware does not target the selector's model.
	OutcomeFirmwareMismatch RolloutOutcome = "firmware_mismatch"
)

// Firmware is the firmware metadata a rollout needs to command an update: the version and the
// checksum every target device verifies its download against, and the models it may be deployed
// to.
type Firmware struct {
	// ID is the firmware identity.
	ID string `json:"id"`
	// Version is the firmware version devices report after applying it.
	Version string `json:"version"`
	// Checksum is the digest a downloaded binary must match.
	Checksum string `json:"checksum"`
	// Models are the device models the firmware targets.
	Models []string `json:"models"`
}

// WaveHealth is one wave's evaluated health: the measurement the gate decides on, and the decision
// it records when the wave ends the rollout.
type WaveHealth struct {
	// Verdict is the decision the measurement produced: healthy, unhealthy, or undecided.
	Verdict wavehealth.Verdict `json:"verdict"`
	// SuccessRatio is the share of samples that succeeded, in [0, 1].
	SuccessRatio float64 `json:"success_ratio"`
	// SampleSize is the number of samples the ratio was computed over.
	SampleSize int64 `json:"sample_size"`
	// WindowStart is the start of the effective window the measurement read.
	WindowStart time.Time `json:"window_start"`
	// WindowEnd is the end of the effective window: the moment the gate decided to evaluate.
	WindowEnd time.Time `json:"window_end"`
}

// WaveView is one entry of a rollout's configured sequence as its state query reports it.
type WaveView struct {
	// Percent is the wave's cumulative share of the eligible pool.
	Percent int `json:"percent"`
	// Status is the wave's recorded status, or rollout.WavePending before the wave starts.
	Status rollout.WaveStatus `json:"status"`
	// SuccessRate is the success ratio the wave's gate measured; zero until it decided.
	SuccessRate float64 `json:"success_rate"`
	// TargetCount is how many devices the wave targets; zero before it is resolved.
	TargetCount int `json:"target_count"`
}

// RolloutView is what a rollout's state query reports: where the rollout stands, what it deploys,
// how each wave of its sequence fared, and — once it concluded — why.
type RolloutView struct {
	// RolloutID is the rollout this view belongs to.
	RolloutID string `json:"rollout_id"`
	// Status is the rollout's lifecycle status.
	Status rollout.RolloutStatus `json:"status"`
	// FirmwareID is the firmware the rollout deploys.
	FirmwareID string `json:"firmware_id"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
	// Waves is the configured sequence with each wave's recorded outcome.
	Waves []WaveView `json:"waves"`
	// Current is the position in Waves of the wave in flight — the wave a waiting rollout waits
	// for — or -1 when no wave is in flight.
	Current int `json:"current"`
	// ApprovalOutstanding reports whether an operator's approval is held for the next wave that
	// requires one.
	ApprovalOutstanding bool `json:"approval_outstanding"`
	// Outcome is why the rollout concluded; empty while it is running.
	Outcome RolloutOutcome `json:"outcome,omitempty"`
	// EndedBy is the id of the wave that ended the rollout; empty unless one did.
	EndedBy string `json:"ended_by,omitempty"`
	// Decision is the failing wave's measured health; nil unless a wave's gate ended the rollout.
	Decision *WaveHealth `json:"decision,omitempty"`
}
