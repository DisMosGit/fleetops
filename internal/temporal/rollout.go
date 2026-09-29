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
	// PauseRolloutSignalName carries a PauseRolloutSignal.
	PauseRolloutSignalName = "pause_rollout"
	// ResumeRolloutSignalName carries a ResumeRolloutSignal.
	ResumeRolloutSignalName = "resume_rollout"
	// LoadFirmwareActivityName is the registered name of the load-firmware activity.
	LoadFirmwareActivityName = "load-firmware"
	// ResolveWaveTargetsActivityName is the registered name of the resolve-wave-targets activity.
	ResolveWaveTargetsActivityName = "resolve-wave-targets"
	// RecordRolloutStateActivityName is the registered name of the record-rollout-state activity.
	RecordRolloutStateActivityName = "record-rollout-state"
	// RecordWaveStateActivityName is the registered name of the record-wave-state activity.
	RecordWaveStateActivityName = "record-wave-state"
	// UpdateDeviceActivityName is the registered name of the update-device activity: it commands
	// one device and waits for that device's reported result.
	UpdateDeviceActivityName = "update-device"
	// DowngradeDeviceActivityName is the registered name of the downgrade-device activity: it
	// restores one device to the firmware version it ran before the deployed one and waits for
	// that device's reported result.
	DowngradeDeviceActivityName = "downgrade-device"
	// ReconcileInventoryActivityName is the registered name of the reconcile-device-inventory
	// activity: it reconciles one device's recorded firmware version against the version that
	// device's workflow holds.
	ReconcileInventoryActivityName = "reconcile-device-inventory"
	// AnnounceRollbackActivityName is the registered name of the announce-rollback activity: it
	// publishes one phase of a rollback's announcement into the broker.
	AnnounceRollbackActivityName = "announce-rollback"
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

// NewRolloutSettings builds the canary policy a rollout drives under from a deployment's
// configured values: the health window, the decision timeout, how long a wave waits for each
// device's reported result, and the wave sequence with its approval flags.
//
// It validates what it builds, so a policy no rollout could sequence fails at the entrypoint that
// read the configuration rather than inside a running rollout, and every entrypoint that starts a
// rollout maps its configuration through this one function.
func NewRolloutSettings(
	healthWindow, decisionTimeout, resultTimeout time.Duration,
	waves []RolloutWave,
) (RolloutSettings, error) {
	settings := RolloutSettings{
		HealthWindow:    healthWindow,
		DecisionTimeout: decisionTimeout,
		ResultTimeout:   resultTimeout,
		Waves:           waves,
	}
	if err := settings.validate(); err != nil {
		return RolloutSettings{}, err
	}
	return settings, nil
}

// RolloutSettings are the configured canary policy one rollout decides under.
type RolloutSettings struct {
	// HealthWindow is the width of the durable window each wave's health is measured over.
	HealthWindow time.Duration `json:"health_window"`
	// DecisionTimeout is the longest a wave may stay undecided before its gate treats it as
	// unhealthy, measured from the wave's recorded start.
	DecisionTimeout time.Duration `json:"decision_timeout"`
	// ResultTimeout is the longest a wave waits for one device's reported update result before
	// that device counts as unreported and the wave stops waiting on it, measured from the
	// wave's dispatch.
	ResultTimeout time.Duration `json:"result_timeout"`
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

// PauseRolloutSignal is the pause_rollout signal payload: an operator holding the rollout so it
// starts no further wave. It carries nothing: the signal's arrival is the decision, and a pause
// holds the rollout until a resume arrives.
type PauseRolloutSignal struct{}

// ResumeRolloutSignal is the resume_rollout signal payload: an operator letting a paused rollout
// continue from exactly where it stopped. It carries nothing, and a resume of a rollout that is
// not paused changes nothing.
type ResumeRolloutSignal struct{}

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
	// FailedCount is how many of the wave's devices reported a failed update.
	FailedCount int `json:"failed_count"`
	// UnreportedCount is how many of the wave's devices never reported a result before the
	// wave stopped waiting on them.
	UnreportedCount int `json:"unreported_count"`
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
	// FirmwareVersion is the deployed firmware's version, empty until its metadata has been
	// loaded. It is what the run's RolloutFirmware search attribute holds.
	FirmwareVersion string `json:"firmware_version,omitempty"`
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
	// Rollback is the rollback the rollout derived and is running or ran, or nil while the
	// rollout has never entered rollback.
	Rollback *RollbackView `json:"rollback,omitempty"`
}

// RollbackView is the rollback a rollout reports: the plan with each step's status and what it
// achieved, the inventory the reconciliation established, and the devices the rollback could not
// restore.
type RollbackView struct {
	// Plan is the rollback's steps in plan order.
	Plan []RollbackStepView `json:"plan"`
	// Inventory is the reconciled inventory — how many of the devices the rollback touched were
	// found on each firmware version — ordered by version; empty until the reconciliation has
	// run.
	Inventory []FirmwareCount `json:"inventory,omitempty"`
	// UnrestoredDeviceIDs are the devices the rollback could not restore.
	UnrestoredDeviceIDs []string `json:"unrestored_device_ids,omitempty"`
}

// RollbackStepView is one step of the rollback plan as the state query reports it: what it
// compensates, where it stands, and what it achieved.
type RollbackStepView struct {
	// Kind is what the step does.
	Kind rollout.RollbackStepKind `json:"kind"`
	// WaveID is the wave a compensating step compensates; empty for a step that compensates no
	// wave.
	WaveID string `json:"wave_id,omitempty"`
	// Status is where the step stands: pending, running, completed, or failed.
	Status rollout.RollbackStepStatus `json:"status"`
	// Devices is how many devices the step compensates or reconciles.
	Devices int `json:"devices"`
	// Restored, Failed, Unreported, Skipped, and Unavailable are the outcomes of a downgrade
	// step's devices.
	Restored    int `json:"restored"`
	Failed      int `json:"failed"`
	Unreported  int `json:"unreported"`
	Skipped     int `json:"skipped"`
	Unavailable int `json:"unavailable"`
	// Agreed, Corrected, and Unverified are the outcomes of a reconciliation step's device
	// records.
	Agreed     int `json:"agreed"`
	Corrected  int `json:"corrected"`
	Unverified int `json:"unverified"`
	// Detail is why a step failed; empty otherwise.
	Detail string `json:"detail,omitempty"`
}

// FirmwareCount is how many of a rollback's devices run one firmware version.
type FirmwareCount struct {
	// Version is the firmware version devices were found on.
	Version string `json:"version"`
	// Devices is how many devices were found on it.
	Devices int `json:"devices"`
}
