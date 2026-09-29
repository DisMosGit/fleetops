package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// FirmwareSource loads one firmware's metadata. *firmware.Store satisfies it, and the interface
// lives here because this package is where the metadata is consumed.
type FirmwareSource interface {
	// Metadata returns the firmware document with the given id, reporting an error wrapping
	// firmware.ErrNotFound when there is none.
	Metadata(ctx context.Context, id string) (firmware.Record, error)
}

// TargetResolver resolves a wave into the membership it records. *rollout.Store satisfies it.
type TargetResolver interface {
	// ResolveWave returns a wave's recorded membership and start time, resolving and recording
	// them when the wave has no document yet.
	ResolveWave(ctx context.Context, req rollout.ResolveRequest) (rollout.WaveRecord, error)
}

// RolloutRecorder records a rollout document. *rollout.Store satisfies it.
type RolloutRecorder interface {
	// RecordRollout ensures the rollout document exists and moves its status, leaving a
	// terminal status alone.
	RecordRollout(ctx context.Context, rec rollout.RolloutRecord) error
}

// WaveRecorder records a wave document's decided state. *rollout.Store satisfies it.
type WaveRecorder interface {
	// RecordWaveState records a resolved wave's status and success rate.
	RecordWaveState(ctx context.Context, update rollout.WaveStateUpdate) error
}

// DeviceCommander delivers one command to a device's workflow. *Signaler satisfies it.
type DeviceCommander interface {
	// SignalCommandIssued delivers a command to its target device's workflow.
	SignalCommandIssued(ctx context.Context, cmd CommandIssuedSignal) error
}

// DeviceStateReader reads one device's authoritative state from its entity workflow.
// *DeviceStates satisfies it. It is what the update activity observes while it waits for a
// device to report, so it reads the workflow that owns the device rather than a projection.
type DeviceStateReader interface {
	// State returns the state of one device's entity workflow, reporting an error wrapping
	// ErrDeviceNotFound when the device has no workflow execution.
	State(ctx context.Context, deviceID string) (State, error)
}

// HealthEvaluator evaluates one wave's measured health. *wavehealth.Aggregator satisfies it.
type HealthEvaluator interface {
	// Evaluate measures the wave over its recorded membership and start time, ending the
	// effective window at at.
	Evaluate(ctx context.Context, query wavehealth.Query, at time.Time) (wavehealth.Result, error)
}

// LoadFirmware is the load-firmware activity's request: the firmware to load and the model the
// rollout targets, which that firmware must support.
type LoadFirmware struct {
	// FirmwareID is the firmware to load.
	FirmwareID string `json:"firmware_id"`
	// Model is the device model the rollout targets.
	Model string `json:"model"`
}

// NewLoadFirmwareActivity returns the load-firmware activity bound to source: it reads the
// firmware's metadata and refuses one that cannot be deployed to the rollout's model. Both
// refusals — an unknown firmware and a model the firmware does not target — are decisions nothing
// can retry away, so they are reported as non-retryable errors the workflow turns into a failed
// rollout before any command is dispatched.
func NewLoadFirmwareActivity(source FirmwareSource) func(ctx context.Context, req LoadFirmware) (Firmware, error) {
	return func(ctx context.Context, req LoadFirmware) (Firmware, error) {
		rec, err := source.Metadata(ctx, req.FirmwareID)
		if errors.Is(err, firmware.ErrNotFound) {
			return Firmware{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("firmware %s is unknown", req.FirmwareID), firmwareUnknownErrorType, err)
		}
		if err != nil {
			return Firmware{}, fmt.Errorf("load firmware %s: %w", req.FirmwareID, err)
		}
		loaded := Firmware{
			ID:       rec.ID,
			Version:  rec.Version,
			Checksum: rec.Checksum,
			Models:   rec.Models,
		}
		if !slices.Contains(loaded.Models, req.Model) {
			return Firmware{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("firmware %s does not target model %s", req.FirmwareID, req.Model),
				firmwareMismatchErrorType, nil)
		}
		return loaded, nil
	}
}

// ResolveWaveRequest is the resolve-wave-targets activity's request: the wave to resolve and the
// start time the workflow decided for it. The start time is a workflow clock reading, which is
// what makes a replayed resolution record the same window.
type ResolveWaveRequest struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave's derived identity.
	WaveID string `json:"wave_id"`
	// Percent is the wave's cumulative share of the eligible pool.
	Percent int `json:"percent"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
	// StartedAt is when the wave starts: the earliest time its health window may read from.
	StartedAt time.Time `json:"started_at"`
}

// ResolvedWave is a wave's recorded membership and start time — the facts its dispatch and its
// health evaluation both rest on.
type ResolvedWave struct {
	// WaveID is the wave's identity.
	WaveID string `json:"wave_id"`
	// DeviceIDs are the devices the wave targets; it may be empty.
	DeviceIDs []string `json:"device_ids"`
	// StartedAt is when the wave started, as recorded.
	StartedAt time.Time `json:"started_at"`
}

// NewResolveWaveActivity returns the resolve-wave-targets activity bound to resolver: it resolves
// the wave's membership against the eligible pool and records it before anything is dispatched.
// The store treats a wave that already has a document as the answer, so a retried resolution
// returns the recorded membership and start time instead of re-deriving them.
func NewResolveWaveActivity(resolver TargetResolver) func(ctx context.Context, req ResolveWaveRequest) (ResolvedWave, error) {
	return func(ctx context.Context, req ResolveWaveRequest) (ResolvedWave, error) {
		rec, err := resolver.ResolveWave(ctx, rollout.ResolveRequest{
			RolloutID: req.RolloutID,
			WaveID:    req.WaveID,
			Percent:   req.Percent,
			Region:    req.Region,
			Model:     req.Model,
			StartedAt: req.StartedAt,
		})
		if err != nil {
			return ResolvedWave{}, fmt.Errorf("resolve wave %s of rollout %s: %w",
				req.WaveID, req.RolloutID, err)
		}
		return ResolvedWave{WaveID: rec.ID, DeviceIDs: rec.DeviceIDs, StartedAt: rec.StartedAt}, nil
	}
}

// RecordRolloutRequest is the rollout document the record-rollout-state activity writes: the
// rollout's identity fields and the status it is in.
type RecordRolloutRequest struct {
	// RolloutID is the rollout to record.
	RolloutID string `json:"rollout_id"`
	// FirmwareID is the firmware the rollout deploys.
	FirmwareID string `json:"firmware_id"`
	// WorkflowID is the id of the workflow execution driving the rollout.
	WorkflowID string `json:"workflow_id"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
	// Status is the status to record.
	Status rollout.RolloutStatus `json:"status"`
}

// NewRecordRolloutActivity returns the record-rollout-state activity bound to recorder. The write
// is idempotent and never moves a concluded rollout, so a retry of a recorded transition leaves
// the document as it stands.
func NewRecordRolloutActivity(recorder RolloutRecorder) func(ctx context.Context, req RecordRolloutRequest) error {
	return func(ctx context.Context, req RecordRolloutRequest) error {
		err := recorder.RecordRollout(ctx, rollout.RolloutRecord{
			ID:         req.RolloutID,
			FirmwareID: req.FirmwareID,
			Status:     req.Status,
			WorkflowID: req.WorkflowID,
			Region:     req.Region,
			Model:      req.Model,
		})
		if err != nil {
			return fmt.Errorf("record rollout %s as %s: %w", req.RolloutID, req.Status, err)
		}
		return nil
	}
}

// RecordWaveRequest is the record-wave-state activity's request: the wave to move and the state it
// reached, including the device outcomes its dispatch collected.
type RecordWaveRequest struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave to move.
	WaveID string `json:"wave_id"`
	// Status is the status to record.
	Status rollout.WaveStatus `json:"status"`
	// SuccessRate is the evaluated success ratio; zero until the wave has been evaluated.
	SuccessRate float64 `json:"success_rate"`
	// FailedDeviceIDs are the devices that reported a failed update.
	FailedDeviceIDs []string `json:"failed_device_ids"`
	// UnreportedDeviceIDs are the devices that never reported before the wave stopped waiting.
	UnreportedDeviceIDs []string `json:"unreported_device_ids"`
}

// NewRecordWaveActivity returns the record-wave-state activity bound to recorder: it moves a
// recorded wave's status and success rate and stores the device outcomes it collected, leaving its
// membership and start time alone.
func NewRecordWaveActivity(recorder WaveRecorder) func(ctx context.Context, req RecordWaveRequest) error {
	return func(ctx context.Context, req RecordWaveRequest) error {
		err := recorder.RecordWaveState(ctx, rollout.WaveStateUpdate{
			RolloutID:           req.RolloutID,
			WaveID:              req.WaveID,
			Status:              req.Status,
			SuccessRate:         req.SuccessRate,
			FailedDeviceIDs:     req.FailedDeviceIDs,
			UnreportedDeviceIDs: req.UnreportedDeviceIDs,
		})
		if err != nil {
			return fmt.Errorf("record wave %s as %s: %w", req.WaveID, req.Status, err)
		}
		return nil
	}
}

// EvaluateWaveRequest is the evaluate-wave-health activity's request: the wave to measure and the
// moment the workflow decided to judge it.
type EvaluateWaveRequest struct {
	// RolloutID is the rollout the wave belongs to.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave to measure.
	WaveID string `json:"wave_id"`
	// At is the workflow's decision time: the end of the effective window.
	At time.Time `json:"at"`
}

// NewEvaluateWaveActivity returns the evaluate-wave-health activity bound to evaluator: it
// measures the wave over the devices and start time its document records, ending the window at
// the workflow-decided moment. An evaluation failure is left to the caller's retry policy — a
// failed evaluation never decides a wave, only a returned verdict does.
func NewEvaluateWaveActivity(evaluator HealthEvaluator) func(ctx context.Context, req EvaluateWaveRequest) (WaveHealth, error) {
	return func(ctx context.Context, req EvaluateWaveRequest) (WaveHealth, error) {
		result, err := evaluator.Evaluate(ctx,
			wavehealth.Query{RolloutID: req.RolloutID, WaveID: req.WaveID}, req.At)
		if err != nil {
			return WaveHealth{}, fmt.Errorf("evaluate wave %s of rollout %s: %w",
				req.WaveID, req.RolloutID, err)
		}
		return WaveHealth{
			Verdict:      result.Verdict,
			SuccessRatio: result.SuccessRatio,
			SampleSize:   result.SampleSize,
			WindowStart:  result.WindowStart,
			WindowEnd:    result.WindowEnd,
		}, nil
	}
}
