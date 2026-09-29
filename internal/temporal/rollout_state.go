package temporal

import (
	"errors"
	"fmt"
	"time"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// rolloutCarryVersion stamps the rollout state schema. A run refuses a state it cannot
// understand instead of guessing at a foreign shape.
const rolloutCarryVersion = 1

// noWave is the current-wave position of a rollout with no wave in flight.
const noWave = -1

// rolloutState is the rollout workflow's only mutable data: the rollout's identity, the policy it
// drives under, and the recorded outcome of every wave of its sequence. The fleet database holds
// a projection of it, but the workflow's own state is what the state query and every next
// decision read.
type rolloutState struct {
	// CarryVersion stamps the state schema; see rolloutCarryVersion.
	CarryVersion int `json:"carry_version"`
	// RolloutID is the rollout this state belongs to; the workflow's identity.
	RolloutID string `json:"rollout_id"`
	// FirmwareID is the firmware the rollout deploys.
	FirmwareID string `json:"firmware_id"`
	// FirmwareVersion is the version of the firmware the rollout deploys, empty until its
	// metadata has been loaded. It mirrors the deployed firmware for the rollout's search
	// attributes, and is not part of the rollout's identity: the id is.
	FirmwareVersion string `json:"firmware_version,omitempty"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
	// WorkflowID is the id of the workflow execution driving the rollout.
	WorkflowID string `json:"workflow_id"`
	// Settings are the configured canary policy the rollout drives under.
	Settings RolloutSettings `json:"settings"`
	// Status is the status the rollout's own control flow reached: running, or awaiting an
	// approval, or one of the terminal statuses. It is not what the rollout reports — see
	// reported, which folds the operator's pause over it — and every recorded status goes
	// through that projection, so the document, the search attribute, and the state query
	// cannot disagree.
	Status rollout.RolloutStatus `json:"status"`
	// Paused reports whether an operator has held the rollout. A pause takes effect immediately
	// for advancement (no wave is resolved or dispatched) and is visible immediately even while
	// a wave is in flight, because the operator's intent is what the status reports.
	Paused bool `json:"paused,omitempty"`
	// Waves is one entry per configured wave, in sequence order.
	Waves []rolloutWaveProgress `json:"waves"`
	// Current is the position in Waves of the wave in flight, or noWave.
	Current int `json:"current"`
	// ApprovalOutstanding reports whether an approval is held for the next wave that requires
	// one: an operator may approve before the gate is reached, and the approval is banked.
	ApprovalOutstanding bool `json:"approval_outstanding"`
	// Outcome is why the rollout concluded; empty while it runs.
	Outcome RolloutOutcome `json:"outcome,omitempty"`
	// EndedBy is the id of the wave that ended the rollout; empty unless one did.
	EndedBy string `json:"ended_by,omitempty"`
	// Decision is the failing wave's measured health; nil unless a gate ended the rollout.
	Decision *WaveHealth `json:"decision,omitempty"`
	// LastAttrs is the search-attribute map the workflow last upserted; nil before the first
	// upsert. Keeping it in state is what makes the change detection survive a replay: a state
	// change that mirrors nothing does not upsert.
	LastAttrs map[string]any `json:"last_attrs,omitempty"`
}

// rolloutWaveProgress is one wave's progress within a rollout: the identity its resolution
// derived, the status and success rate its gate recorded, how many devices it targets, and which
// of those devices did not take the update.
type rolloutWaveProgress struct {
	// ID is the wave's derived identity; empty before the wave is resolved.
	ID string `json:"id,omitempty"`
	// Status is the wave's recorded status, or rollout.WavePending before it starts.
	Status rollout.WaveStatus `json:"status"`
	// SuccessRate is the success ratio its gate measured; zero until it decided.
	SuccessRate float64 `json:"success_rate"`
	// TargetCount is the size of the membership the wave was resolved to.
	TargetCount int `json:"target_count"`
	// FailedDeviceIDs are the wave's devices that reported a failed update.
	FailedDeviceIDs []string `json:"failed_device_ids,omitempty"`
	// UnreportedDeviceIDs are the wave's devices that never reported a result.
	UnreportedDeviceIDs []string `json:"unreported_device_ids,omitempty"`
}

// newRolloutState returns the state of a rollout that has not started: running, with no wave in
// flight and every configured wave still pending.
func newRolloutState(in RolloutInput) rolloutState {
	waves := make([]rolloutWaveProgress, len(in.Settings.Waves))
	for i := range waves {
		waves[i].Status = rollout.WavePending
	}
	return rolloutState{
		CarryVersion: rolloutCarryVersion,
		RolloutID:    in.RolloutID,
		FirmwareID:   in.FirmwareID,
		Region:       in.Region,
		Model:        in.Model,
		WorkflowID:   RolloutWorkflowID(in.RolloutID),
		Settings:     in.Settings,
		Status:       rollout.RolloutRunning,
		Waves:        waves,
		Current:      noWave,
	}
}

// validate checks the input a run was started from. A rollout no workflow could drive fails
// loudly instead of half-running: it names no rollout, firmware, or selector, or carries a policy
// that cannot sequence a canary. The sequence's shape is validated at startup too; a rollout
// started by hand in the Temporal UI has no configuration file behind it.
func (s rolloutState) validate() error {
	if s.CarryVersion != rolloutCarryVersion {
		return fmt.Errorf("rollout state of %q at carry version %d (want %d): %w",
			s.RolloutID, s.CarryVersion, rolloutCarryVersion, ErrUnsupportedCarryVersion)
	}
	for _, required := range []struct {
		field string
		value string
	}{
		{"rollout id", s.RolloutID},
		{"firmware id", s.FirmwareID},
		{"region", s.Region},
		{"model", s.Model},
	} {
		if required.value == "" {
			return fmt.Errorf("rollout input without %s", required.field)
		}
	}
	if err := s.Settings.validate(); err != nil {
		return err
	}
	if len(s.Waves) != len(s.Settings.Waves) {
		return fmt.Errorf("rollout state carries %d waves for a sequence of %d",
			len(s.Waves), len(s.Settings.Waves))
	}
	return nil
}

// validate checks that a policy can sequence a canary: a positive health window, a decision
// timeout no shorter than it, a result timeout inside that decision timeout, and an ordered
// sequence whose shares are inside (0, 100], strictly increasing, and end at 100% so a completed
// rollout means the whole target group was reached.
func (s RolloutSettings) validate() error {
	if s.HealthWindow <= 0 {
		return errors.New("rollout settings without a positive health window")
	}
	if s.DecisionTimeout < s.HealthWindow {
		return errors.New("rollout settings with a decision timeout below the health window")
	}
	if s.ResultTimeout <= 0 {
		return errors.New("rollout settings without a positive result timeout")
	}
	// Waiting longer for device results than a wave may stay undecided would let the wave's
	// decision timeout pass while it still waits on devices that can no longer change it.
	if s.ResultTimeout > s.DecisionTimeout {
		return errors.New("rollout settings with a result timeout above the decision timeout")
	}
	if len(s.Waves) == 0 {
		return errors.New("rollout settings without a wave sequence")
	}
	for i, wave := range s.Waves {
		if wave.Percent <= 0 || wave.Percent > 100 {
			return fmt.Errorf("rollout wave %d: percent %d must be in (0, 100]", i, wave.Percent)
		}
		if i > 0 && wave.Percent <= s.Waves[i-1].Percent {
			return fmt.Errorf("rollout wave %d: percent %d must be above the previous wave's %d",
				i, wave.Percent, s.Waves[i-1].Percent)
		}
	}
	if last := s.Waves[len(s.Waves)-1].Percent; last != 100 {
		return fmt.Errorf("rollout wave %d: percent %d must be 100", len(s.Waves)-1, last)
	}
	return nil
}

// terminal reports whether the rollout has concluded. A concluded rollout dispatches nothing and
// its recorded status never changes again.
func (s rolloutState) terminal() bool {
	return s.Status.Terminal()
}

// reported derives the status the rollout reports, which is what its document, its search
// attribute, and its state query all carry: a terminal status, else `paused` while an operator
// holds it, else `awaiting_approval` while it holds at a gated wave, else `running`. One function
// produces it, so the three records cannot disagree.
//
// The pause outranks `awaiting_approval` because it is the operator's most recent intent and the
// more informative answer: a paused rollout waiting at a gate reports the hold, and its
// outstanding approval stays recordable through the query's own field.
func (s rolloutState) reported() rollout.RolloutStatus {
	switch {
	case s.terminal():
		return s.Status
	case s.Paused:
		return rollout.RolloutPaused
	default:
		return s.Status
	}
}

// pause holds the rollout: an operator stopped it from starting further waves. A pause is
// idempotent, and a concluded rollout ignores it — a terminal status is never left.
func (s *rolloutState) pause() {
	if s.terminal() {
		return
	}
	s.Paused = true
}

// resume lets a held rollout continue from exactly where it stopped: every recorded wave outcome
// and any banked approval are untouched. A resume of a rollout that is not paused changes
// nothing, and a concluded rollout ignores it.
func (s *rolloutState) resume() {
	if s.terminal() {
		return
	}
	s.Paused = false
}

// holdForApproval records that the rollout is holding at a wave that requires an approval, with
// that wave as the one it waits for.
func (s *rolloutState) holdForApproval(position int) {
	s.Status = rollout.RolloutAwaitingApproval
	s.Current = position
}

// releaseApprovalHold returns a rollout that was holding for approval to running. The operator's
// pause, if any, is untouched: a resumed hold still reports the pause.
func (s *rolloutState) releaseApprovalHold() {
	s.Status = rollout.RolloutRunning
}

// applyApproval folds one delivered approval into the state: the approval is outstanding and the
// next wave that requires one may start without waiting. An approval delivered after the rollout
// concluded changes nothing.
func (s *rolloutState) applyApproval() {
	if s.terminal() {
		return
	}
	s.ApprovalOutstanding = true
}

// consumeApproval spends the outstanding approval on the wave that is about to start: one
// approval authorizes exactly one wave.
func (s *rolloutState) consumeApproval() {
	s.ApprovalOutstanding = false
}

// startWave records that a wave is in flight: the identity its resolution derived, the size of the
// membership it targets, and its position as the rollout's current wave.
func (s *rolloutState) startWave(position int, resolved ResolvedWave) {
	s.Waves[position].ID = resolved.WaveID
	s.Waves[position].TargetCount = len(resolved.DeviceIDs)
	s.Current = position
}

// recordWave records a wave's decided status and the success rate its gate measured, along with
// the device outcomes its dispatch collected: which of its devices failed their update and which
// never reported. The sets are replaced whole, so a retried record converges on the same outcome.
func (s *rolloutState) recordWave(
	position int,
	status rollout.WaveStatus,
	successRate float64,
	outcomes waveOutcomes,
) {
	s.Waves[position].Status = status
	s.Waves[position].SuccessRate = successRate
	s.Waves[position].FailedDeviceIDs = outcomes.Failed
	s.Waves[position].UnreportedDeviceIDs = outcomes.Unreported
}

// waveOutcomes are the device outcomes one wave's dispatch collected: the devices that reported a
// failed update and the devices that never reported before the wave stopped waiting on them.
type waveOutcomes struct {
	// Failed are the devices that reported a failed update.
	Failed []string
	// Unreported are the devices that never reported a result.
	Unreported []string
}

// complete concludes a rollout whose whole sequence was promoted.
func (s *rolloutState) complete() {
	s.Status = rollout.RolloutCompleted
	s.Outcome = OutcomeCompleted
	s.Current = noWave
}

// rollback concludes a rollout on the wave that ended it, carrying that wave's decision — nil when
// the wave failed before it could be measured.
func (s *rolloutState) rollback(position int, outcome RolloutOutcome, decision *WaveHealth) {
	s.Status = rollout.RolloutRolledBack
	s.Outcome = outcome
	s.EndedBy = s.Waves[position].ID
	s.Decision = decision
	s.Current = noWave
}

// fail concludes a rollout that could not start: nothing was dispatched and no wave is current.
func (s *rolloutState) fail(outcome RolloutOutcome) {
	s.Status = rollout.RolloutFailed
	s.Outcome = outcome
	s.Current = noWave
}

// view derives the state query's answer: the rollout's identity and status, the configured
// sequence with each wave's recorded outcome, and — once it concluded — why and on which wave.
func (s rolloutState) view() RolloutView {
	waves := make([]WaveView, 0, len(s.Settings.Waves))
	for i, wave := range s.Settings.Waves {
		view := WaveView{Percent: wave.Percent, Status: rollout.WavePending}
		if i < len(s.Waves) {
			view.Status = s.Waves[i].Status
			view.SuccessRate = s.Waves[i].SuccessRate
			view.TargetCount = s.Waves[i].TargetCount
			view.FailedCount = len(s.Waves[i].FailedDeviceIDs)
			view.UnreportedCount = len(s.Waves[i].UnreportedDeviceIDs)
		}
		waves = append(waves, view)
	}
	return RolloutView{
		RolloutID:           s.RolloutID,
		Status:              s.reported(),
		FirmwareID:          s.FirmwareID,
		FirmwareVersion:     s.FirmwareVersion,
		Region:              s.Region,
		Model:               s.Model,
		Waves:               waves,
		Current:             s.Current,
		ApprovalOutstanding: s.ApprovalOutstanding,
		Outcome:             s.Outcome,
		EndedBy:             s.EndedBy,
		Decision:            s.Decision,
	}
}

// waveDecision is what one wave's gate concluded: the health it measured and the status the wave
// is recorded with.
type waveDecision struct {
	// Health is the measurement the decision rests on.
	Health WaveHealth
	// Status is the wave's recorded status: healthy, or unhealthy.
	Status rollout.WaveStatus
	// TimedOut reports that the wave was still undecided when its decision timeout passed, so it
	// is treated as unhealthy rather than decided on evidence.
	TimedOut bool
}

// windowDeadline returns the moment a wave's health window has fully elapsed.
func (s rolloutState) windowDeadline(startedAt time.Time) time.Time {
	return startedAt.Add(s.Settings.HealthWindow)
}

// decisionDeadline returns the moment a wave has been undecided for too long: measured from the
// wave's recorded start, it bounds how long a wave may hold a rollout open.
func (s rolloutState) decisionDeadline(startedAt time.Time) time.Time {
	return startedAt.Add(s.Settings.DecisionTimeout)
}
