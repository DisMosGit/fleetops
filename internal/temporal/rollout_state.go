package temporal

import (
	"errors"
	"fmt"
	"slices"
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
	// Rollback is the rollback the rollout derived when it entered rollback, with each step's
	// recorded outcome; nil until then. It is the plan the workflow executes, so a replay of a
	// run derives and runs the same one.
	Rollback *rollbackProgress `json:"rollback,omitempty"`
	// LastAttrs is the search-attribute map the workflow last upserted; nil before the first
	// upsert. Keeping it in state is what makes the change detection survive a replay: a state
	// change that mirrors nothing does not upsert.
	LastAttrs map[string]any `json:"last_attrs,omitempty"`
}

// rollbackProgress is the rollback a rollout derived and ran: why it happened, the plan's steps in
// order with what each achieved, the inventory the reconciliation established, and the devices it
// could not restore.
type rollbackProgress struct {
	// Outcome is the outcome that ended the rollout.
	Outcome RolloutOutcome `json:"outcome"`
	// Steps are the plan's steps in plan order.
	Steps []rollbackStep `json:"steps"`
	// Inventory is the reconciled inventory, ordered by version; empty until the reconciliation
	// step has run.
	Inventory []FirmwareCount `json:"inventory,omitempty"`
	// Unrestored are the devices the rollback could not restore, in the order the steps that
	// found them ran.
	Unrestored []string `json:"unrestored_device_ids,omitempty"`
}

// rollbackStep is one step of the derived plan: what it compensates, where it stands, and what it
// achieved. The membership it compensates is copied from the wave's recorded membership when the
// plan is derived, so the plan names exactly the devices the rollout moved.
type rollbackStep struct {
	// Kind is what the step does.
	Kind rollout.RollbackStepKind `json:"kind"`
	// WaveID is the wave this step compensates; empty for a step that compensates no wave.
	WaveID string `json:"wave_id,omitempty"`
	// Status is where the step stands.
	Status rollout.RollbackStepStatus `json:"status"`
	// Devices is the membership the step compensates.
	Devices []string `json:"devices,omitempty"`
	// Outcomes are the device outcomes of a downgrade step.
	Outcomes rollbackDeviceCounts `json:"outcomes,omitempty"`
	// Corrections are the record outcomes of a reconciliation step.
	Corrections rollbackInventoryCounts `json:"corrections,omitempty"`
	// Detail is why a step failed; empty otherwise.
	Detail string `json:"detail,omitempty"`
}

// rollbackDeviceCounts counts what one downgrade step's devices achieved. It is total: every device
// the step targeted is counted once.
type rollbackDeviceCounts struct {
	// Restored is how many devices reported their restore concluded successfully.
	Restored int `json:"restored"`
	// Failed is how many devices reported their restore concluded unsuccessfully.
	Failed int `json:"failed"`
	// Unreported is how many devices never concluded their restore before the deadline.
	Unreported int `json:"unreported"`
	// Skipped is how many devices had nothing to restore.
	Skipped int `json:"skipped"`
	// Unavailable is how many devices could not be restored at all.
	Unavailable int `json:"unavailable"`
}

// rollbackInventoryCounts counts what one reconciliation step's device records achieved.
type rollbackInventoryCounts struct {
	// Agreed is how many records already matched the version their device holds.
	Agreed int `json:"agreed"`
	// Corrected is how many records were wrong and now hold the version their device reports.
	Corrected int `json:"corrected"`
	// Unverified is how many devices' records could not be verified.
	Unverified int `json:"unverified"`
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
	// DeviceIDs is the membership the wave was resolved to, recorded when the wave starts and
	// never re-derived: the rollback plan compensates exactly the devices the rollout moved, and
	// the pool can have moved under the rollout since.
	DeviceIDs []string `json:"device_ids,omitempty"`
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
// attribute, and its state query all carry: a terminal status, else `rolling_back` while the
// compensations run, else `paused` while an operator holds it, else `awaiting_approval` while it
// holds at a gated wave, else `running`. One function produces it, so the three records cannot
// disagree.
//
// The pause outranks `awaiting_approval` because it is the operator's most recent intent and the
// more informative answer: a paused rollout waiting at a gate reports the hold, and its
// outstanding approval stays recordable through the query's own field. The compensating phase
// outranks the pause: a rollout that has begun compensating has stopped deciding, its
// compensations are not interruptible, and a hold folded before it entered rollback cannot make
// the fleet's restoration look optional.
func (s rolloutState) reported() rollout.RolloutStatus {
	switch {
	case s.terminal():
		return s.Status
	case s.rollbackBegun():
		return s.Status
	case s.Paused:
		return rollout.RolloutPaused
	default:
		return s.Status
	}
}

// rollbackBegun reports whether the rollout has entered the compensating phase. From that moment
// its forward progress is over: no wave is resolved, dispatched, or gated, and operator control
// changes nothing.
func (s rolloutState) rollbackBegun() bool {
	return s.Status == rollout.RolloutRollingBack
}

// pause holds the rollout: an operator stopped it from starting further waves. A pause is
// idempotent, and a concluded or compensating rollout ignores it — a terminal status is never
// left, and a compensation is not interruptible.
func (s *rolloutState) pause() {
	if s.terminal() || s.rollbackBegun() {
		return
	}
	s.Paused = true
}

// resume lets a held rollout continue from exactly where it stopped: every recorded wave outcome
// and any banked approval are untouched. A resume of a rollout that is not paused changes
// nothing, and a concluded or compensating rollout ignores it.
func (s *rolloutState) resume() {
	if s.terminal() || s.rollbackBegun() {
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
// concluded or entered rollback changes nothing.
func (s *rolloutState) applyApproval() {
	if s.terminal() || s.rollbackBegun() {
		return
	}
	s.ApprovalOutstanding = true
}

// consumeApproval spends the outstanding approval on the wave that is about to start: one
// approval authorizes exactly one wave.
func (s *rolloutState) consumeApproval() {
	s.ApprovalOutstanding = false
}

// startWave records that a wave is in flight: the identity its resolution derived, the membership
// it targets, and its position as the rollout's current wave. The membership is recorded here, and
// nowhere else: it is what a rollback compensates, and re-deriving it later could name a different
// set of devices.
func (s *rolloutState) startWave(position int, resolved ResolvedWave) {
	s.Waves[position].ID = resolved.WaveID
	s.Waves[position].TargetCount = len(resolved.DeviceIDs)
	s.Waves[position].DeviceIDs = resolved.DeviceIDs
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

// beginRollback ends a rollout on the wave that failed it and derives the plan of compensations it
// will run, carrying that wave's decision — nil when the wave failed before it could be measured.
// The rollout stops deciding here and reports the compensating phase until finishRollback is
// called; its terminal status is recorded only once the plan has run.
func (s *rolloutState) beginRollback(position int, outcome RolloutOutcome, decision *WaveHealth) {
	s.Status = rollout.RolloutRollingBack
	s.Outcome = outcome
	s.EndedBy = s.Waves[position].ID
	s.Decision = decision
	s.Current = noWave
	s.Rollback = &rollbackProgress{Outcome: outcome, Steps: deriveRollbackPlan(*s)}
}

// finishRollback concludes a rollout whose plan has run: the compensations are done, so the rollout
// records the terminal status it was always going to reach.
func (s *rolloutState) finishRollback() {
	s.Status = rollout.RolloutRolledBack
}

// deriveRollbackPlan builds the ordered plan of compensations from the rollout's own recorded
// progress: the announcement of the rollback, one downgrade step per wave the rollout dispatched
// with the most recently dispatched compensated first, the inventory reconciliation, and the
// completion announcement. Each compensating step names the forward step it compensates — the wave
// it restores, or the bookkeeping it corrects.
//
// The plan is a pure function of the state, so a replay derives the identical plan. A wave the
// rollout never dispatched — a wave still pending, or a share that resolved to no device —
// contributes no step, and a rollout with nothing to compensate derives no plan at all: there is
// nothing to undo and nothing to announce.
func deriveRollbackPlan(s rolloutState) []rollbackStep {
	var downgrades []rollbackStep
	for position := len(s.Waves) - 1; position >= 0; position-- {
		wave := s.Waves[position]
		if len(wave.DeviceIDs) == 0 {
			continue
		}
		downgrades = append(downgrades, rollbackStep{
			Kind:    rollout.RollbackDowngrade,
			WaveID:  wave.ID,
			Status:  rollout.RollbackStepPending,
			Devices: wave.DeviceIDs,
		})
	}
	if len(downgrades) == 0 {
		return nil
	}
	// The reconciliation covers exactly the devices the downgrade steps compensate, so its
	// membership is recorded with the plan rather than re-derived when it runs.
	var compensated []string
	for _, step := range downgrades {
		compensated = append(compensated, step.Devices...)
	}

	plan := make([]rollbackStep, 0, len(downgrades)+3)
	plan = append(plan, rollbackStep{
		Kind:   rollout.RollbackNotifyStarted,
		Status: rollout.RollbackStepPending,
	})
	plan = append(plan, downgrades...)
	plan = append(plan,
		rollbackStep{
			Kind:    rollout.RollbackReconcileInventory,
			Status:  rollout.RollbackStepPending,
			Devices: compensated,
		},
		rollbackStep{Kind: rollout.RollbackNotifyCompleted, Status: rollout.RollbackStepPending},
	)
	return plan
}

// compensatedDevices returns every device the rollback's plan compensates, in the order the plan
// compensates them: the membership of the waves it restores, most recently dispatched first. The
// waves of one rollout target disjoint devices, so no device appears twice.
func compensatedDevices(progress *rollbackProgress) []string {
	for _, step := range progress.Steps {
		if step.Kind == rollout.RollbackReconcileInventory {
			return step.Devices
		}
	}
	return nil
}

// startStep records that the plan has reached a step.
func (r *rollbackProgress) startStep(position int) {
	r.Steps[position].Status = rollout.RollbackStepRunning
}

// completeStep records that a step did its work.
func (r *rollbackProgress) completeStep(position int) {
	r.Steps[position].Status = rollout.RollbackStepCompleted
}

// failStep records a step whose own work failed: what it was trying to do, and why it could not.
func (r *rollbackProgress) failStep(position int, detail string) {
	r.Steps[position].Status = rollout.RollbackStepFailed
	r.Steps[position].Detail = detail
}

// recordDowngrade folds one downgrade step's device outcomes into the step's counts and adds the
// devices that could not be restored to the rollback's unrestored set. Every device the step
// targeted is counted exactly once; a skipped device is not unrestored, because it never ran the
// rolled-back firmware and there is nothing left to put back.
func (r *rollbackProgress) recordDowngrade(position int, restores []DeviceRestore) {
	var counts rollbackDeviceCounts
	for _, restore := range restores {
		switch restore.Outcome {
		case RestoreRestored:
			counts.Restored++
		case RestoreFailed:
			counts.Failed++
		case RestoreUnreported:
			counts.Unreported++
		case RestoreSkipped:
			counts.Skipped++
		default:
			counts.Unavailable++
		}
		if restore.Outcome != RestoreRestored && restore.Outcome != RestoreSkipped {
			r.Unrestored = append(r.Unrestored, restore.DeviceID)
		}
	}
	r.Steps[position].Outcomes = counts
	r.completeStep(position)
}

// recordReconciliation folds one reconciliation step's device outcomes into the step's counts and
// stores the inventory the step established. The inventory counts every device the step verified on
// the version it was found to run; a device that could not be verified — including one whose outcome
// carried no version — is counted as unverified and deliberately not counted as running any version,
// so the inventory still accounts for every device exactly once.
func (r *rollbackProgress) recordReconciliation(position int, devices []DeviceInventory) {
	var counts rollbackInventoryCounts
	verdicts := make(map[string]int)
	for _, device := range devices {
		switch {
		case device.Outcome == InventoryUnverified || device.Version == "":
			counts.Unverified++
		case device.Outcome == InventoryCorrected:
			counts.Corrected++
			verdicts[device.Version]++
		default:
			counts.Agreed++
			verdicts[device.Version]++
		}
	}
	r.Steps[position].Corrections = counts
	r.Inventory = sortedInventory(verdicts)
	r.completeStep(position)
}

// sortedInventory renders a version-to-device count as an inventory ordered by version, which is
// what makes the recorded inventory and the announced one comparable.
func sortedInventory(counts map[string]int) []FirmwareCount {
	versions := make([]string, 0, len(counts))
	for version := range counts {
		versions = append(versions, version)
	}
	slices.Sort(versions)
	inventory := make([]FirmwareCount, 0, len(versions))
	for _, version := range versions {
		inventory = append(inventory, FirmwareCount{Version: version, Devices: counts[version]})
	}
	return inventory
}

// view derives the rollback the state query reports: the plan with each step's status and what it
// achieved, the reconciled inventory, and the devices the rollback could not restore. It is nil for
// a rollout that has never entered rollback.
func (r *rollbackProgress) view() *RollbackView {
	if r == nil {
		return nil
	}
	plan := make([]RollbackStepView, 0, len(r.Steps))
	for _, step := range r.Steps {
		plan = append(plan, step.view())
	}
	return &RollbackView{
		Plan:                plan,
		Inventory:           r.Inventory,
		UnrestoredDeviceIDs: r.Unrestored,
	}
}

// view derives one step of the plan as the state query reports it.
func (s rollbackStep) view() RollbackStepView {
	return RollbackStepView{
		Kind:        s.Kind,
		WaveID:      s.WaveID,
		Status:      s.Status,
		Devices:     len(s.Devices),
		Restored:    s.Outcomes.Restored,
		Failed:      s.Outcomes.Failed,
		Unreported:  s.Outcomes.Unreported,
		Skipped:     s.Outcomes.Skipped,
		Unavailable: s.Outcomes.Unavailable,
		Agreed:      s.Corrections.Agreed,
		Corrected:   s.Corrections.Corrected,
		Unverified:  s.Corrections.Unverified,
		Detail:      s.Detail,
	}
}

// record maps the rollback onto the document the fleet database carries, built from the same state
// the query reports so the two cannot disagree. It is nil for a rollout that never entered
// rollback, which leaves a stored record alone.
func (r *rollbackProgress) record() *rollout.RollbackRecord {
	if r == nil {
		return nil
	}
	steps := make([]rollout.RollbackStepRecord, 0, len(r.Steps))
	for _, step := range r.Steps {
		steps = append(steps, rollout.RollbackStepRecord{
			Kind:        step.Kind,
			WaveID:      step.WaveID,
			Status:      step.Status,
			Devices:     len(step.Devices),
			Restored:    step.Outcomes.Restored,
			Failed:      step.Outcomes.Failed,
			Unreported:  step.Outcomes.Unreported,
			Skipped:     step.Outcomes.Skipped,
			Unavailable: step.Outcomes.Unavailable,
			Agreed:      step.Corrections.Agreed,
			Corrected:   step.Corrections.Corrected,
			Unverified:  step.Corrections.Unverified,
			Detail:      step.Detail,
		})
	}
	inventory := make([]rollout.FirmwareInventoryRecord, 0, len(r.Inventory))
	for _, count := range r.Inventory {
		inventory = append(inventory, rollout.FirmwareInventoryRecord{
			Version: count.Version,
			Devices: count.Devices,
		})
	}
	return &rollout.RollbackRecord{
		Outcome:             string(r.Outcome),
		Steps:               steps,
		Inventory:           inventory,
		UnrestoredDeviceIDs: nonNilIDs(r.Unrestored),
	}
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
		Rollback:            s.Rollback.view(),
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
