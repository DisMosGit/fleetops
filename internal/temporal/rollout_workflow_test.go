package temporal

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
)

func TestRolloutWorkflowDrivesTheSequenceInOrder(t *testing.T) {
	t.Parallel()

	settings := rolloutTestSettings()
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
	// Every wave is decided healthy at the boundary ratio.
	fakes.scriptHealth(healthyAt(0.95))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted || view.Outcome != OutcomeCompleted {
		t.Fatalf("rollout concluded as %s/%s, want completed/completed", view.Status, view.Outcome)
	}
	// The shares are cumulative: 1% of 100 devices is one device, 5% is four more, 25% twenty
	// more, and 100% the remaining 75.
	wantWaves := []WaveView{
		{Percent: 1, Status: rollout.WaveHealthy, SuccessRate: 0.95, TargetCount: 1},
		{Percent: 5, Status: rollout.WaveHealthy, SuccessRate: 0.95, TargetCount: 4},
		{Percent: 25, Status: rollout.WaveHealthy, SuccessRate: 0.95, TargetCount: 20},
		{Percent: 100, Status: rollout.WaveHealthy, SuccessRate: 0.95, TargetCount: 75},
	}
	if diff := cmp.Diff(wantWaves, view.Waves); diff != "" {
		t.Errorf("wave progression mismatch (-want +got):\n%s", diff)
	}
	if view.Current != noWave {
		t.Errorf("current wave = %d, want none after the rollout concluded", view.Current)
	}

	// The order of the wave-level events is the proof that waves run in sequence order and
	// that only one is in flight: each wave is resolved, dispatched, held, evaluated, and
	// recorded before the next one is resolved.
	wantEvents := []string{"rollout running"}
	for position, wave := range settings.Waves {
		id := rollout.WaveID("ro-1", position, wave.Percent)
		wantEvents = append(wantEvents,
			"resolve "+id, "wave "+id+" evaluating", "evaluate "+id, "wave "+id+" healthy")
	}
	wantEvents = append(wantEvents, "rollout completed")
	if diff := cmp.Diff(wantEvents, waveLevelEvents(fakes)); diff != "" {
		t.Errorf("wave progression order mismatch (-want +got):\n%s", diff)
	}

	// Every target device receives exactly one update command, in wave order, carrying the
	// firmware's id, version, and checksum under a command id derived from wave and device.
	var wantCommands []CommandIssuedSignal
	for position, wave := range settings.Waves {
		rec, ok := fakes.recordedWave(rollout.WaveID("ro-1", position, wave.Percent))
		if !ok {
			t.Fatalf("wave %d was never recorded", position)
		}
		if rec.Status != rollout.WaveHealthy || rec.SuccessRate != 0.95 || rec.StartedAt.IsZero() {
			t.Errorf("recorded wave %d = %+v, want a healthy wave with a measured rate and a start",
				position, rec)
		}
		for _, device := range rec.DeviceIDs {
			wantCommands = append(wantCommands, CommandIssuedSignal{
				CommandID:  CommandID(rec.ID, device),
				DeviceID:   device,
				Kind:       CommandKindUpdate,
				FirmwareID: "fw-1",
				Version:    "2.0.0",
				Checksum:   "sha256:0f1e2d",
			})
		}
	}
	if diff := cmp.Diff(wantCommands, fakes.recordedCommands()); diff != "" {
		t.Errorf("dispatched commands mismatch (-want +got):\n%s", diff)
	}
}

func TestRolloutWorkflowSkipsAWaveThatTargetsNobody(t *testing.T) {
	t.Parallel()

	// A share smaller than one device: 1% of ten devices resolves to no device at all.
	settings := RolloutSettings{
		HealthWindow:    5 * time.Minute,
		DecisionTimeout: 30 * time.Minute,
		Waves:           []RolloutWave{{Percent: 1}, {Percent: 100}},
	}
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(10)...)
	fakes.scriptHealth(healthyAt(0.99))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	wantWaves := []WaveView{
		{Percent: 1, Status: rollout.WaveSkipped},
		{Percent: 100, Status: rollout.WaveHealthy, SuccessRate: 0.99, TargetCount: 10},
	}
	if diff := cmp.Diff(wantWaves, view.Waves); diff != "" {
		t.Errorf("wave progression mismatch (-want +got):\n%s", diff)
	}

	skipped, ok := fakes.recordedWave(rollout.WaveID("ro-1", 0, 1))
	if !ok {
		t.Fatal("the skipped wave was not recorded")
	}
	if skipped.Status != rollout.WaveSkipped || len(skipped.DeviceIDs) != 0 {
		t.Errorf("recorded skipped wave = %+v, want a skipped wave with no devices", skipped)
	}
	if skipped.DeviceIDs == nil {
		t.Error("recorded skipped wave has a nil membership, want an empty one")
	}

	// A skipped wave is not dispatched and not gated on health: only the full wave is
	// evaluated, and every command belongs to it.
	evaluations := fakes.recordedEvaluations()
	if len(evaluations) != 1 || evaluations[0].WaveID != rollout.WaveID("ro-1", 1, 100) {
		t.Errorf("evaluations = %+v, want only the wave that was dispatched", evaluations)
	}
	for _, cmd := range fakes.recordedCommands() {
		if !strings.HasPrefix(cmd.CommandID, rollout.WaveID("ro-1", 1, 100)) {
			t.Errorf("command %s does not belong to the dispatched wave", cmd.CommandID)
		}
	}
}

func TestRolloutWorkflowCannotStart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		records []firmware.Record
		want    RolloutOutcome
	}{
		{
			name:    "an unknown firmware fails the rollout",
			records: []firmware.Record{},
			want:    OutcomeFirmwareUnknown,
		},
		{
			name: "a firmware that does not target the selector's model fails the rollout",
			records: []firmware.Record{{
				ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3-mini"}, Checksum: "sha256:0f1e2d",
			}},
			want: OutcomeFirmwareMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutTestSettings()
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
			fakes.firmware = map[string]firmware.Record{}
			for _, rec := range tc.records {
				fakes.firmware[rec.ID] = rec
			}

			view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

			if view.Status != rollout.RolloutFailed || view.Outcome != tc.want {
				t.Fatalf("rollout concluded as %s/%s, want failed/%s", view.Status, view.Outcome, tc.want)
			}
			if view.EndedBy != "" {
				t.Errorf("ended by %q, want no wave to have ended a rollout that never started", view.EndedBy)
			}
			for position, wave := range view.Waves {
				if wave.Status != rollout.WavePending {
					t.Errorf("wave %d is %s, want it never to have started", position, wave.Status)
				}
			}
			if commands := fakes.recordedCommands(); len(commands) != 0 {
				t.Errorf("dispatched %d commands, want none for a rollout that cannot start", len(commands))
			}
			if len(fakes.recordedEvaluations()) != 0 {
				t.Error("a wave was evaluated, want nothing gated for a rollout that cannot start")
			}
			if rec, ok := fakes.recordedRollout("ro-1"); !ok || rec.Status != rollout.RolloutFailed {
				t.Errorf("recorded rollout = %+v (recorded %v), want the terminal failed status", rec, ok)
			}
		})
	}
}

func TestRolloutWorkflowRollsBackOnDispatchFailure(t *testing.T) {
	t.Parallel()

	settings := RolloutSettings{
		HealthWindow:    5 * time.Minute,
		DecisionTimeout: 30 * time.Minute,
		Waves:           []RolloutWave{{Percent: 10}, {Percent: 100}},
	}
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(10)...)
	fakes.dispatchErr = errors.New("no device workflow accepted the command")
	fakes.scriptHealth(healthyAt(0.99))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutRolledBack || view.Outcome != OutcomeDispatchFailed {
		t.Fatalf("rollout concluded as %s/%s, want rolled_back/dispatch_failed", view.Status, view.Outcome)
	}
	failed := rollout.WaveID("ro-1", 0, 10)
	if view.EndedBy != failed {
		t.Errorf("ended by %q, want the wave that could not be delivered (%s)", view.EndedBy, failed)
	}
	if view.Decision != nil {
		t.Errorf("decision = %+v, want none for a wave that was never measured", view.Decision)
	}
	if rec, ok := fakes.recordedWave(failed); !ok || rec.Status != rollout.WaveFailed {
		t.Errorf("recorded wave = %+v (recorded %v), want it recorded as failed", rec, ok)
	}

	// A wave that cannot be delivered is not promoted, and the rollout does not advance: the
	// second wave is never resolved, dispatched, or gated.
	if _, ok := fakes.recordedWave(rollout.WaveID("ro-1", 1, 100)); ok {
		t.Error("the next wave was resolved, want the rollout to have stopped")
	}
	if len(fakes.recordedEvaluations()) != 0 {
		t.Error("a wave was evaluated, want no gate after a dispatch failure")
	}
	// The dispatch is retried before the wave is declared undeliverable, and every attempt
	// repeats the same command id.
	commands := fakes.recordedCommands()
	if len(commands) < 2 {
		t.Fatalf("dispatch attempts = %d, want the delivery retried", len(commands))
	}
	for _, cmd := range commands {
		if cmd.CommandID != CommandID(failed, cmd.DeviceID) {
			t.Errorf("command id = %q, want the same id on every attempt", cmd.CommandID)
		}
	}
}

func TestRolloutWorkflowRecordsRetriedWrites(t *testing.T) {
	t.Parallel()

	settings := rolloutTestSettings()
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
	// The first write of the run fails: its activity retries, and the retried write converges
	// on the same document instead of duplicating or regressing it.
	fakes.recordFails = 1
	fakes.scriptHealth(healthyAt(0.96))

	view := runRollout(t, newRolloutEnv(fakes), rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed despite the retried write", view.Status)
	}
	want := rollout.RolloutRecord{
		ID: "ro-1", FirmwareID: "fw-1", Status: rollout.RolloutCompleted,
		WorkflowID: RolloutWorkflowID("ro-1"), Region: "eu-west", Model: "oak-s3",
	}
	got, ok := fakes.recordedRollout("ro-1")
	if !ok {
		t.Fatal("the rollout was never recorded")
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("recorded rollout mismatch (-want +got):\n%s", diff)
	}
	last, ok := fakes.recordedWave(rollout.WaveID("ro-1", 3, 100))
	if !ok {
		t.Fatal("the last wave was never recorded")
	}
	if last.Status != rollout.WaveHealthy || last.SuccessRate != 0.96 {
		t.Errorf("recorded wave = %+v, want the decided healthy wave", last)
	}
}

func TestRolloutWorkflowAnswersTheStateQuery(t *testing.T) {
	t.Parallel()

	settings := RolloutSettings{
		HealthWindow:    5 * time.Minute,
		DecisionTimeout: 30 * time.Minute,
		Waves:           []RolloutWave{{Percent: 10}, {Percent: 100}},
	}
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(10)...)
	fakes.scriptHealth(healthyAt(0.98))

	env := newRolloutEnv(fakes)
	// The first wave's window closes five minutes in; the second wave is inside its own window
	// a minute later, which is the moment the query is asked.
	duringSecondWave := scheduleQuery(env, 6*time.Minute)
	view := runRollout(t, env, rolloutInputFor(settings))

	running := duringSecondWave.result(t)
	if running.Status != rollout.RolloutRunning || running.Current != 1 {
		t.Fatalf("mid-flight query = %s at wave %d, want running at the second wave",
			running.Status, running.Current)
	}
	wantRunning := []WaveView{
		{Percent: 10, Status: rollout.WaveHealthy, SuccessRate: 0.98, TargetCount: 1},
		{Percent: 100, Status: rollout.WaveEvaluating, TargetCount: 9},
	}
	if diff := cmp.Diff(wantRunning, running.Waves); diff != "" {
		t.Errorf("mid-flight waves mismatch (-want +got):\n%s", diff)
	}

	if view.Status != rollout.RolloutCompleted {
		t.Errorf("final query = %s, want completed", view.Status)
	}
	if len(view.Waves) != 2 || view.Waves[1].Status != rollout.WaveHealthy {
		t.Errorf("final waves = %+v, want both waves recorded healthy", view.Waves)
	}
}

func TestRolloutWorkflowFailsOnASideEffectThatCannotRecover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		broken func(*rolloutFakes)
	}{
		{
			name:   "the firmware registry cannot be read",
			broken: func(f *rolloutFakes) { f.firmwareErr = errors.New("mongo is down") },
		},
		{
			name:   "the rollout cannot be recorded",
			broken: func(f *rolloutFakes) { f.recordErr = errors.New("mongo is down") },
		},
		{
			name:   "a wave cannot be resolved",
			broken: func(f *rolloutFakes) { f.resolveErr = errors.New("mongo is down") },
		},
		{
			name:   "a wave's state cannot be recorded",
			broken: func(f *rolloutFakes) { f.waveRecordErr = errors.New("mongo is down") },
		},
		{
			name: "the wave's health cannot be evaluated",
			broken: func(f *rolloutFakes) {
				f.healthFails = 100
				f.scriptHealth(healthyAt(0.99))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			settings := rolloutTestSettings()
			fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(100)...)
			tc.broken(fakes)
			if len(fakes.health) == 0 {
				fakes.scriptHealth(healthyAt(0.99))
			}

			env := newRolloutEnv(fakes)
			env.ExecuteWorkflow(RolloutWorkflowName, rolloutInputFor(settings))
			if err := env.GetWorkflowError(); err == nil {
				t.Error("workflow error = nil, want a failed execution rather than a silent stall")
			}
		})
	}
}

func TestRolloutWorkflowRejectsAnInputItCannotDrive(t *testing.T) {
	t.Parallel()

	fakes := newRolloutFakes(rolloutTestSettings(), rolloutTestFirmware(), deviceIDs(10)...)
	input := rolloutInputFor(rolloutTestSettings())
	input.FirmwareID = ""

	env := newRolloutEnv(fakes)
	env.ExecuteWorkflow(RolloutWorkflowName, input)
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("workflow error = nil, want an invalid input to be refused")
	}
	if !strings.Contains(err.Error(), "firmware id") {
		t.Errorf("workflow error = %q, want it to name the missing field", err)
	}
	if len(fakes.recordedEvents()) != 0 {
		t.Errorf("events = %v, want nothing recorded for an input that cannot be driven",
			fakes.recordedEvents())
	}
}

// waveLevelEvents returns the recorded events without the per-device command deliveries, which is
// the progression the wave-level assertions are about.
func waveLevelEvents(fakes *rolloutFakes) []string {
	var events []string
	for _, event := range fakes.recordedEvents() {
		if !strings.HasPrefix(event, "command ") {
			events = append(events, event)
		}
	}
	return events
}
