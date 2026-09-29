//go:build integration

package main

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// TestRolloutRestartsAcrossEveryActivity drives a full canary sequence — two waves, the second
// gated by an operator's approval — with a restart injected at the first attempt of every activity
// the rollout runs: each logical unit's first attempt is ended by the worker's stop and its retry
// runs on the fresh worker. The approval is sent while the worker is deliberately down, which is the
// case a restart could lose: a signal delivered while no worker was executing the rollout.
//
// It asserts what the specs claim of such a run: the sequence completes with every wave promoted,
// exactly one approval carries exactly one gated wave, each targeted device was commanded under
// exactly the one command id its wave and device derive, every logical unit was scheduled exactly
// once, the rollout's state never regressed across a restart, a restart after the rollout concluded
// applies nothing further, and the whole history replays.
func TestRolloutRestartsAcrossEveryActivity(t *testing.T) {
	world := newRestartWorld(t, worldOptions{
		rolloutID:        "ro-restart-every-activity",
		waves:            []temporal.RolloutWave{{Percent: 25}, {Percent: 100, RequireApproval: true}},
		fleet:            4,
		plan:             holdEvery(forwardActivityNames()...),
		sampleResumption: true,
	})
	world.startRollout()

	// The rollout resolves nothing about its gated wave until an operator approves it, so this is
	// the moment the worker can be taken down without an attempt in flight.
	waitForRolloutStatus(t, world.rollouts, world.opts.rolloutID, rollout.RolloutAwaitingApproval)
	if err := world.worker.Halt(); err != nil {
		t.Fatalf("stop the worker at the approval gate: %v", err)
	}
	// The approval arrives while no worker is polling the task queue: Temporal records it, and the
	// fresh worker folds it when it resumes the workflow.
	if err := temporal.NewRolloutSignals(world.client).Approve(world.ctx, world.opts.rolloutID); err != nil {
		t.Fatalf("approve the gated wave while no worker is polling: %v", err)
	}
	world.worker.Start()
	world.sampleResumption()

	record := waitForRolloutStatus(t, world.rollouts, world.opts.rolloutID, rollout.RolloutCompleted)
	// The rollout document names the workflow execution driving it and the firmware it deployed, so
	// a reader of the record has the handle to the run's state query and its subject.
	if record.WorkflowID != temporal.RolloutWorkflowID(world.opts.rolloutID) {
		t.Errorf("rollout workflow id = %q, want %q",
			record.WorkflowID, temporal.RolloutWorkflowID(world.opts.rolloutID))
	}
	if record.FirmwareID != restartFirmwareID {
		t.Errorf("rollout firmware id = %q, want %q", record.FirmwareID, restartFirmwareID)
	}
	first := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 0, 25))
	gated := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 1, 100))
	for _, wave := range []rollout.WaveRecord{first, gated} {
		if wave.Status != rollout.WaveHealthy {
			t.Errorf("wave %s status = %q, want healthy", wave.ID, wave.Status)
		}
	}
	if len(first.DeviceIDs) != 1 || len(gated.DeviceIDs) != 3 {
		t.Errorf("wave membership = %d and %d devices, want 1 and 3 of the four-device fleet",
			len(first.DeviceIDs), len(gated.DeviceIDs))
	}

	// Every targeted device was commanded under the one command id its wave and device derive, and
	// no other command id addresses them: a restart that re-dispatched a wave would show up here
	// even though the redeliveries repeat an id.
	assertWaveCommandIdentities(t, world.dispatcher, []rollout.WaveRecord{first, gated})

	// One approval was delivered and it carried exactly the gated wave: the rollout completed, so
	// the second wave needed no second approval and no wave was resolved twice.
	history := assertReplays(t, world.client, world.opts.rolloutID)
	if got := signalCount(history, temporal.ApproveNextWaveSignalName); got != 1 {
		t.Errorf("the history carries %d approvals, want exactly one", got)
	}
	assertScheduledOnce(t, history, forwardActivityNames()...)
	assertHeldAcross(t, world, forwardActivityNames()...)
	// The resumption check had a reading for every restart the harness injected, so the rollout had
	// somewhere to fail if it had resumed anywhere but where it stopped.
	if got, want := world.sampler.count(), world.worker.restarts(); got < want {
		t.Errorf("the rollout's state was sampled %d times for %d restarts, want a reading per restart",
			got, want)
	}

	// The document carries the terminal status as the workflow's last write, so the run is waited
	// out: a restart after the rollout concluded has to be a non-event, and it can only be asserted
	// on a workflow execution that really has nothing left to do.
	waitForWorkflowStatus(t, world.client, world.opts.rolloutID,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	assertNoFurtherEffects(t, world)

	// The replay assertion is one that can fail: the same history against a different workflow
	// registration is not a history that workflow could have produced.
	assertReplayRejectsAForeignWorkflow(t, history)
}

// TestRolloutPauseSurvivesARestart pauses a rollout inside a wave's health window, restarts the
// worker while the pause holds, and resumes it: the pause is the operator's intent and a restart
// must not lose it, must not move the window the wave is being measured over, and must not start the
// next wave on its own.
func TestRolloutPauseSurvivesARestart(t *testing.T) {
	world := newRestartWorld(t, worldOptions{
		rolloutID: "ro-restart-paused",
		waves:     []temporal.RolloutWave{{Percent: 50}, {Percent: 100}},
		fleet:     2,
		// The window is the scenario's own: the pause and the restart both have to land while the
		// wave is still inside it.
		healthWindow:     restartPauseWindow,
		plan:             &restartPlan{},
		sampleResumption: true,
	})
	world.startRollout()

	// The wave is inside its health window once its membership is recorded and its updates have
	// settled, which is the state its started_at and its deadline belong to.
	firstWaveID := rollout.WaveID(world.opts.rolloutID, 0, 50)
	waitForWaveStatus(t, world.waves, firstWaveID, rollout.WaveEvaluating)
	if err := temporal.NewRolloutSignals(world.client).Pause(world.ctx, world.opts.rolloutID); err != nil {
		t.Fatalf("pause the rollout inside the wave's window: %v", err)
	}
	waitForRolloutStatus(t, world.rollouts, world.opts.rolloutID, rollout.RolloutPaused)
	before := readWave(t, world.waves, firstWaveID)
	world.sampleResumption()

	// The restart happens while the rollout is paused and the wave is in flight.
	if _, err := world.worker.Restart(); err != nil {
		t.Fatalf("restart the worker while the rollout is paused: %v", err)
	}
	world.sampleResumption()

	if paused := readRollout(t, world, world.opts.rolloutID); paused.Status != rollout.RolloutPaused {
		t.Errorf("rollout status after the restart = %q, want it still paused", paused.Status)
	}
	if after := readWave(t, world.waves, firstWaveID); !after.StartedAt.Equal(before.StartedAt) {
		t.Errorf("wave %s started at %v after the restart, want its recorded %v",
			firstWaveID, after.StartedAt, before.StartedAt)
	}

	// The wave is still driven to its recorded decision — a pause holds promotion, never safety —
	// and it is judged at the deadline its recorded start and the configured window imply.
	decided := waitForWave(t, world.waves, firstWaveID)
	if decided.Status != rollout.WaveHealthy {
		t.Errorf("wave %s status = %q, want the paused rollout's wave still judged healthy",
			firstWaveID, decided.Status)
	}
	if deadline := before.StartedAt.Add(world.cfg.Rollout.HealthWindow.Duration); time.Now().Before(deadline) {
		t.Errorf("wave %s was judged before the deadline %v its recorded start implies", firstWaveID, deadline)
	}

	// The rollout holds at the next wave boundary: nothing about the next wave was resolved or
	// dispatched while the pause held.
	if status := readRollout(t, world, world.opts.rolloutID).Status; status != rollout.RolloutPaused {
		t.Errorf("rollout status after its wave was judged = %q, want it still paused", status)
	}
	if count := countWaves(t, world.waves, world.opts.rolloutID); count != 1 {
		t.Errorf("the paused rollout has %d wave documents, want only the wave in flight", count)
	}

	// The resume continues the rollout from where it stopped, and no wave's commands were
	// dispatched twice across the restart.
	if err := temporal.NewRolloutSignals(world.client).Resume(world.ctx, world.opts.rolloutID); err != nil {
		t.Fatalf("resume the paused rollout: %v", err)
	}
	waitForRolloutStatus(t, world.rollouts, world.opts.rolloutID, rollout.RolloutCompleted)
	next := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 1, 100))
	if next.Status != rollout.WaveHealthy {
		t.Errorf("wave %s status after the resume = %q, want healthy", next.ID, next.Status)
	}
	assertWaveCommandIdentities(t, world.dispatcher, []rollout.WaveRecord{decided, next})
	assertScheduledOnce(t, assertReplays(t, world.client, world.opts.rolloutID), forwardActivityNames()...)
}

// TestRolloutRestartsAfterEveryEffect runs a complete rollout — wave, failed gate, and rollback —
// with every activity's first attempt executed and then failed, so that attempt's effect has landed
// while its result was lost, and restarts the worker before the retry: each retry therefore runs on
// a fresh worker and has to converge on the effect its first attempt intended instead of adding a
// second one. The fleet is one device in one wave, so every activity is the only unit in flight and
// the attempt each unit takes is exactly the injected one and its retry.
func TestRolloutRestartsAfterEveryEffect(t *testing.T) {
	world := newRestartWorld(t, worldOptions{
		rolloutID: "ro-restart-after-effect",
		waves:     []temporal.RolloutWave{{Percent: 100}},
		fleet:     1,
		plan:      crashAfterEvery(rolloutActivityNames()...),
	})
	// The wave's fleet is failing from the start, so the wave is judged unhealthy and the rollout
	// compensates the single device it dispatched.
	world.breakFleet()
	world.startRollout()

	record := awaitConcludedRollback(t, world)
	awaitClosure(t, world)
	wave := waitForWave(t, world.waves, rollout.WaveID(world.opts.rolloutID, 0, 100))
	if wave.Status != rollout.WaveUnhealthy {
		t.Errorf("wave status = %q, want unhealthy", wave.Status)
	}

	// Every logical unit was carried by the attempt that landed its effect, which was then failed,
	// and by the retry on a fresh worker that converged on the same effect: no unit was decided
	// again, and none of them ran once.
	units := world.ledger.units()
	if len(units) == 0 {
		t.Fatal("the worker recorded no activity attempt at all")
	}
	for id, attempts := range units {
		if len(attempts) != 2 {
			t.Errorf("unit %s took %d attempts, want the injected failure and its retry: %s",
				id, len(attempts), describeAttempts(attempts))
			continue
		}
		first, retry := attempts[0], attempts[1]
		if first.Attempt != 1 || retry.Attempt != 2 {
			t.Errorf("unit %s attempts = %d then %d, want the attempt and its first retry",
				id, first.Attempt, retry.Attempt)
		}
		if retry.Outcome != attemptRan || retry.Err != "" {
			t.Errorf("unit %s's retry ended as %s (%s), want it to run and return",
				id, retry.Outcome, retry.Err)
		}
		if first.Outcome != attemptCrashed {
			t.Errorf("unit %s's first attempt ended as %s (%s), want the injected post-effect failure",
				id, first.Outcome, first.Err)
		}
		if first.Worker == retry.Worker {
			t.Errorf("unit %s was retried on the same worker generation (%s), want a fresh one",
				id, first.Worker)
		}
	}
	if got := len(world.ledger.crashed()); got != len(units) {
		t.Errorf("the ledger records %d post-effect failures for %d units, want one each", got, len(units))
	}

	// The effect ledger is exactly-once by identity: the redelivered attempt repeated the identity
	// its first attempt used rather than minting a second one. The announcement is the one effect a
	// redelivery really does repeat on the broker, and both copies carry the phase's one event id.
	assertWaveCommandIdentities(t, world.dispatcher, []rollout.WaveRecord{wave})
	assertRestoreIdentities(t, world.dispatcher, world.fleet)
	assertAnnouncementIdentities(t, awaitAnnouncements(t, world.broker, 4), world.opts.rolloutID)

	// The recorded rollout, its wave, and the device's version describe one run of one device.
	if record.Rollback == nil {
		t.Fatal("the rollout document carries no rollback record")
	}
	if got := countWaves(t, world.waves, world.opts.rolloutID); got != 1 {
		t.Errorf("the rollout recorded %d waves, want the one wave it drove", got)
	}
	for _, step := range record.Rollback.Steps {
		if step.Status != rollout.RollbackStepCompleted {
			t.Errorf("step %s is recorded as %s on a concluded rollback, want completed", step.Kind, step.Status)
		}
	}
	if got := recordedFirmware(t, world.db, world.fleet[0]); got != smokeInitialFirmware {
		t.Errorf("the compensated device is recorded on %q, want the firmware it was restored to (%q)",
			got, smokeInitialFirmware)
	}
	assertRecordsMatchDevices(t, world, world.fleet)
	assertScheduledOnce(t, assertReplays(t, world.client, world.opts.rolloutID), rolloutActivityNames()...)
}
