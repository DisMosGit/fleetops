//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// The assertions a restart scenario shares: what the harness injected, when a run is over, and
// what a rollback record has to say about itself and the waves it compensated.
// describeAttempts renders one logical unit's attempts for a failure message.
func describeAttempts(attempts []activityAttempt) string {
	parts := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		parts = append(parts, fmt.Sprintf("%s #%d ended %s on %s (%s)",
			attempt.Name, attempt.Attempt, attempt.Outcome, attempt.Worker, attempt.Err))
	}
	return strings.Join(parts, "; ")
}

// restartPauseWindow is the pause scenario's health window: wide enough that the operator's pause
// and the restart both land while the wave is still inside it, and short enough to keep the scenario
// bounded.
const restartPauseWindow = 8 * time.Second

// forwardActivityNames returns the activities a rollout that completes runs: every activity but the
// compensations a rollback adds.
func forwardActivityNames() []string {
	return []string{
		temporal.LoadFirmwareActivityName,
		temporal.ResolveWaveTargetsActivityName,
		temporal.RecordRolloutStateActivityName,
		temporal.RecordWaveStateActivityName,
		temporal.UpdateDeviceActivityName,
		temporal.EvaluateWaveHealthActivityName,
	}
}

// assertHeldAcross asserts the restarts this run injected: an attempt of every named activity was
// held, every held attempt was ended by the worker's stop rather than executed, and the harness
// reports at least one restart per named activity. It is the assertion that fails if a held attempt
// ran instead of being cancelled, or if a plan stopped injecting restarts.
func assertHeldAcross(t *testing.T, w *restartWorld, names ...string) {
	t.Helper()
	held := w.ledger.held()
	if len(held) == 0 {
		t.Fatal("the plan held no activity attempt at all")
	}
	byName := make(map[string]int, len(names))
	for _, attempt := range held {
		if attempt.Outcome != attemptCancelled {
			t.Errorf("the held attempt of %s (activity %s) ended as %s (%s), want the worker's stop to cancel it",
				attempt.Name, attempt.ActivityID, attempt.Outcome, attempt.Err)
		}
		if attempt.Err == "" {
			t.Errorf("the held attempt of %s (activity %s) records no cancellation", attempt.Name, attempt.ActivityID)
		}
		byName[attempt.Name]++
	}
	for _, name := range names {
		if byName[name] == 0 {
			t.Errorf("no attempt of %s was held, want the plan to inject a restart at every activity", name)
		}
	}
	if got, want := w.worker.restarts(), len(names); got < want {
		t.Errorf("the harness injected %d restarts, want at least one per activity (%d)", got, want)
	}
}

// awaitClosure waits for a rollout's workflow execution to finish. A rollout's last activity writes its
// terminal document before the workflow returns, so a scenario that reads the ledger, the history, or
// the effects after that write has to wait out the run: the final unit's retry may still be the step
// between the write and the workflow's own end.
func awaitClosure(t *testing.T, w *restartWorld) {
	t.Helper()
	waitForWorkflowStatus(t, w.client, w.opts.rolloutID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// waitForWorkflowStatus waits until the server reports the given status for a rollout's workflow
// execution, which is how a scenario distinguishes "the last write landed" from "the run is over".
func waitForWorkflowStatus(
	t *testing.T,
	c temporalclient.Client,
	rolloutID string,
	status enumspb.WorkflowExecutionStatus,
) {
	t.Helper()
	deadline := time.Now().Add(smokePollTimeout)
	var last enumspb.WorkflowExecutionStatus
	for time.Now().Before(deadline) {
		last = workflowStatus(t, c, rolloutID)
		if last == status {
			return
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("rollout %s workflow status = %s, want %s within %v", rolloutID, last, status, smokePollTimeout)
}

// awaitConcludedRollback waits for a rollout to conclude as rolled back and asserts, on every
// reading it takes, that the terminal status never appears before the plan's last step has run:
// "rolled back" is a statement about compensations that ran, not about ones that were planned.
func awaitConcludedRollback(t *testing.T, w *restartWorld) rollout.RolloutRecord {
	t.Helper()
	deadline := time.Now().Add(smokePollTimeout)
	var last rollout.RolloutRecord
	for time.Now().Before(deadline) {
		var record rollout.RolloutRecord
		err := w.rollouts.FindOne(context.Background(),
			map[string]any{"_id": w.opts.rolloutID}).Decode(&record)
		switch {
		case err != nil && !isNoDocuments(err):
			t.Fatalf("read rollout %s: %v", w.opts.rolloutID, err)
		case err == nil:
			last = record
			if record.Status == rollout.RolloutRolledBack {
				assertRollbackFinished(t, record)
				return record
			}
			if record.Rollback != nil {
				// A rollback the workflow is running must still be reported as running: the
				// terminal status is the plan's own conclusion.
				if record.Status != rollout.RolloutRollingBack {
					t.Errorf("rollout status = %q while its rollback is recorded, want %q",
						record.Status, rollout.RolloutRollingBack)
				}
			}
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("rollout %s reached %q, want %q within %v",
		w.opts.rolloutID, last.Status, rollout.RolloutRolledBack, smokePollTimeout)
	return rollout.RolloutRecord{}
}

// assertRollbackFinished asserts a concluded rollback's record carries a plan that ran to its end.
func assertRollbackFinished(t *testing.T, record rollout.RolloutRecord) {
	t.Helper()
	if record.Rollback == nil {
		t.Fatal("the rollout concluded as rolled back with no rollback record")
	}
	if len(record.Rollback.Steps) == 0 {
		t.Fatal("the rollback record carries no step")
	}
	for _, step := range record.Rollback.Steps {
		if step.Status != rollout.RollbackStepCompleted && step.Status != rollout.RollbackStepFailed {
			t.Errorf("step %s is recorded as %s on a concluded rollback", step.Kind, step.Status)
		}
	}
	last := record.Rollback.Steps[len(record.Rollback.Steps)-1]
	if last.Kind != rollout.RollbackNotifyCompleted {
		t.Errorf("the rollback's last step is %s, want the completion announcement", last.Kind)
	}
	if last.Status != rollout.RollbackStepCompleted {
		t.Errorf("the completion announcement is recorded as %s, want completed", last.Status)
	}
}

// isNoDocuments reports whether reading a document failed because it does not exist yet.
func isNoDocuments(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}

// reconcileStep returns the plan's reconciliation step.
func reconcileStep(t *testing.T, steps []rollout.RollbackStepRecord) rollout.RollbackStepRecord {
	t.Helper()
	for _, step := range steps {
		if step.Kind == rollout.RollbackReconcileInventory {
			return step
		}
	}
	t.Fatal("the rollback plan carries no reconciliation step")
	return rollout.RollbackStepRecord{}
}

// compensatedDevicesOf returns every device a rollback compensates, in the order its plan
// compensates them: the membership of the waves the rollout dispatched, most recent first. The
// waves of one rollout target disjoint devices, so no device appears twice.
func compensatedDevicesOf(waves ...rollout.WaveRecord) []string {
	var devices []string
	for i := len(waves) - 1; i >= 0; i-- {
		devices = append(devices, waves[i].DeviceIDs...)
	}
	return devices
}

// waitForWaveStatus polls until a wave document carries the given status, returning it. It is the
// in-flight counterpart of the smoke's waitForWave, which waits for a decided wave: a scenario that
// has to act inside a wave's window needs to read the wave while it is still being measured.
func waitForWaveStatus(
	t *testing.T,
	coll *mongo.Collection,
	id string,
	status rollout.WaveStatus,
) rollout.WaveRecord {
	t.Helper()
	deadline := time.Now().Add(smokePollTimeout)
	for time.Now().Before(deadline) {
		record, ok := tryReadWave(t, coll, id)
		if ok && record.Status == status {
			return record
		}
		time.Sleep(smokePollInterval)
	}
	t.Fatalf("wave %s never reached %q within %v", id, status, smokePollTimeout)
	return rollout.WaveRecord{}
}

// readWave reads one wave document, failing when it does not exist.
func readWave(t *testing.T, coll *mongo.Collection, id string) rollout.WaveRecord {
	t.Helper()
	record, ok := tryReadWave(t, coll, id)
	if !ok {
		t.Fatalf("wave %s has no document", id)
	}
	return record
}

// tryReadWave reads one wave document, reporting whether it exists.
func tryReadWave(t *testing.T, coll *mongo.Collection, id string) (rollout.WaveRecord, bool) {
	t.Helper()
	var record rollout.WaveRecord
	err := coll.FindOne(context.Background(), map[string]any{"_id": id}).Decode(&record)
	switch {
	case err == nil:
		return record, true
	case isNoDocuments(err):
		return rollout.WaveRecord{}, false
	default:
		t.Fatalf("read wave %s: %v", id, err)
		return rollout.WaveRecord{}, false
	}
}
