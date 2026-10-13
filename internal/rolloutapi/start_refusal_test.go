package rolloutapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/DisMosGit/fleetops/internal/temporal"
)

// testRolloutSettings is a valid canary policy for a test that needs the real starter but never
// drives a rollout: one full wave, and windows no test waits on.
func testRolloutSettings(t *testing.T) temporal.RolloutSettings {
	t.Helper()

	settings, err := temporal.NewRolloutSettings(time.Minute, time.Minute, time.Second,
		[]temporal.RolloutWave{{Percent: 100}})
	if err != nil {
		t.Fatalf("build rollout settings: %v", err)
	}
	return settings
}

// recordedStart is one ExecuteWorkflow call as the stand-in frontend saw it.
type recordedStart struct {
	options      client.StartWorkflowOptions
	workflowType any
	input        any
}

// fakeTemporalFrontend stands in for Temporal's frontend on the start path: it creates at most one
// execution per workflow id, and refuses a start for an id that already has a live execution when
// the caller's reuse policy asks the server to. That is the server behavior "one rollout, one
// execution" rests on, so a test can observe that a repeated start created no second execution
// without running a server.
type fakeTemporalFrontend struct {
	mu sync.Mutex
	// calls records every start call, refused ones included.
	calls []recordedStart
	// executions counts the executions the calls would have created, by workflow id.
	executions map[string]int
}

// ExecuteWorkflow implements the start seam *temporal.RolloutStarter consumes.
func (f *fakeTemporalFrontend) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType any,
	args ...any,
) (client.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var input any
	if len(args) > 0 {
		input = args[0]
	}
	f.calls = append(f.calls, recordedStart{options: options, workflowType: workflowType, input: input})

	if f.executions[options.ID] > 0 && refusesDuplicate(options.WorkflowIDReusePolicy) {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf("workflow execution for %s already started", options.ID), options.ID, "")
	}
	f.executions[options.ID]++
	return &fakeWorkflowRun{id: options.ID}, nil
}

// refusesDuplicate reports whether a reuse policy makes a real frontend refuse a start for a
// workflow id that already has a live execution: REJECT_DUPLICATE always does, and
// ALLOW_DUPLICATE_FAILED_ONLY does while that execution has not failed. The unspecified policy is
// what the server reads as ALLOW_DUPLICATE, so it creates a second run chain.
func refusesDuplicate(policy enumspb.WorkflowIdReusePolicy) bool {
	switch policy {
	case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		return true
	default:
		return false
	}
}

// starts returns every start call the frontend saw, in order.
func (f *fakeTemporalFrontend) starts() []recordedStart {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedStart(nil), f.calls...)
}

// executionCounts returns how many executions exist per workflow id.
func (f *fakeTemporalFrontend) executionCounts() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := make(map[string]int, len(f.executions))
	for id, execs := range f.executions {
		counts[id] = execs
	}
	return counts
}

// fakeWorkflowRun is the handle a started execution hands back. The starter discards it, so it only
// has to satisfy the client's contract.
type fakeWorkflowRun struct{ id string }

func (r *fakeWorkflowRun) GetID() string { return r.id }

func (r *fakeWorkflowRun) GetRunID() string { return "" }

func (r *fakeWorkflowRun) GetFirstExecutionRunID() string { return "" }

func (r *fakeWorkflowRun) Get(context.Context, any) error { return nil }

func (r *fakeWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return nil
}

// TestHandlerStartRefusesASecondExecution backs the rollout-api scenario "A repeated start is
// refused": a client posts a start request for a rollout id that already has a workflow execution,
// the response is 409, and the rollout still has exactly one execution. The handler and the
// starter are the real ones and only the frontend is a stand-in, so the test fails if the starter
// stops asking the server to reject a duplicate id, if the handler stops answering the refusal
// with 409, or if a second start is let through.
func TestHandlerStartRefusesASecondExecution(t *testing.T) {
	t.Parallel()

	frontend := &fakeTemporalFrontend{executions: map[string]int{}}
	handler := NewHandler(
		temporal.NewRolloutStarter(frontend, "rollout-queue", testRolloutSettings(t)),
		&fakeStates{}, &fakeSignals{}, testLogger(),
	)
	body := `{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`

	first := do(t, handler, http.MethodPost, "/api/rollouts", body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first start status = %d, want %d (body %s)",
			first.Code, http.StatusAccepted, first.Body)
	}
	second := do(t, handler, http.MethodPost, "/api/rollouts", body)
	if second.Code != http.StatusConflict {
		t.Fatalf("second start status = %d, want %d (body %s)",
			second.Code, http.StatusConflict, second.Body)
	}
	if reason := decode[errorResponse](t, second).Error; !strings.Contains(reason, "ro-1") {
		t.Errorf("conflict reason = %q, want it to name the rollout", reason)
	}

	// Both requests reached the start path: the second was refused by the server's duplicate
	// rule, not by something that never tried to start it.
	starts := frontend.starts()
	if len(starts) != 2 {
		t.Fatalf("start calls = %d, want 2", len(starts))
	}
	wantID := temporal.RolloutWorkflowID("ro-1")
	counts := frontend.executionCounts()
	if len(counts) != 1 || counts[wantID] != 1 {
		t.Fatalf("workflow executions = %v, want exactly one under %s", counts, wantID)
	}
	for position, start := range starts {
		if start.options.ID != wantID {
			t.Errorf("start %d workflow id = %q, want %q", position, start.options.ID, wantID)
		}
		if start.options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
			t.Errorf("start %d reuse policy = %v, want REJECT_DUPLICATE",
				position, start.options.WorkflowIDReusePolicy)
		}
	}
}
