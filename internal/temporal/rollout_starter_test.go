package temporal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// fakeStartClient is a hand-written startClient double recording start calls.
type fakeStartClient struct {
	mu    sync.Mutex
	calls []startCall
	err   error
}

// startCall is one recorded workflow start.
type startCall struct {
	workflowID   string
	workflowType any
	options      client.StartWorkflowOptions
	arg          any
}

func (c *fakeStartClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType any,
	args ...any,
) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	var arg any
	if len(args) > 0 {
		arg = args[0]
	}
	c.calls = append(c.calls, startCall{
		workflowID:   options.ID,
		workflowType: workflowType,
		options:      options,
		arg:          arg,
	})
	return nil, nil
}

func (c *fakeStartClient) recorded() []startCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]startCall(nil), c.calls...)
}

func TestRolloutStarterStart(t *testing.T) {
	t.Parallel()

	settings := rolloutTestSettings()
	req := RolloutRequest{
		RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
	}

	t.Run("starts the rollout under its derived identity", func(t *testing.T) {
		t.Parallel()

		fc := &fakeStartClient{}
		starter := NewRolloutStarter(fc, "fleetops", settings)
		if err := starter.Start(context.Background(), req); err != nil {
			t.Fatalf("Start() error = %v", err)
		}

		calls := fc.recorded()
		if len(calls) != 1 {
			t.Fatalf("start calls = %d, want 1", len(calls))
		}
		call := calls[0]
		if call.workflowID != "rollout-ro-1" {
			t.Errorf("workflow id = %q, want %q", call.workflowID, RolloutWorkflowID("ro-1"))
		}
		if call.workflowType != RolloutWorkflowName {
			t.Errorf("workflow type = %v, want the registered name %q", call.workflowType, RolloutWorkflowName)
		}
		if call.options.TaskQueue != "fleetops" {
			t.Errorf("task queue = %q, want fleetops", call.options.TaskQueue)
		}
		// One rollout is one execution, ever: a second start for the same rollout id must not
		// create a run chain beside the records the first one owns.
		if call.options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
			t.Errorf("workflow id reuse policy = %v, want REJECT_DUPLICATE",
				call.options.WorkflowIDReusePolicy)
		}
		// The configured policy reaches the workflow's start path: the caller names what to
		// deploy, the worker's configuration decides how.
		want := RolloutInput{RolloutRequest: req, Settings: settings}
		got, ok := call.arg.(RolloutInput)
		if !ok {
			t.Fatalf("start argument has type %T, want RolloutInput", call.arg)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("start input mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("refuses a request that names no rollout", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name    string
			mutate  func(*RolloutRequest)
			wantErr string
		}{
			{name: "no rollout id", mutate: func(r *RolloutRequest) { r.RolloutID = "" }, wantErr: "rollout id"},
			{name: "no firmware id", mutate: func(r *RolloutRequest) { r.FirmwareID = "" }, wantErr: "firmware id"},
			{name: "no region", mutate: func(r *RolloutRequest) { r.Region = "" }, wantErr: "region"},
			{name: "no model", mutate: func(r *RolloutRequest) { r.Model = "" }, wantErr: "model"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				broken := req
				tc.mutate(&broken)
				fc := &fakeStartClient{}
				err := NewRolloutStarter(fc, "fleetops", settings).Start(context.Background(), broken)
				if err == nil {
					t.Fatal("Start() = nil error, want the request refused")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Start() error = %q, want it to name %q", err, tc.wantErr)
				}
				if calls := fc.recorded(); len(calls) != 0 {
					t.Errorf("start calls = %d, want none for a refused request", len(calls))
				}
			})
		}
	})

	t.Run("reports a start failure", func(t *testing.T) {
		t.Parallel()

		fc := &fakeStartClient{err: errors.New("temporal is unreachable")}
		err := NewRolloutStarter(fc, "fleetops", settings).Start(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "ro-1") {
			t.Errorf("Start() error = %v, want it to name the rollout that could not start", err)
		}
	})

	t.Run("a rollout id already in use is refused as a duplicate", func(t *testing.T) {
		t.Parallel()

		// What a second start for a live rollout id looks like from the client: the reuse
		// policy refused it. The caller gets a sentinel it can map onto its own surface
		// instead of having to read the backend's error.
		fc := &fakeStartClient{
			err: serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already exists", "", ""),
		}
		err := NewRolloutStarter(fc, "fleetops", settings).Start(context.Background(), req)
		if !errors.Is(err, ErrRolloutExists) {
			t.Errorf("Start() error = %v, want ErrRolloutExists", err)
		}
		if !strings.Contains(err.Error(), "ro-1") {
			t.Errorf("Start() error = %v, want it to name the rollout", err)
		}
	})
}
