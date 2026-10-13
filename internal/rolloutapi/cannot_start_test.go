package rolloutapi

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// fixedFirmwareRegistry is a FirmwareSource over the records a test gives it. An id it does not
// hold fails the way the firmware store fails one, so load-firmware sees a registry that cannot
// serve the request.
type fixedFirmwareRegistry struct {
	records map[string]firmware.Record
}

func (r *fixedFirmwareRegistry) Metadata(_ context.Context, id string) (firmware.Record, error) {
	rec, ok := r.records[id]
	if !ok {
		return firmware.Record{}, fmt.Errorf("load firmware %s: %w", id, firmware.ErrNotFound)
	}
	return rec, nil
}

// recordingRolloutRecorder is a RolloutRecorder keeping the rollout documents the workflow writes,
// so a test can assert what the rollout recorded and not only what it reported.
type recordingRolloutRecorder struct {
	mu      sync.Mutex
	records []rollout.RolloutRecord
}

func (r *recordingRolloutRecorder) RecordRollout(_ context.Context, rec rollout.RolloutRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}

func (r *recordingRolloutRecorder) recorded() []rollout.RolloutRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rollout.RolloutRecord(nil), r.records...)
}

// workflowEnvClient is the start client the real RolloutStarter calls: it executes the workflow in
// the SDK's in-process test environment instead of against a Temporal frontend. The transport is
// the only thing it stands in for — the handler, the starter, the workflow, and its activities are
// the production ones.
type workflowEnvClient struct {
	env *testsuite.TestWorkflowEnvironment
}

func (c *workflowEnvClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType any,
	args ...any,
) (client.WorkflowRun, error) {
	c.env.ExecuteWorkflow(workflowType, args...)
	if err := c.env.GetWorkflowError(); err != nil {
		return nil, fmt.Errorf("execute rollout workflow %s: %w", options.ID, err)
	}
	return &fakeWorkflowRun{id: options.ID}, nil
}

// newCannotStartEnv returns a test environment hosting the real RolloutWorkflow with the real
// load-firmware and record-rollout-state activities over source and recorder. The cannot-start
// path calls those two activities and no others.
func newCannotStartEnv(
	t *testing.T,
	source temporal.FirmwareSource,
	recorder temporal.RolloutRecorder,
) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(discardWorkflowLogger{})
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(time.Minute)
	env.RegisterWorkflowWithOptions(temporal.RolloutWorkflow,
		workflow.RegisterOptions{Name: temporal.RolloutWorkflowName})
	env.RegisterActivityWithOptions(temporal.NewLoadFirmwareActivity(source),
		activity.RegisterOptions{Name: temporal.LoadFirmwareActivityName})
	env.RegisterActivityWithOptions(temporal.NewRecordRolloutActivity(recorder),
		activity.RegisterOptions{Name: temporal.RecordRolloutStateActivityName})
	return env
}

// discardWorkflowLogger silences the test environment's workflow logger: every rollout decision is
// logged, and that output would drown the test.
type discardWorkflowLogger struct{}

func (discardWorkflowLogger) Debug(string, ...any) {}
func (discardWorkflowLogger) Info(string, ...any)  {}
func (discardWorkflowLogger) Warn(string, ...any)  {}
func (discardWorkflowLogger) Error(string, ...any) {}

// queryRolloutState returns the state view the executed rollout reports.
func queryRolloutState(t *testing.T, env *testsuite.TestWorkflowEnvironment) temporal.RolloutView {
	t.Helper()

	value, err := env.QueryWorkflow(temporal.GetRolloutStateQueryType)
	if err != nil {
		t.Fatalf("query %s: %v", temporal.GetRolloutStateQueryType, err)
	}
	var view temporal.RolloutView
	if err := value.Get(&view); err != nil {
		t.Fatalf("decode %s answer: %v", temporal.GetRolloutStateQueryType, err)
	}
	return view
}

// TestHandlerStartAcceptsARolloutThatCannotStart backs the rollout-api scenario "A rollout that
// cannot start is still accepted": a start request names a firmware the registry does not hold, or
// one that does not target the selector's model, the request is answered 202, and the rollout it
// began records its own terminal failure. The HTTP half and the workflow half are joined here in
// one test: the handler runs the real starter, which executes the real rollout workflow over the
// real load-firmware and record-rollout-state activities.
func TestHandlerStartAcceptsARolloutThatCannotStart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		registry *fixedFirmwareRegistry
		want     temporal.RolloutOutcome
	}{
		{
			name:     "a firmware the registry does not hold",
			registry: &fixedFirmwareRegistry{},
			want:     temporal.OutcomeFirmwareUnknown,
		},
		{
			name: "a firmware that does not target the selector's model",
			registry: &fixedFirmwareRegistry{records: map[string]firmware.Record{
				"fw-1": {
					ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3-mini"},
					Checksum: "sha256:0f1e2d",
				},
			}},
			want: temporal.OutcomeFirmwareMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := &recordingRolloutRecorder{}
			env := newCannotStartEnv(t, tc.registry, recorder)
			handler := NewHandler(
				temporal.NewRolloutStarter(&workflowEnvClient{env: env}, "rollout-queue",
					testRolloutSettings(t)),
				&fakeStates{}, &fakeSignals{}, testLogger(),
			)

			rec := do(t, handler, http.MethodPost, "/api/rollouts",
				`{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`)

			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("rollout workflow error: %v", err)
			}
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
			}

			// The rollout the API accepted recorded its own terminal failure: the failure
			// belongs to the rollout, not to the request.
			view := queryRolloutState(t, env)
			if view.Status != rollout.RolloutFailed || view.Outcome != tc.want {
				t.Errorf("rollout state = %s/%s, want failed/%s", view.Status, view.Outcome, tc.want)
			}
			if view.EndedBy != "" {
				t.Errorf("ended by %q, want no wave to have ended a rollout that never started",
					view.EndedBy)
			}
			for position, wave := range view.Waves {
				if wave.Status != rollout.WavePending {
					t.Errorf("wave %d is %s, want it never to have started", position, wave.Status)
				}
			}
			records := recorder.recorded()
			if len(records) == 0 {
				t.Fatal("the rollout recorded no state, want its own terminal failure recorded")
			}
			last := records[len(records)-1]
			if last.ID != "ro-1" || last.FirmwareID != "fw-1" ||
				last.Status != rollout.RolloutFailed {
				t.Errorf("last recorded rollout = %+v, want ro-1/fw-1 recorded as failed", last)
			}
		})
	}
}
