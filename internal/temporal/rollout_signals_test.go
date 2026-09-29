package temporal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// fakeRolloutQueryClient is a hand-written rolloutQueryClient double recording query calls and
// answering with a scripted value or failure.
type fakeRolloutQueryClient struct {
	mu    sync.Mutex
	calls []queryCall
	value converter.EncodedValue
	err   error
}

func (c *fakeRolloutQueryClient) QueryWorkflow(
	_ context.Context,
	workflowID, runID, queryType string,
	_ ...any,
) (converter.EncodedValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, queryCall{workflowID: workflowID, queryType: queryType, runID: runID})
	if c.err != nil {
		return nil, c.err
	}
	return c.value, nil
}

func (c *fakeRolloutQueryClient) recorded() []queryCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]queryCall(nil), c.calls...)
}

// fakeRolloutSignalClient is a hand-written rolloutSignalClient double recording signals and
// reporting a scripted failure.
type fakeRolloutSignalClient struct {
	mu    sync.Mutex
	calls []rolloutSignalCall
	err   error
}

// rolloutSignalCall is one recorded signal delivery.
type rolloutSignalCall struct {
	workflowID string
	runID      string
	signalName string
	arg        any
}

func (c *fakeRolloutSignalClient) SignalWorkflow(
	_ context.Context, workflowID, runID, signalName string, arg any,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.calls = append(c.calls, rolloutSignalCall{
		workflowID: workflowID, runID: runID, signalName: signalName, arg: arg,
	})
	return nil
}

func (c *fakeRolloutSignalClient) recorded() []rolloutSignalCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rolloutSignalCall(nil), c.calls...)
}

func TestRolloutStatesState(t *testing.T) {
	t.Parallel()

	want := RolloutView{
		RolloutID:  "ro-1",
		Status:     rollout.RolloutAwaitingApproval,
		FirmwareID: "fw-1",
		Region:     "eu-west",
		Model:      "oak-s3",
		Waves: []WaveView{
			{Percent: 25, Status: rollout.WaveHealthy, SuccessRate: 0.99, TargetCount: 4},
			{Percent: 100, Status: rollout.WavePending},
		},
		Current: 1,
	}

	t.Run("reads the rollout's own state query", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutQueryClient{value: encoded(t, want)}
		got, err := NewRolloutStates(fc).State(context.Background(), "ro-1")
		if err != nil {
			t.Fatalf("State() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("view mismatch (-want +got):\n%s", diff)
		}

		calls := fc.recorded()
		if len(calls) != 1 {
			t.Fatalf("query calls = %d, want 1", len(calls))
		}
		if calls[0].workflowID != "rollout-ro-1" {
			t.Errorf("queried workflow = %q, want %q", calls[0].workflowID, "rollout-ro-1")
		}
		if calls[0].queryType != GetRolloutStateQueryType {
			t.Errorf("queried type = %q, want %q", calls[0].queryType, GetRolloutStateQueryType)
		}
		if calls[0].runID != "" {
			t.Errorf("queried run = %q, want the current execution", calls[0].runID)
		}
	})

	t.Run("a rollout with no execution is not found", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutQueryClient{err: serviceerror.NewNotFound("workflow not found")}
		_, err := NewRolloutStates(fc).State(context.Background(), "ro-9")
		if !errors.Is(err, ErrRolloutNotFound) {
			t.Errorf("State() error = %v, want ErrRolloutNotFound", err)
		}
		if !strings.Contains(err.Error(), "ro-9") {
			t.Errorf("State() error = %v, want it to name the rollout", err)
		}
	})

	t.Run("a query failure is wrapped with the rollout and workflow", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutQueryClient{err: errors.New("frontend is unavailable")}
		_, err := NewRolloutStates(fc).State(context.Background(), "ro-1")
		if err == nil {
			t.Fatal("State() error = nil, want a failure")
		}
		if errors.Is(err, ErrRolloutNotFound) {
			t.Errorf("State() error = %v, want a read failure rather than not-found", err)
		}
		for _, wantPart := range []string{"ro-1", "rollout-ro-1", "frontend is unavailable"} {
			if !strings.Contains(err.Error(), wantPart) {
				t.Errorf("State() error = %v, want it to name %q", err, wantPart)
			}
		}
	})

	t.Run("an undecodable answer is refused", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutQueryClient{value: encoded(t, 42)}
		_, err := NewRolloutStates(fc).State(context.Background(), "ro-1")
		if err == nil || !strings.Contains(err.Error(), "decode") {
			t.Errorf("State() error = %v, want a decode failure", err)
		}
	})

	t.Run("an empty rollout id is refused without a query", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutQueryClient{}
		if _, err := NewRolloutStates(fc).State(context.Background(), ""); err == nil {
			t.Fatal("State() error = nil, want a refusal")
		}
		if calls := fc.recorded(); len(calls) != 0 {
			t.Errorf("query calls = %d, want none for an empty rollout id", len(calls))
		}
	})
}

func TestRolloutSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		deliver func(*RolloutSignals, context.Context, string) error
		want    string
	}{
		{
			name:    "approve delivers approve_next_wave",
			deliver: (*RolloutSignals).Approve,
			want:    ApproveNextWaveSignalName,
		},
		{
			name:    "pause delivers pause_rollout",
			deliver: (*RolloutSignals).Pause,
			want:    PauseRolloutSignalName,
		},
		{
			name:    "resume delivers resume_rollout",
			deliver: (*RolloutSignals).Resume,
			want:    ResumeRolloutSignalName,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fc := &fakeRolloutSignalClient{}
			if err := tc.deliver(NewRolloutSignals(fc), context.Background(), "ro-1"); err != nil {
				t.Fatalf("deliver: %v", err)
			}

			calls := fc.recorded()
			if len(calls) != 1 {
				t.Fatalf("signal calls = %d, want 1", len(calls))
			}
			if calls[0].signalName != tc.want {
				t.Errorf("signal = %q, want %q", calls[0].signalName, tc.want)
			}
			if calls[0].workflowID != "rollout-ro-1" {
				t.Errorf("signalled workflow = %q, want %q", calls[0].workflowID, "rollout-ro-1")
			}
			if calls[0].runID != "" {
				t.Errorf("signalled run = %q, want the current execution", calls[0].runID)
			}
		})
	}

	t.Run("a rollout with no execution is not found", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutSignalClient{err: serviceerror.NewNotFound("workflow not found")}
		err := NewRolloutSignals(fc).Pause(context.Background(), "ro-9")
		if !errors.Is(err, ErrRolloutNotFound) {
			t.Errorf("Pause() error = %v, want ErrRolloutNotFound", err)
		}
	})

	t.Run("a delivery failure is wrapped with the rollout and signal", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutSignalClient{err: errors.New("frontend is unavailable")}
		err := NewRolloutSignals(fc).Resume(context.Background(), "ro-1")
		if err == nil {
			t.Fatal("Resume() error = nil, want a failure")
		}
		if errors.Is(err, ErrRolloutNotFound) {
			t.Errorf("Resume() error = %v, want a delivery failure rather than not-found", err)
		}
		for _, wantPart := range []string{"resume_rollout", "ro-1", "frontend is unavailable"} {
			if !strings.Contains(err.Error(), wantPart) {
				t.Errorf("Resume() error = %v, want it to name %q", err, wantPart)
			}
		}
	})

	t.Run("an empty rollout id is refused without a signal", func(t *testing.T) {
		t.Parallel()

		fc := &fakeRolloutSignalClient{}
		if err := NewRolloutSignals(fc).Approve(context.Background(), ""); err == nil {
			t.Fatal("Approve() error = nil, want a refusal")
		}
		if calls := fc.recorded(); len(calls) != 0 {
			t.Errorf("signal calls = %d, want none for an empty rollout id", len(calls))
		}
	})
}
