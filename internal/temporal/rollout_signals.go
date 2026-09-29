package temporal

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
)

// The sentinels a caller of the rollout adapters maps onto its own surface: a rollout with no
// workflow execution, and a rollout id that already has one. Both are expected outcomes of an
// operator's request rather than failures, so a caller can tell them from a backend failure.
var (
	// ErrRolloutNotFound reports a rollout with no workflow execution: nothing has started it,
	// so there is no state to read and no signal to deliver.
	ErrRolloutNotFound = errors.New("rollout workflow not found")
	// ErrRolloutExists reports a rollout id that already has a workflow execution. The id is the
	// request's idempotency key, so a repeated start is refused rather than creating a second
	// run chain beside the first one's records.
	ErrRolloutExists = errors.New("rollout workflow already exists")
)

// rolloutQueryClient is the slice of the Temporal client the rollout state reader needs — reading
// one workflow's query answer is the only call it makes.
type rolloutQueryClient interface {
	QueryWorkflow(
		ctx context.Context,
		workflowID, runID, queryType string,
		queryArgs ...any,
	) (converter.EncodedValue, error)
}

// RolloutStates reads one rollout's authoritative state from its workflow execution. It answers
// with the workflow's own state view, so the operator surface cannot drift from what the rollout
// decides it is.
type RolloutStates struct {
	client rolloutQueryClient
}

// NewRolloutStates returns a reader over the Temporal client's query call.
func NewRolloutStates(c rolloutQueryClient) *RolloutStates {
	return &RolloutStates{client: c}
}

// State returns the state one rollout's workflow execution reports, reading the rollout's
// get-rollout-state query on the workflow id its rollout id derives. A rollout with no workflow
// execution is reported as ErrRolloutNotFound, which is distinguishable from a read that failed.
func (s *RolloutStates) State(ctx context.Context, rolloutID string) (RolloutView, error) {
	if rolloutID == "" {
		return RolloutView{}, errors.New("read rollout state: rollout id required")
	}
	workflowID := RolloutWorkflowID(rolloutID)
	// An empty run id addresses the workflow's current execution.
	value, err := s.client.QueryWorkflow(ctx, workflowID, "", GetRolloutStateQueryType)
	if err != nil {
		if isNotFound(err) {
			return RolloutView{}, fmt.Errorf("read rollout %s state: %w", rolloutID, ErrRolloutNotFound)
		}
		return RolloutView{}, fmt.Errorf("read rollout %s state on %s: %w", rolloutID, workflowID, err)
	}
	var view RolloutView
	if err := value.Get(&view); err != nil {
		return RolloutView{}, fmt.Errorf("decode rollout %s state: %w", rolloutID, err)
	}
	return view, nil
}

// rolloutSignalClient is the slice of the Temporal client the rollout signal sender needs —
// signalling a running workflow is the only call it makes.
type rolloutSignalClient interface {
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
}

// RolloutSignals delivers the operator's commands to a rollout's workflow execution. It is the
// only signaling path the operator surface uses: what the HTTP API sends are the workflow's own
// signals, so an operator and the Temporal CLI drive a rollout the same way.
type RolloutSignals struct {
	client rolloutSignalClient
}

// NewRolloutSignals returns a sender over the Temporal client's signal call.
func NewRolloutSignals(c rolloutSignalClient) *RolloutSignals {
	return &RolloutSignals{client: c}
}

// Approve delivers approve_next_wave, authorizing the rollout's next gated wave.
func (s *RolloutSignals) Approve(ctx context.Context, rolloutID string) error {
	return s.send(ctx, rolloutID, ApproveNextWaveSignalName, ApproveNextWaveSignal{})
}

// Pause delivers pause_rollout, holding the rollout at its next wave boundary.
func (s *RolloutSignals) Pause(ctx context.Context, rolloutID string) error {
	return s.send(ctx, rolloutID, PauseRolloutSignalName, PauseRolloutSignal{})
}

// Resume delivers resume_rollout, letting a paused rollout continue from where it stopped.
func (s *RolloutSignals) Resume(ctx context.Context, rolloutID string) error {
	return s.send(ctx, rolloutID, ResumeRolloutSignalName, ResumeRolloutSignal{})
}

// send delivers one signal to one rollout's workflow execution, reporting a rollout with no
// execution as ErrRolloutNotFound.
func (s *RolloutSignals) send(ctx context.Context, rolloutID, signalName string, payload any) error {
	if rolloutID == "" {
		return fmt.Errorf("signal %s: rollout id required", signalName)
	}
	workflowID := RolloutWorkflowID(rolloutID)
	if err := s.client.SignalWorkflow(ctx, workflowID, "", signalName, payload); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("signal %s to rollout %s: %w", signalName, rolloutID, ErrRolloutNotFound)
		}
		return fmt.Errorf("signal %s to rollout %s on %s: %w", signalName, rolloutID, workflowID, err)
	}
	return nil
}

// isNotFound reports whether a client call failed because its target workflow execution does not
// exist. A query against a workflow that was never started and a signal to one are both reported
// this way, and neither is a failure of the backend.
func isNotFound(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}
