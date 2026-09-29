package temporal

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// startClient is the slice of the Temporal client the rollout starter needs — starting a workflow
// execution is the only call it makes. client.Client satisfies it, and tests hand-write a fake.
type startClient interface {
	ExecuteWorkflow(
		ctx context.Context,
		options client.StartWorkflowOptions,
		workflow any,
		args ...any,
	) (client.WorkflowRun, error)
}

// RolloutStarter starts rollouts: one workflow execution per rollout id, under the rollout's
// derived workflow id, on the configured task queue, driving the configured canary policy. It
// holds no state of its own, so any binary with a Temporal client can start a rollout.
type RolloutStarter struct {
	client    startClient
	taskQueue string
	settings  RolloutSettings
}

// NewRolloutStarter returns a starter that begins rollouts on taskQueue under settings.
func NewRolloutStarter(c startClient, taskQueue string, settings RolloutSettings) *RolloutStarter {
	return &RolloutStarter{client: c, taskQueue: taskQueue, settings: settings}
}

// Start begins one rollout. It refuses a request that names no rollout, firmware, region, or
// model, and it starts the workflow under the rollout's derived id with the configured policy, so
// a rollout is always driven by exactly one execution: a second start for the same rollout id is
// refused with ErrRolloutExists rather than creating a run chain beside the first one's records.
func (s *RolloutStarter) Start(ctx context.Context, req RolloutRequest) error {
	for _, required := range []struct {
		field string
		value string
	}{
		{"rollout id", req.RolloutID},
		{"firmware id", req.FirmwareID},
		{"region", req.Region},
		{"model", req.Model},
	} {
		if required.value == "" {
			return fmt.Errorf("start rollout: %s required", required.field)
		}
	}

	// The workflow type is its registered name, not the function value: starting by reflection
	// would stamp every execution with the function name the UI shows.
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        RolloutWorkflowID(req.RolloutID),
		TaskQueue: s.taskQueue,
		// A rollout's record is keyed by its id, so a second run chain would fight the first
		// one over the same documents. A closed rollout is not restarted either: its recorded
		// outcome is the answer.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, RolloutWorkflowName, RolloutInput{RolloutRequest: req, Settings: s.settings})
	if err != nil {
		if _, duplicate := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); duplicate {
			return fmt.Errorf("start rollout %s: %w", req.RolloutID, ErrRolloutExists)
		}
		return fmt.Errorf("start rollout %s: %w", req.RolloutID, err)
	}
	return nil
}
