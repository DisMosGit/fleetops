package temporal

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
)

// ErrDeviceNotFound reports a device with no workflow execution: the fleet has never heard from
// it, so there is no authoritative state to read.
var ErrDeviceNotFound = errors.New("device workflow not found")

// queryClient is the slice of the Temporal client the device-state reader needs — reading one
// workflow's query answer is the only call it makes. client.Client satisfies it, and tests
// hand-write a fake. The run id parameter mirrors the client's own signature; the reader passes
// an empty one, which addresses the workflow's current execution.
type queryClient interface {
	QueryWorkflow(
		ctx context.Context,
		workflowID, runID, queryType string,
		queryArgs ...any,
	) (converter.EncodedValue, error)
}

// DeviceStates reads the authoritative state of a device's entity workflow. It is the device
// side of the same contract the entity's get-state query serves: the workflow that owns the
// device answers, and nothing else does.
type DeviceStates struct {
	client queryClient
}

// NewDeviceStates returns a reader over the Temporal client's query call.
func NewDeviceStates(c queryClient) *DeviceStates {
	return &DeviceStates{client: c}
}

// State returns the state of one device's entity workflow, reading the device's get-state query
// on the workflow id its device id derives. A device with no workflow execution — a device the
// fleet has never heard from — is reported as not found, which is distinguishable from a read
// that failed; a device id that derives no workflow id is refused rather than addressed.
func (s *DeviceStates) State(ctx context.Context, deviceID string) (State, error) {
	if deviceID == "" {
		return State{}, errors.New("read device state: device id required")
	}
	workflowID := DeviceWorkflowID(deviceID)
	// An empty run id addresses the workflow's current execution, which is the one the entity
	// is driving the device from.
	value, err := s.client.QueryWorkflow(ctx, workflowID, "", GetStateQueryType)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return State{}, fmt.Errorf("read device %s state: %w", deviceID, ErrDeviceNotFound)
		}
		return State{}, fmt.Errorf("read device %s state on %s: %w", deviceID, workflowID, err)
	}
	var state State
	if err := value.Get(&state); err != nil {
		return State{}, fmt.Errorf("decode device %s state: %w", deviceID, err)
	}
	return state, nil
}
