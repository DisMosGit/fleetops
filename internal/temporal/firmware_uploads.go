package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// uploadStartClient is the slice of the Temporal client the upload starter needs — starting, or
// attaching to, one workflow execution is the only call it makes.
type uploadStartClient interface {
	ExecuteWorkflow(
		ctx context.Context,
		options client.StartWorkflowOptions,
		workflow any,
		args ...any,
	) (client.WorkflowRun, error)
}

// FirmwareUploadStarter begins the workflow that owns a firmware upload, or attaches to the
// execution already running it, under the version-derived identity. It holds no state of its own,
// so any process with a Temporal client can drive an upload.
type FirmwareUploadStarter struct {
	client    uploadStartClient
	taskQueue string
}

// NewFirmwareUploadStarter returns a starter that begins uploads on taskQueue — the queue the
// process that received the binary polls.
func NewFirmwareUploadStarter(c uploadStartClient, taskQueue string) *FirmwareUploadStarter {
	return &FirmwareUploadStarter{client: c, taskQueue: taskQueue}
}

// StartOrAttach starts the upload's execution, or attaches to the one that already owns the
// version, reporting which happened. Exactly one caller starts it: the first attempt is a claim
// the backend arbitrates on the workflow id, so two concurrent uploads of one version collapse to
// one execution and one of them is told it attached. A version whose upload already concluded is
// refused with an error wrapping firmware.ErrVersionConflict rather than restarted, because its
// record is the answer.
func (s *FirmwareUploadStarter) StartOrAttach(
	ctx context.Context,
	req firmware.UploadRequest,
) (firmware.UploadRun, error) {
	if strings.TrimSpace(req.Version) == "" {
		return nil, errors.New("start firmware upload: version required")
	}
	policies := startPolicies(enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL)
	run, err := s.client.ExecuteWorkflow(ctx, s.options(req, policies), FirmwareWorkflowName, req)
	if err == nil {
		return firmwareUploadRun{run: run}, nil
	}
	if !isAlreadyStarted(err) {
		return nil, fmt.Errorf("start firmware upload %s: %w", req.Version, err)
	}

	// The version already has an execution. Attaching reaches it when it is still running, and
	// meets the reuse policy when it concluded.
	attached, attachErr := s.client.ExecuteWorkflow(ctx, s.options(req,
		startPolicies(enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)), FirmwareWorkflowName, req)
	if attachErr == nil {
		return firmwareUploadRun{run: attached, attached: true}, nil
	}
	if isAlreadyStarted(attachErr) {
		return nil, fmt.Errorf("start firmware upload %s: %w: the version's upload already concluded",
			req.Version, firmware.ErrVersionConflict)
	}
	return nil, fmt.Errorf("attach to firmware upload %s: %w", req.Version, attachErr)
}

// options returns the start options one attempt uses: the version's identity on the queue that
// owns the upload, with the given conflict and reuse policies.
func (s *FirmwareUploadStarter) options(
	req firmware.UploadRequest,
	policies uploadStartPolicies,
) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                                       FirmwareWorkflowID(req.Version),
		TaskQueue:                                s.taskQueue,
		WorkflowIDConflictPolicy:                 policies.conflict,
		WorkflowIDReusePolicy:                    policies.reuse,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
}

// uploadStartPolicies are the workflow-id policies one start attempt uses.
type uploadStartPolicies struct {
	conflict enumspb.WorkflowIdConflictPolicy
	reuse    enumspb.WorkflowIdReusePolicy
}

// startPolicies returns the policies a start attempt uses: a concluded upload of the version is
// never restarted, because its record is what the version resolved to.
func startPolicies(conflict enumspb.WorkflowIdConflictPolicy) uploadStartPolicies {
	return uploadStartPolicies{
		conflict: conflict,
		reuse:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
}

// firmwareUploadRun is a handle on one upload's execution.
type firmwareUploadRun struct {
	run      client.WorkflowRun
	attached bool
}

// Attached reports whether the execution was already running when the upload reached it.
func (u firmwareUploadRun) Attached() bool { return u.attached }

// Await waits for the upload to conclude and returns the state it recorded as its result.
func (u firmwareUploadRun) Await(ctx context.Context) (firmware.UploadState, error) {
	var state firmware.UploadState
	if err := u.run.Get(ctx, &state); err != nil {
		return firmware.UploadState{}, err
	}
	return state, nil
}

// uploadStateClient is the slice of the Temporal client the upload state reader needs: what an
// execution is doing, its running query, and its recorded result.
type uploadStateClient interface {
	DescribeWorkflowExecution(
		ctx context.Context, workflowID, runID string,
	) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	QueryWorkflow(
		ctx context.Context, workflowID, runID, queryType string, queryArgs ...any,
	) (converter.EncodedValue, error)
	GetWorkflow(ctx context.Context, workflowID, runID string) client.WorkflowRun
}

// FirmwareUploadStates reads one upload's state from the workflow that owns it: a running upload
// answers its state query, and a concluded one reports the outcome it recorded as its result.
// Reading a running upload never blocks it, which is what makes the state available while the
// binary is still streaming.
type FirmwareUploadStates struct {
	client uploadStateClient
}

// NewFirmwareUploadStates returns a reader over the Temporal client's describe, query, and result
// calls.
func NewFirmwareUploadStates(c uploadStateClient) *FirmwareUploadStates {
	return &FirmwareUploadStates{client: c}
}

// State returns the state of one version's upload, reading the running execution's state query or
// the outcome a concluded execution recorded. A version with no execution is reported as
// firmware.ErrNotFound, which is distinguishable from a read that failed.
func (s *FirmwareUploadStates) State(
	ctx context.Context, version string,
) (firmware.UploadState, error) {
	if strings.TrimSpace(version) == "" {
		return firmware.UploadState{}, errors.New("read firmware upload state: version required")
	}
	workflowID := FirmwareWorkflowID(version)
	desc, err := s.client.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		if isNotFound(err) {
			return firmware.UploadState{}, fmt.Errorf("read firmware upload %s state: %w",
				version, firmware.ErrNotFound)
		}
		return firmware.UploadState{}, fmt.Errorf("describe firmware upload %s: %w", version, err)
	}
	if desc.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		var state firmware.UploadState
		if err := s.client.GetWorkflow(ctx, workflowID, "").Get(ctx, &state); err != nil {
			return firmware.UploadState{}, fmt.Errorf("read firmware upload %s outcome: %w", version, err)
		}
		return state, nil
	}
	value, err := s.client.QueryWorkflow(ctx, workflowID, "", GetFirmwareUploadStateQueryType)
	if err != nil {
		return firmware.UploadState{}, fmt.Errorf("query firmware upload %s state: %w", version, err)
	}
	var state firmware.UploadState
	if err := value.Get(&state); err != nil {
		return firmware.UploadState{}, fmt.Errorf("decode firmware upload %s state: %w", version, err)
	}
	return state, nil
}

// isAlreadyStarted reports whether err is the backend refusing a second execution for a workflow
// id: the version's upload is already running, or one already concluded.
func isAlreadyStarted(err error) bool {
	_, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err)
	return ok
}
