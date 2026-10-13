package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// uploadTestQueue is the queue the starter is configured with in these tests; a value distinct
// from any default, so an assertion says which field the option came from.
const uploadTestQueue = "fleetops-controlplane"

// uploadTestRequest is the upload the starter tests start.
func uploadTestRequest() firmware.UploadRequest {
	return firmware.UploadRequest{
		Version: "2.0.0", Models: []string{"oak-s3"}, SpoolPath: "/spool/upload-1",
	}
}

// fakeUploadStarts is a hand-written uploadStartClient double replaying one outcome per call: the
// nth call returns the nth scripted error, or the nth scripted run when that error is nil.
type fakeUploadStarts struct {
	mu    sync.Mutex
	errs  []error
	runs  []client.WorkflowRun
	calls []startCall
}

// ExecuteWorkflow records the start and replays this call's scripted outcome.
func (c *fakeUploadStarts) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType any,
	args ...any,
) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	i := len(c.calls) - 1
	if i < len(c.errs) && c.errs[i] != nil {
		return nil, c.errs[i]
	}
	if i < len(c.runs) {
		return c.runs[i], nil
	}
	return nil, nil
}

// recorded returns the recorded starts.
func (c *fakeUploadStarts) recorded() []startCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]startCall(nil), c.calls...)
}

// alreadyStarted is the backend refusing a second execution for a workflow id.
func alreadyStarted() error {
	return &serviceerror.WorkflowExecutionAlreadyStarted{Message: "workflow execution already started"}
}

// fakeWorkflowRun is a hand-written client.WorkflowRun double answering Get with a scripted
// result or failure. The other calls are unused by the package under test.
type fakeWorkflowRun struct {
	result firmware.UploadState
	err    error
}

// GetID returns an empty workflow id.
func (r fakeWorkflowRun) GetID() string { return "" }

// GetRunID returns an empty run id.
func (r fakeWorkflowRun) GetRunID() string { return "" }

// GetFirstExecutionRunID returns an empty run id.
func (r fakeWorkflowRun) GetFirstExecutionRunID() string { return "" }

// Get fills valuePtr with the scripted result, or returns the scripted failure.
func (r fakeWorkflowRun) Get(_ context.Context, valuePtr any) error {
	if r.err != nil {
		return r.err
	}
	state, ok := valuePtr.(*firmware.UploadState)
	if !ok {
		return fmt.Errorf("fake workflow run: unsupported result type %T", valuePtr)
	}
	*state = r.result
	return nil
}

// GetWithOptions fills valuePtr with the scripted result.
func (r fakeWorkflowRun) GetWithOptions(
	ctx context.Context, valuePtr any, _ client.WorkflowRunGetOptions,
) error {
	return r.Get(ctx, valuePtr)
}

func TestFirmwareUploadStarter(t *testing.T) {
	t.Parallel()

	t.Run("starts the upload under the version's identity", func(t *testing.T) {
		t.Parallel()

		starts := &fakeUploadStarts{runs: []client.WorkflowRun{fakeWorkflowRun{}}}
		run, err := NewFirmwareUploadStarter(starts, uploadTestQueue).
			StartOrAttach(context.Background(), uploadTestRequest())
		if err != nil {
			t.Fatalf("StartOrAttach() error = %v", err)
		}
		if run.Attached() {
			t.Error("Attached() = true, want this upload to have started the execution")
		}

		calls := starts.recorded()
		if len(calls) != 1 {
			t.Fatalf("starts = %d, want 1", len(calls))
		}
		got := calls[0]
		if got.workflowID != "firmware-upload-2.0.0" {
			t.Errorf("workflow id = %q, want the version's identity", got.workflowID)
		}
		if got.workflowType != FirmwareWorkflowName {
			t.Errorf("workflow type = %v, want the registered name %q", got.workflowType, FirmwareWorkflowName)
		}
		if got.options.TaskQueue != uploadTestQueue {
			t.Errorf("task queue = %q, want %q", got.options.TaskQueue, uploadTestQueue)
		}
		if got.options.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
			t.Errorf("conflict policy = %v, want the first attempt to claim the version",
				got.options.WorkflowIDConflictPolicy)
		}
		if got.options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
			t.Errorf("reuse policy = %v, want a concluded upload never restarted",
				got.options.WorkflowIDReusePolicy)
		}
		if !got.options.WorkflowExecutionErrorWhenAlreadyStarted {
			t.Error("WorkflowExecutionErrorWhenAlreadyStarted = false, want a taken version reported")
		}
		req, ok := got.arg.(firmware.UploadRequest)
		if !ok {
			t.Fatalf("start argument has type %T, want the upload request", got.arg)
		}
		if diff := cmp.Diff(uploadTestRequest(), req); diff != "" {
			t.Errorf("start argument mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a second upload attaches to the running execution", func(t *testing.T) {
		t.Parallel()

		starts := &fakeUploadStarts{
			errs: []error{alreadyStarted(), nil},
			runs: []client.WorkflowRun{nil, fakeWorkflowRun{}},
		}
		run, err := NewFirmwareUploadStarter(starts, uploadTestQueue).
			StartOrAttach(context.Background(), uploadTestRequest())
		if err != nil {
			t.Fatalf("StartOrAttach() error = %v", err)
		}
		if !run.Attached() {
			t.Error("Attached() = false, want the upload to have reached the existing execution")
		}

		calls := starts.recorded()
		if len(calls) != 2 {
			t.Fatalf("starts = %d, want a claim attempt and an attach", len(calls))
		}
		if calls[0].workflowID != calls[1].workflowID {
			t.Errorf("attach targeted %q, want the claimed id %q", calls[1].workflowID, calls[0].workflowID)
		}
		if want := enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING; calls[1].options.WorkflowIDConflictPolicy != want {
			t.Errorf("attach conflict policy = %v, want %v", calls[1].options.WorkflowIDConflictPolicy, want)
		}
		if want := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE; calls[1].options.WorkflowIDReusePolicy != want {
			t.Errorf("attach reuse policy = %v, want %v", calls[1].options.WorkflowIDReusePolicy, want)
		}
	})

	t.Run("a concluded upload is refused as the version conflict", func(t *testing.T) {
		t.Parallel()

		starts := &fakeUploadStarts{errs: []error{alreadyStarted(), alreadyStarted()}}
		_, err := NewFirmwareUploadStarter(starts, uploadTestQueue).
			StartOrAttach(context.Background(), uploadTestRequest())
		if !errors.Is(err, firmware.ErrVersionConflict) {
			t.Fatalf("StartOrAttach() error = %v, want ErrVersionConflict", err)
		}
		if len(starts.recorded()) != 2 {
			t.Errorf("starts = %d, want the attach attempted before the version was refused",
				len(starts.recorded()))
		}
	})

	t.Run("a backend failure is reported as itself", func(t *testing.T) {
		t.Parallel()

		starts := &fakeUploadStarts{errs: []error{errors.New("frontend unavailable")}}
		_, err := NewFirmwareUploadStarter(starts, uploadTestQueue).
			StartOrAttach(context.Background(), uploadTestRequest())
		if err == nil {
			t.Fatal("StartOrAttach() error = nil, want the backend failure")
		}
		if errors.Is(err, firmware.ErrVersionConflict) {
			t.Errorf("StartOrAttach() error = %v, want no version conflict for a backend failure", err)
		}
		if !strings.Contains(err.Error(), "frontend unavailable") {
			t.Errorf("StartOrAttach() error = %q, want the backend failure named", err)
		}
	})

	t.Run("a version is required", func(t *testing.T) {
		t.Parallel()

		starts := &fakeUploadStarts{}
		_, err := NewFirmwareUploadStarter(starts, uploadTestQueue).
			StartOrAttach(context.Background(), firmware.UploadRequest{SpoolPath: "/spool/upload-1"})
		if err == nil {
			t.Fatal("StartOrAttach() error = nil, want the missing version refused")
		}
		if len(starts.recorded()) != 0 {
			t.Errorf("starts = %d, want none for an upload with no version", len(starts.recorded()))
		}
	})
}

// TestFirmwareUploadRunAwait pins the handle's wait: it reports the state the upload recorded as
// its result, and a wait that failed is reported rather than answered with an empty state.
func TestFirmwareUploadRunAwait(t *testing.T) {
	t.Parallel()

	t.Run("a concluded upload reports its outcome", func(t *testing.T) {
		t.Parallel()

		want := firmware.UploadState{
			Version: "2.0.0", State: firmware.UploadAvailable, Concluded: true,
			FirmwareID: "fw-1", Checksum: "sum", Size: 7,
		}
		run := firmwareUploadRun{run: fakeWorkflowRun{result: want}}

		got, err := run.Await(context.Background())
		if err != nil {
			t.Fatalf("Await() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("outcome mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a failed wait is reported", func(t *testing.T) {
		t.Parallel()

		run := firmwareUploadRun{run: fakeWorkflowRun{err: errors.New("deadline exceeded")}}
		if _, err := run.Await(context.Background()); err == nil {
			t.Fatal("Await() error = nil, want the wait failure")
		}
	})
}

// fakeUploadStateClient is a hand-written uploadStateClient double replaying a scripted
// description, query answer, and recorded result.
type fakeUploadStateClient struct {
	desc      *workflowservice.DescribeWorkflowExecutionResponse
	descErr   error
	value     converter.EncodedValue
	queryErr  error
	run       client.WorkflowRun
	mu        sync.Mutex
	described []string
	queried   []string
	got       []string
}

// DescribeWorkflowExecution returns the scripted description.
func (c *fakeUploadStateClient) DescribeWorkflowExecution(
	_ context.Context, workflowID, runID string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.described = append(c.described, workflowID+"/"+runID)
	c.mu.Unlock()
	if c.descErr != nil {
		return nil, c.descErr
	}
	return c.desc, nil
}

// QueryWorkflow returns the scripted query answer.
func (c *fakeUploadStateClient) QueryWorkflow(
	_ context.Context, workflowID, runID, queryType string, _ ...any,
) (converter.EncodedValue, error) {
	c.mu.Lock()
	c.queried = append(c.queried, workflowID+"/"+queryType)
	c.mu.Unlock()
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.value, nil
}

// GetWorkflow returns the scripted result handle.
func (c *fakeUploadStateClient) GetWorkflow(_ context.Context, workflowID, runID string) client.WorkflowRun {
	c.mu.Lock()
	c.got = append(c.got, workflowID+"/"+runID)
	c.mu.Unlock()
	return c.run
}

// status returns a description reporting one execution status.
func status(s enumspb.WorkflowExecutionStatus) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: s},
	}
}

func TestFirmwareUploadStates(t *testing.T) {
	t.Parallel()

	t.Run("a running upload answers its state query", func(t *testing.T) {
		t.Parallel()

		want := firmware.UploadState{Version: "2.0.0", State: firmware.UploadStoring}
		states := &fakeUploadStateClient{
			desc:  status(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			value: encoded(t, want),
		}

		got, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("State() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
		wantQuery := "firmware-upload-2.0.0/" + GetFirmwareUploadStateQueryType
		if len(states.queried) != 1 || states.queried[0] != wantQuery {
			t.Errorf("queries = %v, want [%s]", states.queried, wantQuery)
		}
		if len(states.got) != 0 {
			t.Errorf("result reads = %v, want none while the upload runs", states.got)
		}
	})

	t.Run("a concluded upload reports its recorded outcome", func(t *testing.T) {
		t.Parallel()

		want := firmware.UploadState{
			Version: "2.0.0", State: firmware.UploadAvailable, Concluded: true, FirmwareID: "fw-1",
		}
		states := &fakeUploadStateClient{
			desc: status(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
			run:  fakeWorkflowRun{result: want},
		}

		got, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0")
		if err != nil {
			t.Fatalf("State() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}
		if len(states.queried) != 0 {
			t.Errorf("queries = %v, want none for a concluded upload", states.queried)
		}
		wantRead := "firmware-upload-2.0.0/"
		if len(states.got) != 1 || states.got[0] != wantRead {
			t.Errorf("result reads = %v, want [%s]", states.got, wantRead)
		}
	})

	t.Run("a version with no execution is not found", func(t *testing.T) {
		t.Parallel()

		states := &fakeUploadStateClient{descErr: &serviceerror.NotFound{Message: "no execution"}}
		_, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0")
		if !errors.Is(err, firmware.ErrNotFound) {
			t.Errorf("State() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a describe failure is reported as itself", func(t *testing.T) {
		t.Parallel()

		states := &fakeUploadStateClient{descErr: errors.New("frontend unavailable")}
		_, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0")
		if err == nil {
			t.Fatal("State() error = nil, want the describe failure")
		}
		if errors.Is(err, firmware.ErrNotFound) {
			t.Errorf("State() error = %v, want no not-found for a failing backend", err)
		}
	})

	t.Run("a query failure is reported", func(t *testing.T) {
		t.Parallel()

		states := &fakeUploadStateClient{
			desc:     status(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			queryErr: errors.New("query rejected"),
		}
		if _, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0"); err == nil {
			t.Fatal("State() error = nil, want the query failure")
		}
	})

	t.Run("a result that cannot be read is reported", func(t *testing.T) {
		t.Parallel()

		states := &fakeUploadStateClient{
			desc: status(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED),
			run:  fakeWorkflowRun{err: errors.New("workflow failed")},
		}
		if _, err := NewFirmwareUploadStates(states).State(context.Background(), "2.0.0"); err == nil {
			t.Fatal("State() error = nil, want the failed result reported")
		}
	})

	t.Run("a version is required", func(t *testing.T) {
		t.Parallel()

		states := &fakeUploadStateClient{}
		if _, err := NewFirmwareUploadStates(states).State(context.Background(), " "); err == nil {
			t.Fatal("State() error = nil, want the missing version refused")
		}
		if len(states.described) != 0 {
			t.Errorf("describes = %v, want none for a missing version", states.described)
		}
	})
}
