package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// testUpload is the upload every workflow test drives: one version, one model, one spooled file.
func testUpload() firmware.UploadRequest {
	return firmware.UploadRequest{
		Version:   "2.0.0",
		Models:    []string{"oak-s3"},
		SpoolPath: "/spool/upload-1",
	}
}

// fakeModelCatalog is a hand-written FirmwareModelCatalog double reporting the device registry's
// models, or failing the lookup.
type fakeModelCatalog struct {
	models []string
	err    error
}

// DeviceModels returns the scripted models.
func (f fakeModelCatalog) DeviceModels(context.Context) ([]string, error) {
	return f.models, f.err
}

// binaryCall is one storage step's request as the store saw it.
type binaryCall struct {
	version string
	path    string
}

// fakeUploadStore is a hand-written FirmwareUploadStore double: it records every transition and
// removal the upload's steps asked for, replays their scripted outcomes, and lets a test observe
// the workflow from inside a step.
type fakeUploadStore struct {
	mu sync.Mutex
	// stored is what the storage step reports; storeErr fails it instead.
	stored   firmware.StoredBinary
	storeErr error
	// upsertErr fails the metadata step for the named state; record is what it reports otherwise.
	upsertErr map[string]error
	record    firmware.Record
	// discards and upserts record what the steps were asked to do.
	discards []DiscardFirmwareUploadRequest
	upserts  []firmware.UploadMetadata
	binaries []binaryCall
	// observe runs inside the metadata step before its outcome is reported, which is how a test
	// reads the state query while the upload is mid-flight.
	observe func(md firmware.UploadMetadata)
}

// StoreBinary records the storage request and returns the scripted object.
func (f *fakeUploadStore) StoreBinary(_ context.Context, version, path string) (firmware.StoredBinary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.binaries = append(f.binaries, binaryCall{version: version, path: path})
	if f.storeErr != nil {
		return firmware.StoredBinary{}, f.storeErr
	}
	return f.stored, nil
}

// UpsertMetadata records one transition and returns the scripted record or failure.
func (f *fakeUploadStore) UpsertMetadata(_ context.Context, md firmware.UploadMetadata) (firmware.Record, error) {
	f.mu.Lock()
	f.upserts = append(f.upserts, md)
	observe := f.observe
	f.mu.Unlock()
	if observe != nil {
		observe(md)
	}
	if err := f.upsertErr[md.State]; err != nil {
		return firmware.Record{}, err
	}
	return f.record, nil
}

// DiscardUpload records the compensating removal.
func (f *fakeUploadStore) DiscardUpload(_ context.Context, version, workflowID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discards = append(f.discards, DiscardFirmwareUploadRequest{
		Version: version, WorkflowID: workflowID,
	})
	return nil
}

// transitions returns the states the metadata step was asked to write, in order.
func (f *fakeUploadStore) transitions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	states := make([]string, 0, len(f.upserts))
	for _, md := range f.upserts {
		states = append(states, md.State)
	}
	return states
}

// binaryRequests returns the storage requests, in order.
func (f *fakeUploadStore) binaryRequests() []binaryCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]binaryCall(nil), f.binaries...)
}

// removed returns the compensating removals, in order.
func (f *fakeUploadStore) removed() []DiscardFirmwareUploadRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]DiscardFirmwareUploadRequest(nil), f.discards...)
}

// newFirmwareEnv returns a test environment hosting FirmwareWorkflow under its registered name
// with the upload's four steps implemented by the real activity constructors over fakes, so a
// test asserts what the steps are asked for rather than what a mock was told to answer.
func newFirmwareEnv(
	catalog FirmwareModelCatalog,
	store FirmwareUploadStore,
) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(discardLogger{})
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(time.Minute)
	env.RegisterWorkflowWithOptions(FirmwareWorkflow, workflow.RegisterOptions{
		Name: FirmwareWorkflowName,
	})
	env.RegisterActivityWithOptions(NewValidateFirmwareModelsActivity(catalog),
		activity.RegisterOptions{Name: ValidateFirmwareModelsActivityName})
	env.RegisterActivityWithOptions(NewStoreFirmwareBinaryActivity(store),
		activity.RegisterOptions{Name: StoreFirmwareBinaryActivityName})
	env.RegisterActivityWithOptions(NewUpsertFirmwareMetadataActivity(store),
		activity.RegisterOptions{Name: UpsertFirmwareMetadataActivityName})
	env.RegisterActivityWithOptions(NewDiscardFirmwareUploadActivity(store),
		activity.RegisterOptions{Name: DiscardFirmwareUploadActivityName})
	return env
}

// runUpload drives one upload and returns the state it concluded with.
func runUpload(t *testing.T, env *testsuite.TestWorkflowEnvironment, in firmware.UploadRequest) firmware.UploadState {
	t.Helper()
	env.ExecuteWorkflow(FirmwareWorkflowName, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("firmware workflow error: %v", err)
	}
	var state firmware.UploadState
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatalf("read firmware workflow result: %v", err)
	}
	return state
}

// TestFirmwareWorkflowUploadsOneFirmware pins the upload's happy path: the models are validated
// first, the version's record is claimed and advanced through the states the upload reaches, the
// spooled binary is stored by path, and the upload concludes available with the record it wrote.
func TestFirmwareWorkflowUploadsOneFirmware(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{
		stored: firmware.StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7},
		record: firmware.Record{ID: "fw-1", Version: "2.0.0", Models: []string{"oak-s3"},
			Checksum: "sum", Size: 7, GridFSID: "fwbin-1", State: firmware.UploadAvailable},
	}
	env := newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3", "birch-x1"}}, store)

	state := runUpload(t, env, testUpload())

	want := firmware.UploadState{
		Version: "2.0.0", State: firmware.UploadAvailable, Concluded: true,
		FirmwareID: "fw-1", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
	}
	if diff := cmp.Diff(want, state); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
	wantTransitions := []string{
		firmware.UploadAccepted, firmware.UploadStoring, firmware.UploadAvailable,
	}
	if diff := cmp.Diff(wantTransitions, store.transitions()); diff != "" {
		t.Errorf("recorded transitions mismatch (-want +got):\n%s", diff)
	}
	for _, md := range store.upserts {
		if md.WorkflowID == "" {
			t.Errorf("transition %s records no owning workflow execution", md.State)
		}
	}
	wantBinary := []binaryCall{{version: "2.0.0", path: "/spool/upload-1"}}
	if diff := cmp.Diff(wantBinary, store.binaryRequests(), cmp.AllowUnexported(binaryCall{})); diff != "" {
		t.Errorf("storage requests mismatch (-want +got):\n%s", diff)
	}
	if len(store.removed()) != 0 {
		t.Errorf("removals = %v, want none for an upload that became available", store.removed())
	}
}

// TestFirmwareWorkflowServesItsStateWhileStoring pins the state query's answer mid-flight: the
// upload reports the state it has reached and no firmware id or checksum yet, and answering it
// does not hold the upload.
func TestFirmwareWorkflowServesItsStateWhileStoring(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{
		stored: firmware.StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7},
		record: firmware.Record{ID: "fw-1", Version: "2.0.0", Checksum: "sum", Size: 7},
	}
	var (
		mu      sync.Mutex
		queried []firmware.UploadState
		env     *testsuite.TestWorkflowEnvironment
	)
	store.observe = func(md firmware.UploadMetadata) {
		if md.State != firmware.UploadStoring {
			return
		}
		value, err := env.QueryWorkflow(GetFirmwareUploadStateQueryType)
		if err != nil {
			t.Errorf("query the storing upload: %v", err)
			return
		}
		var state firmware.UploadState
		if err := value.Get(&state); err != nil {
			t.Errorf("decode the storing upload: %v", err)
			return
		}
		mu.Lock()
		queried = append(queried, state)
		mu.Unlock()
	}
	env = newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3"}}, store)
	runUpload(t, env, testUpload())

	mu.Lock()
	defer mu.Unlock()
	if len(queried) != 1 {
		t.Fatalf("queries = %d, want the upload queried once while storing", len(queried))
	}
	want := firmware.UploadState{
		Version: "2.0.0", State: firmware.UploadStoring, Models: []string{"oak-s3"},
	}
	if diff := cmp.Diff(want, queried[0]); diff != "" {
		t.Errorf("state query mismatch (-want +got):\n%s", diff)
	}
}

// TestFirmwareWorkflowRefusesUnknownModels pins what a refused upload leaves behind: the upload
// concludes failed naming the model, and neither a record nor a binary is stored.
func TestFirmwareWorkflowRefusesUnknownModels(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{}
	env := newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3"}}, store)
	in := testUpload()
	in.Models = []string{"oak-s3", "walnut-9"}

	state := runUpload(t, env, in)

	if state.State != firmware.UploadFailed || !state.Concluded {
		t.Fatalf("state = %+v, want a concluded failure", state)
	}
	if state.Failure != firmware.FailureRefused {
		t.Errorf("failure = %q, want %q", state.Failure, firmware.FailureRefused)
	}
	if !strings.Contains(state.Reason, "walnut-9") {
		t.Errorf("reason = %q, want it to name the refused model", state.Reason)
	}
	if len(store.upserts) != 0 || len(store.binaries) != 0 || len(store.discards) != 0 {
		t.Errorf("steps recorded = %d transitions, %d stores, %d removals, want none",
			len(store.upserts), len(store.binaries), len(store.discards))
	}
}

// TestFirmwareWorkflowFailsAVersionConflict pins the conflict path: an upload naming a version
// whose record another upload owns concludes failed with the conflict as its reason and stores
// nothing, and the metadata step is not retried because no retry can change the answer.
func TestFirmwareWorkflowFailsAVersionConflict(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{
		upsertErr: map[string]error{
			firmware.UploadAccepted: fmt.Errorf("upsert firmware 2.0.0: %w", firmware.ErrVersionConflict),
		},
	}
	env := newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3"}}, store)

	state := runUpload(t, env, testUpload())

	if state.State != firmware.UploadFailed || state.Failure != firmware.FailureConflict {
		t.Fatalf("state = %+v, want a concluded conflict", state)
	}
	if !strings.Contains(state.Reason, "already registered") {
		t.Errorf("reason = %q, want the conflict named", state.Reason)
	}
	if len(store.upserts) != 1 {
		t.Errorf("transitions = %d, want the refused claim attempted once", len(store.upserts))
	}
	if len(store.binaries) != 0 {
		t.Errorf("storage requests = %d, want none for a refused version", len(store.binaries))
	}
}

// TestFirmwareWorkflowRecordsAStorageFailure pins the failed upload that leaves a record: the
// binary could not be stored, so the upload's document carries the failed state and the reason,
// and nothing is removed because nothing was stored.
func TestFirmwareWorkflowRecordsAStorageFailure(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{storeErr: errors.New("gridfs unavailable")}
	env := newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3"}}, store)

	state := runUpload(t, env, testUpload())

	if state.State != firmware.UploadFailed || state.Failure != firmware.FailureInternal {
		t.Fatalf("state = %+v, want a concluded internal failure", state)
	}
	wantTransitions := []string{
		firmware.UploadAccepted, firmware.UploadStoring, firmware.UploadFailed,
	}
	if diff := cmp.Diff(wantTransitions, store.transitions()); diff != "" {
		t.Errorf("recorded transitions mismatch (-want +got):\n%s", diff)
	}
	if last := store.upserts[len(store.upserts)-1]; last.Reason == "" {
		t.Error("the failed transition carries no reason")
	}
	if len(store.discards) != 0 {
		t.Errorf("removals = %d, want none when no binary was stored", len(store.discards))
	}
}

// TestFirmwareWorkflowDiscardsAnUnrecordableUpload pins the compensation: when the binary was
// stored but the record that would reference it cannot be written, the upload removes both and
// concludes failed, so no orphaned binary and no record outlive it.
func TestFirmwareWorkflowDiscardsAnUnrecordableUpload(t *testing.T) {
	t.Parallel()

	store := &fakeUploadStore{
		stored: firmware.StoredBinary{Ref: "fwbin-1", Checksum: "sum", Size: 7},
		upsertErr: map[string]error{
			firmware.UploadAvailable: errors.New("record write refused"),
		},
	}
	env := newFirmwareEnv(fakeModelCatalog{models: []string{"oak-s3"}}, store)

	state := runUpload(t, env, testUpload())

	if state.State != firmware.UploadFailed || state.Failure != firmware.FailureInternal {
		t.Fatalf("state = %+v, want a concluded internal failure", state)
	}
	removed := store.removed()
	if len(removed) != 1 {
		t.Fatalf("removals = %d, want the incomplete upload discarded once", len(removed))
	}
	if removed[0].Version != "2.0.0" {
		t.Errorf("removed version = %q, want the upload's version", removed[0].Version)
	}
	if removed[0].WorkflowID == "" || removed[0].WorkflowID != store.upserts[0].WorkflowID {
		t.Errorf("removed workflow = %q, want the upload's own execution %q",
			removed[0].WorkflowID, store.upserts[0].WorkflowID)
	}
	for _, md := range store.upserts {
		if md.State == firmware.UploadFailed {
			t.Errorf("transitions include %s, want none: the upload leaves no record behind",
				firmware.UploadFailed)
		}
	}
}
