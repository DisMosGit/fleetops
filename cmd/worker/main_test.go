package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// fakeRegistry is a hand-written registry double recording what gets registered and keeping the
// registered functions so a test can call them.
type fakeRegistry struct {
	workflows  map[string]any
	activities map[string]any
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		workflows:  map[string]any{},
		activities: map[string]any{},
	}
}

func (r *fakeRegistry) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	r.workflows[options.Name] = w
}

func (r *fakeRegistry) RegisterActivityWithOptions(a any, options activity.RegisterOptions) {
	r.activities[options.Name] = a
}

// fakeSnapshotStore is a hand-written temporal.StateSnapshotter double recording what the
// registered activity saves.
type fakeSnapshotStore struct {
	saved []devices.Snapshot
}

func (s *fakeSnapshotStore) SaveSnapshot(_ context.Context, snap devices.Snapshot) error {
	s.saved = append(s.saved, snap)
	return nil
}

// fakeSearchAttributes is a hand-written searchAttributeRegistry double recording what the
// startup bootstrap registers.
type fakeSearchAttributes struct {
	registered map[string]enumspb.IndexedValueType
	namespaces []string
}

func newFakeSearchAttributes() *fakeSearchAttributes {
	return &fakeSearchAttributes{registered: map[string]enumspb.IndexedValueType{}}
}

func (f *fakeSearchAttributes) ListSearchAttributes(
	_ context.Context, in *operatorservice.ListSearchAttributesRequest, _ ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
	f.namespaces = append(f.namespaces, in.GetNamespace())
	attrs := make(map[string]enumspb.IndexedValueType, len(f.registered))
	for name, typ := range f.registered {
		attrs[name] = typ
	}
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: attrs}, nil
}

func (f *fakeSearchAttributes) AddSearchAttributes(
	_ context.Context, in *operatorservice.AddSearchAttributesRequest, _ ...grpc.CallOption,
) (*operatorservice.AddSearchAttributesResponse, error) {
	for name, typ := range in.GetSearchAttributes() {
		f.registered[name] = typ
	}
	return &operatorservice.AddSearchAttributesResponse{}, nil
}

// TestBootstrapNamespace pins what the worker registers before it polls: every custom search
// attribute its workflows upsert — the device family's and the rollout family's — so no run can
// mirror a value the Temporal UI cannot filter on. An attribute a namespace already carries is
// left alone, which is what makes the bootstrap safe to run on every replica start.
func TestBootstrapNamespace(t *testing.T) {
	t.Parallel()

	t.Run("a fresh namespace gets all seven attributes", func(t *testing.T) {
		t.Parallel()

		reg := newFakeSearchAttributes()
		if err := bootstrapNamespace(context.Background(), reg, "fleetops-dev"); err != nil {
			t.Fatalf("bootstrapNamespace: %v", err)
		}

		want := map[string]enumspb.IndexedValueType{
			"DeviceRegion":    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceModel":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceFirmware":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceOnline":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"RolloutFirmware": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutRegion":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutStatus":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}
		if diff := cmp.Diff(want, reg.registered); diff != "" {
			t.Errorf("registered attributes mismatch (-want +got):\n%s", diff)
		}
		if len(reg.namespaces) == 0 || reg.namespaces[0] != "fleetops-dev" {
			t.Errorf("namespaces inspected = %v, want the configured one", reg.namespaces)
		}
	})

	t.Run("a namespace with the device attributes gets the rollout ones", func(t *testing.T) {
		t.Parallel()

		// The upgrade path: a namespace seeded by the previous build already carries the four
		// device attributes.
		reg := newFakeSearchAttributes()
		reg.registered = map[string]enumspb.IndexedValueType{
			"DeviceRegion":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceModel":    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceFirmware": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceOnline":   enumspb.INDEXED_VALUE_TYPE_BOOL,
		}
		if err := bootstrapNamespace(context.Background(), reg, "default"); err != nil {
			t.Fatalf("bootstrapNamespace: %v", err)
		}

		want := map[string]enumspb.IndexedValueType{
			"DeviceRegion":    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceModel":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceFirmware":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceOnline":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"RolloutFirmware": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutRegion":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutStatus":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}
		if diff := cmp.Diff(want, reg.registered); diff != "" {
			t.Errorf("registered attributes mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a second bootstrap changes nothing", func(t *testing.T) {
		t.Parallel()

		reg := newFakeSearchAttributes()
		if err := bootstrapNamespace(context.Background(), reg, "default"); err != nil {
			t.Fatalf("bootstrapNamespace: %v", err)
		}
		first := len(reg.registered)
		if err := bootstrapNamespace(context.Background(), reg, "default"); err != nil {
			t.Fatalf("bootstrapNamespace rerun: %v", err)
		}
		if len(reg.registered) != first {
			t.Errorf("registered attributes = %d after a rerun, want %d", len(reg.registered), first)
		}
	})

	t.Run("a failure is reported with its cause", func(t *testing.T) {
		t.Parallel()

		err := bootstrapNamespace(context.Background(), &failingSearchAttributes{}, "default")
		if err == nil || !strings.Contains(err.Error(), "search attributes") {
			t.Errorf("bootstrapNamespace = %v, want a search-attribute error", err)
		}
	})
}

// failingSearchAttributes reports every registry call as a failure.
type failingSearchAttributes struct{}

func (f *failingSearchAttributes) ListSearchAttributes(
	context.Context, *operatorservice.ListSearchAttributesRequest, ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
	return nil, errors.New("temporal is unavailable")
}

func (f *failingSearchAttributes) AddSearchAttributes(
	context.Context, *operatorservice.AddSearchAttributesRequest, ...grpc.CallOption,
) (*operatorservice.AddSearchAttributesResponse, error) {
	return nil, errors.New("temporal is unavailable")
}

// fakeRolloutDeps is a hand-written double for every side effect the rollout activities own,
// recording the writes so a test can prove the registered activities are bound to it.
type fakeRolloutDeps struct {
	rollouts []rollout.RolloutRecord
	waves    []rollout.WaveStateUpdate
	commands []temporal.CommandIssuedSignal
	// deviceStates answers the device-state reads the update and downgrade activities perform.
	deviceStates map[string]temporal.State
	// reconciliations are the devices the inventory activity was asked to reconcile.
	reconciliations []string
	// announcements are the rollback events the notifier was asked to publish.
	announcements []telemetry.RollbackEvent
}

func (d *fakeRolloutDeps) Metadata(_ context.Context, id string) (firmware.Record, error) {
	return firmware.Record{ID: id, Version: "2.0.0", Models: []string{"oak-s3"}}, nil
}

// MetadataByVersion answers the registry lookup a downgrade resolves a previous version through.
func (d *fakeRolloutDeps) MetadataByVersion(_ context.Context, version string) (firmware.Record, error) {
	return firmware.Record{ID: "fw-0", Version: version, Models: []string{"oak-s3"}}, nil
}

// ReconcileFirmware answers the fleet's record of one device's firmware version.
func (d *fakeRolloutDeps) ReconcileFirmware(
	_ context.Context, deviceID, _ string,
) (devices.FirmwareState, error) {
	d.reconciliations = append(d.reconciliations, deviceID)
	return devices.FirmwareAgreed, nil
}

// Announce records one rollback announcement the notifier was asked to publish.
func (d *fakeRolloutDeps) Announce(_ context.Context, event telemetry.RollbackEvent) error {
	d.announcements = append(d.announcements, event)
	return nil
}

func (d *fakeRolloutDeps) ResolveWave(_ context.Context, req rollout.ResolveRequest) (rollout.WaveRecord, error) {
	return rollout.WaveRecord{
		ID: req.WaveID, RolloutID: req.RolloutID, Percent: req.Percent,
		Status: rollout.WaveDispatching, DeviceIDs: []string{"dev-1"}, StartedAt: req.StartedAt,
	}, nil
}

func (d *fakeRolloutDeps) RecordRollout(_ context.Context, rec rollout.RolloutRecord) error {
	d.rollouts = append(d.rollouts, rec)
	return nil
}

func (d *fakeRolloutDeps) RecordWaveState(_ context.Context, update rollout.WaveStateUpdate) error {
	d.waves = append(d.waves, update)
	return nil
}

func (d *fakeRolloutDeps) SignalCommandIssued(_ context.Context, cmd temporal.CommandIssuedSignal) error {
	d.commands = append(d.commands, cmd)
	return nil
}

// State answers one device's authoritative state: the state the test scripted, moved on by the
// commands this process delivered to that device — which is what makes an update's and a restore's
// wait end the way a real device's would.
func (d *fakeRolloutDeps) State(_ context.Context, deviceID string) (temporal.State, error) {
	state, ok := d.deviceStates[deviceID]
	if !ok {
		return temporal.State{}, fmt.Errorf("device %s: %w", deviceID, temporal.ErrDeviceNotFound)
	}
	for _, cmd := range d.commands {
		if cmd.DeviceID != deviceID || cmd.Version == "" {
			continue
		}
		if cmd.Version != state.CurrentFw {
			state.PreviousFw, state.CurrentFw = state.CurrentFw, cmd.Version
		}
		concluded := temporal.ConcludedCommand{Command: cmd, Outcome: temporal.OutcomeSucceeded}
		state.LastCommand, state.Pending = &concluded, nil
	}
	return state, nil
}

func (d *fakeRolloutDeps) Evaluate(
	_ context.Context, query wavehealth.Query, at time.Time,
) (wavehealth.Result, error) {
	return wavehealth.Result{
		RolloutID: query.RolloutID, WaveID: query.WaveID,
		Verdict: wavehealth.VerdictHealthy, SuccessRatio: 0.99, WindowEnd: at,
	}, nil
}

func TestRegisterDevice(t *testing.T) {
	t.Parallel()

	registry := newFakeRegistry()
	store := &fakeSnapshotStore{}
	registerDevice(registry, store)

	// Registration names are the operators' contract: the workflow and its activity appear
	// in the Temporal UI under these names, never under function-reflection names.
	if len(registry.workflows) != 1 {
		t.Errorf("registered workflows = %d, want 1", len(registry.workflows))
	}
	if _, ok := registry.workflows[temporal.DeviceWorkflowName]; !ok {
		t.Errorf("workflow %q is not registered", temporal.DeviceWorkflowName)
	}
	if len(registry.activities) != 1 {
		t.Errorf("registered activities = %d, want 1", len(registry.activities))
	}
	if _, ok := registry.activities[temporal.SnapshotActivityName]; !ok {
		t.Errorf("activity %q is not registered", temporal.SnapshotActivityName)
	}

	// The registered activity is bound to the store it was registered with: whatever the
	// workflow snapshots lands there.
	snapshot, ok := registry.activities[temporal.SnapshotActivityName].(func(context.Context, temporal.Snapshot) error)
	if !ok {
		t.Fatalf("registered activity has type %T, want the snapshot activity",
			registry.activities[temporal.SnapshotActivityName])
	}
	if err := snapshot(context.Background(), temporal.Snapshot{
		State:      temporal.State{DeviceID: "dev-1", Region: "eu-west"},
		SnapshotAt: time.Now(),
	}); err != nil {
		t.Fatalf("run registered activity: %v", err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("store received %d snapshots, want 1", len(store.saved))
	}
	if store.saved[0].DeviceID != "dev-1" || store.saved[0].Region != "eu-west" {
		t.Errorf("stored snapshot = %+v, want device dev-1 in eu-west", store.saved[0])
	}
}

func TestRegisterRollout(t *testing.T) {
	t.Parallel()

	registry := newFakeRegistry()
	deps := &fakeRolloutDeps{}
	registerRollout(registry, rolloutDeps{
		firmware: deps, versions: deps, targets: deps, rollouts: deps, waves: deps,
		inventory: deps, commands: deps, devices: deps, health: deps, notifier: deps,
	})

	if _, ok := registry.workflows[temporal.RolloutWorkflowName]; !ok {
		t.Errorf("workflow %q is not registered", temporal.RolloutWorkflowName)
	}
	wantActivities := []string{
		temporal.LoadFirmwareActivityName,
		temporal.ResolveWaveTargetsActivityName,
		temporal.RecordRolloutStateActivityName,
		temporal.RecordWaveStateActivityName,
		temporal.UpdateDeviceActivityName,
		temporal.DowngradeDeviceActivityName,
		temporal.ReconcileInventoryActivityName,
		temporal.AnnounceRollbackActivityName,
		temporal.EvaluateWaveHealthActivityName,
	}
	for _, name := range wantActivities {
		if _, ok := registry.activities[name]; !ok {
			t.Errorf("activity %q is not registered", name)
		}
	}
	if len(registry.activities) != len(wantActivities) {
		t.Errorf("registered activities = %d, want %d",
			len(registry.activities), len(wantActivities))
	}
	// Activities run beside the side effect they own: the one that dispatches to a live agent
	// stream stays in the control plane, beside the connections it writes to.
	if _, ok := registry.activities[temporal.DispatchActivityName]; ok {
		t.Errorf("activity %q is registered here, want it to stay in the control plane",
			temporal.DispatchActivityName)
	}

	// The registered activities are bound to the deps they were registered with: a rollout's
	// state write lands in the store this process wired.
	record, ok := registry.activities[temporal.RecordRolloutStateActivityName].(func(context.Context, temporal.RecordRolloutRequest) error)
	if !ok {
		t.Fatalf("registered activity has type %T, want the record-rollout-state activity",
			registry.activities[temporal.RecordRolloutStateActivityName])
	}
	if err := record(context.Background(), temporal.RecordRolloutRequest{
		RolloutID: "ro-1", FirmwareID: "fw-1", WorkflowID: "rollout-ro-1",
		Region: "eu-west", Model: "oak-s3", Status: rollout.RolloutRunning,
	}); err != nil {
		t.Fatalf("run registered activity: %v", err)
	}
	if len(deps.rollouts) != 1 || deps.rollouts[0].ID != "ro-1" {
		t.Errorf("store received %+v, want the recorded rollout", deps.rollouts)
	}

	// The per-device update activity is bound to the device world this process wired: its
	// command reaches the command seam, and its wait ends on the state that seam's device
	// reports.
	update, ok := registry.activities[temporal.UpdateDeviceActivityName].(func(context.Context, temporal.UpdateDeviceRequest) (temporal.DeviceUpdate, error))
	if !ok {
		t.Fatalf("registered activity has type %T, want the update-device activity",
			registry.activities[temporal.UpdateDeviceActivityName])
	}
	waveID := rollout.WaveID("ro-1", 0, 100)
	firmwareRec := temporal.Firmware{ID: "fw-1", Version: "2.0.0", Checksum: "sha256:0f1e2d"}
	deps.deviceStates = map[string]temporal.State{"dev-1": {
		DeviceID: "dev-1",
		LastCommand: &temporal.ConcludedCommand{
			Command: temporal.CommandIssuedSignal{
				CommandID: temporal.CommandID(waveID, "dev-1"), DeviceID: "dev-1",
				Kind: temporal.CommandKindUpdate, FirmwareID: "fw-1", Version: "2.0.0",
			},
			Outcome: temporal.OutcomeSucceeded,
		},
	}}
	got, err := update(context.Background(), temporal.UpdateDeviceRequest{
		RolloutID: "ro-1", WaveID: waveID, DeviceID: "dev-1", Firmware: firmwareRec,
		Deadline: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("run registered update activity: %v", err)
	}
	if got.Outcome != temporal.UpdateSucceeded {
		t.Errorf("update outcome = %q, want %q", got.Outcome, temporal.UpdateSucceeded)
	}
	wantCommands := []temporal.CommandIssuedSignal{{
		CommandID: temporal.CommandID(waveID, "dev-1"), DeviceID: "dev-1",
		Kind: temporal.CommandKindUpdate, FirmwareID: "fw-1", Version: "2.0.0",
		Checksum: "sha256:0f1e2d",
	}}
	if diff := cmp.Diff(wantCommands, deps.commands); diff != "" {
		t.Errorf("delivered commands mismatch (-want +got):\n%s", diff)
	}

	// The rollback's per-device downgrade is bound to the same device world and registry: it
	// restores the version the device records as the one it ran before.
	restore, ok := registry.activities[temporal.DowngradeDeviceActivityName].(func(context.Context, temporal.DowngradeDeviceRequest) (temporal.DeviceRestore, error))
	if !ok {
		t.Fatalf("registered activity has type %T, want the downgrade-device activity",
			registry.activities[temporal.DowngradeDeviceActivityName])
	}
	deps.deviceStates["dev-2"] = temporal.State{
		DeviceID: "dev-2", Model: "oak-s3", CurrentFw: "2.0.0", PreviousFw: "1.0.0",
		LastCommand: &temporal.ConcludedCommand{
			Command: temporal.CommandIssuedSignal{
				CommandID: temporal.CommandID(waveID, "dev-2"), DeviceID: "dev-2",
				Kind: temporal.CommandKindUpdate, FirmwareID: "fw-1", Version: "2.0.0",
			},
			Outcome: temporal.OutcomeSucceeded,
		},
	}
	restored, err := restore(context.Background(), temporal.DowngradeDeviceRequest{
		RolloutID: "ro-1", WaveID: waveID, DeviceID: "dev-2",
		DeployedFw: "2.0.0", Deadline: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("run registered downgrade activity: %v", err)
	}
	if restored.Outcome != temporal.RestoreRestored {
		t.Errorf("restore outcome = %q, want %q", restored.Outcome, temporal.RestoreRestored)
	}
	wantRestore := []temporal.CommandIssuedSignal{{
		CommandID: rollbackCommandIDForTest(),
		DeviceID:  "dev-2", Kind: temporal.CommandKindUpdate,
		FirmwareID: "fw-0", Version: "1.0.0", Checksum: "",
	}}
	if diff := cmp.Diff(wantRestore, deps.commands[len(deps.commands)-1:]); diff != "" {
		t.Errorf("delivered restore mismatch (-want +got):\n%s", diff)
	}

	// The reconciliation and the announcement are bound to the fleet inventory and the broker
	// publisher this process wired.
	reconcile, ok := registry.activities[temporal.ReconcileInventoryActivityName].(func(context.Context, temporal.ReconcileInventoryRequest) (temporal.DeviceInventory, error))
	if !ok {
		t.Fatalf("registered activity has type %T, want the reconcile-device-inventory activity",
			registry.activities[temporal.ReconcileInventoryActivityName])
	}
	reconciled, err := reconcile(context.Background(), temporal.ReconcileInventoryRequest{
		RolloutID: "ro-1", WaveID: waveID, DeviceID: "dev-2",
	})
	if err != nil {
		t.Fatalf("run registered reconciliation activity: %v", err)
	}
	if reconciled.Outcome != temporal.InventoryAgreed || len(deps.reconciliations) != 1 {
		t.Errorf("reconciliation = %+v against %v, want it agreed over dev-2",
			reconciled, deps.reconciliations)
	}

	announce, ok := registry.activities[temporal.AnnounceRollbackActivityName].(func(context.Context, temporal.AnnounceRollbackRequest) error)
	if !ok {
		t.Fatalf("registered activity has type %T, want the announce-rollback activity",
			registry.activities[temporal.AnnounceRollbackActivityName])
	}
	if err := announce(context.Background(), temporal.AnnounceRollbackRequest{
		Phase: telemetry.RollbackStarted, RolloutID: "ro-1", FirmwareID: "fw-1",
		FirmwareVersion: "2.0.0", Region: "eu-west", Model: "oak-s3", Outcome: temporal.OutcomeUnhealthyWave,
		PlanSteps: 4, PlanDevices: 1, OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("run registered announcement activity: %v", err)
	}
	if len(deps.announcements) != 1 ||
		deps.announcements[0].EventID != telemetry.RollbackEventID("ro-1", telemetry.RollbackStarted) {
		t.Errorf("announcements = %+v, want the started phase published", deps.announcements)
	}
}

// rollbackCommandIDForTest is the restore command id one rollback derives for dev-2: the workflow
// derives it the same way, from the rollout and the device.
func rollbackCommandIDForTest() string {
	return "rollback-ro-1-dev-2"
}

// TestStartNotifier pins how the announcement publisher is hosted: its loop is running before the
// worker begins polling, and a broker that is unreachable at startup is a reconnect loop inside the
// notifier — the announcements fail, the worker runs.
func TestStartNotifier(t *testing.T) {
	t.Parallel()

	// A port nothing listens on: every dial fails, which is the broker that is down when the
	// worker starts.
	notifier := telemetry.NewNotifier(
		"amqp://guest:guest@127.0.0.1:9/",
		telemetry.NewTopology(3, time.Second, time.Minute),
		slog.New(slog.DiscardHandler),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run, ready := startNotifier(notifier, ctx)
	done := make(chan error, 1)
	go func() { done <- run() }()

	select {
	case <-ready:
		// The publisher's loop is running: the worker may begin polling.
	case <-time.After(5 * time.Second):
		t.Fatal("the notifier never reported that its loop was running")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("the notifier run = %v, want nil so an unreachable broker never stops the worker", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the notifier did not stop with the worker's context")
	}
}

func TestRolloutHealthSettings(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Rollout.MinSuccessRatio = 0.9
	cfg.Rollout.MinSamples = 25
	cfg.Rollout.SampleHealthThreshold = 0.5
	cfg.Rollout.HealthWindow = config.Duration{Duration: 2 * time.Minute}

	want := wavehealth.Settings{
		HealthWindow:          2 * time.Minute,
		SampleHealthThreshold: 0.5,
		MinSuccessRatio:       0.9,
		MinSamples:            25,
	}
	got := rolloutHealthSettings(cfg.Rollout)
	if got != want {
		t.Errorf("health settings = %+v, want %+v", got, want)
	}
}
