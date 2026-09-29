package main

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/config"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
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

// fakeRolloutDeps is a hand-written double for every side effect the rollout activities own,
// recording the writes so a test can prove the registered activities are bound to it.
type fakeRolloutDeps struct {
	rollouts []rollout.RolloutRecord
	waves    []rollout.WaveStateUpdate
}

func (d *fakeRolloutDeps) Metadata(_ context.Context, id string) (firmware.Record, error) {
	return firmware.Record{ID: id, Version: "2.0.0", Models: []string{"oak-s3"}}, nil
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

func (d *fakeRolloutDeps) SignalCommandIssued(context.Context, temporal.CommandIssuedSignal) error {
	return nil
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
		firmware: deps, targets: deps, rollouts: deps,
		waves: deps, commands: deps, health: deps,
	})

	if _, ok := registry.workflows[temporal.RolloutWorkflowName]; !ok {
		t.Errorf("workflow %q is not registered", temporal.RolloutWorkflowName)
	}
	wantActivities := []string{
		temporal.LoadFirmwareActivityName,
		temporal.ResolveWaveTargetsActivityName,
		temporal.RecordRolloutStateActivityName,
		temporal.RecordWaveStateActivityName,
		temporal.DispatchWaveUpdateActivityName,
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
}

// fakeStartClient is a hand-written double for the Temporal client's start call, recording the
// workflow options and the start input a rollout is begun with.
type fakeStartClient struct {
	inputs []temporal.RolloutInput
}

func (c *fakeStartClient) ExecuteWorkflow(
	_ context.Context, _ client.StartWorkflowOptions, _ any, args ...any,
) (client.WorkflowRun, error) {
	var input temporal.RolloutInput
	if len(args) > 0 {
		input, _ = args[0].(temporal.RolloutInput)
	}
	c.inputs = append(c.inputs, input)
	return nil, nil
}

func TestRolloutSettingsReachTheStartPath(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Rollout.HealthWindow = config.Duration{Duration: time.Minute}
	cfg.Rollout.DecisionTimeout = config.Duration{Duration: 5 * time.Minute}
	cfg.Rollout.Waves = []config.Wave{
		{Percent: 10},
		{Percent: 100, RequireApproval: true},
	}

	settings := rolloutSettings(cfg.Rollout)
	// The configured sequence reaches the workflow's start path unchanged, approval flags and
	// all: this is the policy a rollout started through this worker drives under.
	fc := &fakeStartClient{}
	starter := temporal.NewRolloutStarter(fc, cfg.Temporal.TaskQueue, settings)
	req := temporal.RolloutRequest{
		RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
	}
	if err := starter.Start(context.Background(), req); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	if len(fc.inputs) != 1 {
		t.Fatalf("started rollouts = %d, want 1", len(fc.inputs))
	}
	got := fc.inputs[0]
	if got.RolloutRequest != req {
		t.Errorf("start input request = %+v, want %+v", got.RolloutRequest, req)
	}
	if got.Settings.HealthWindow != time.Minute || got.Settings.DecisionTimeout != 5*time.Minute {
		t.Errorf("start input windows = %v/%v, want the configured 1m/5m",
			got.Settings.HealthWindow, got.Settings.DecisionTimeout)
	}
	wantWaves := []temporal.RolloutWave{
		{Percent: 10},
		{Percent: 100, RequireApproval: true},
	}
	if len(got.Settings.Waves) != len(wantWaves) {
		t.Fatalf("start input waves = %+v, want %+v", got.Settings.Waves, wantWaves)
	}
	for i, wave := range wantWaves {
		if got.Settings.Waves[i] != wave {
			t.Errorf("start input wave %d = %+v, want %+v", i, got.Settings.Waves[i], wave)
		}
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
