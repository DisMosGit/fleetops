package main

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// fakeEntityRegistry is a hand-written entityRegistry double recording what gets registered
// and keeping the registered functions so a test can call them.
type fakeEntityRegistry struct {
	workflows  map[string]any
	activities map[string]any
}

func newFakeEntityRegistry() *fakeEntityRegistry {
	return &fakeEntityRegistry{
		workflows:  map[string]any{},
		activities: map[string]any{},
	}
}

func (r *fakeEntityRegistry) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	r.workflows[options.Name] = w
}

func (r *fakeEntityRegistry) RegisterActivityWithOptions(a any, options activity.RegisterOptions) {
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

func TestRegisterEntity(t *testing.T) {
	t.Parallel()

	registry := newFakeEntityRegistry()
	store := &fakeSnapshotStore{}
	registerEntity(registry, store)

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
