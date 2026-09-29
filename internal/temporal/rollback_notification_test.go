package temporal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/telemetry"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// fakeNotifier is a hand-written RollbackNotifier double recording the announcements it accepted,
// failing every one when err is set.
type fakeNotifier struct {
	mu     sync.Mutex
	events []telemetry.RollbackEvent
	err    error
}

// Announce records one announcement.
func (f *fakeNotifier) Announce(_ context.Context, event telemetry.RollbackEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, event)
	return nil
}

// recorded returns the announcements the notifier accepted, in order.
func (f *fakeNotifier) recorded() []telemetry.RollbackEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]telemetry.RollbackEvent(nil), f.events...)
}

// announcedAt is the moment the announcements in these tests were decided at.
func announcedAt() time.Time { return time.Unix(1700000000, 0).UTC() }

// rollbackAnnouncementRequest is the request the workflow builds for one phase: the rollout's
// identity, why it is rolling back, and — on the completed phase — what the compensations achieved.
func rollbackAnnouncementRequest(phase telemetry.RollbackPhase) AnnounceRollbackRequest {
	req := AnnounceRollbackRequest{
		Phase:           phase,
		RolloutID:       "ro-1",
		FirmwareID:      "fw-1",
		FirmwareVersion: "2.0.0",
		Region:          "eu-west",
		Model:           "oak-s3",
		WaveID:          "ro-1-w1-5",
		Outcome:         OutcomeUnhealthyWave,
		Decision: &WaveHealth{
			Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: 0.4, SampleSize: 120,
			WindowStart: announcedAt().Add(-5 * time.Minute), WindowEnd: announcedAt(),
		},
		PlanSteps:   4,
		PlanDevices: 10,
		OccurredAt:  announcedAt(),
	}
	if phase == telemetry.RollbackCompleted {
		req.Rollback = &RollbackView{
			Plan: []RollbackStepView{
				{Kind: "notify_started", Status: "completed"},
				{
					Kind: "downgrade", WaveID: "ro-1-w1-5", Status: "completed",
					Devices: 8, Restored: 7, Unreported: 1,
				},
				{
					Kind: "downgrade", WaveID: "ro-1-w0-1", Status: "completed",
					Devices: 2, Restored: 1, Skipped: 1,
				},
				{
					Kind: "reconcile_inventory", Status: "completed",
					Devices: 10, Agreed: 1, Corrected: 8, Unverified: 1,
				},
				{Kind: "notify_completed", Status: "completed"},
			},
			Inventory: []FirmwareCount{
				{Version: "1.0.0", Devices: 8},
				{Version: "2.0.0", Devices: 1},
			},
			UnrestoredDeviceIDs: []string{"dev-8"},
		}
	}
	return req
}

func TestAnnounceRollbackActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// req is the request the workflow builds; nil means the started phase's.
		req         *AnnounceRollbackRequest
		notifierErr error
		wantErr     string
		check       func(*testing.T, telemetry.RollbackEvent)
	}{
		{
			name: "the started phase announces the rollout and the plan it will run",
			check: func(t *testing.T, event telemetry.RollbackEvent) {
				t.Helper()
				if event.Phase != telemetry.RollbackStarted {
					t.Errorf("phase = %s, want started", event.Phase)
				}
				if event.EventID != telemetry.RollbackEventID("ro-1", telemetry.RollbackStarted) {
					t.Errorf("event id = %q, want the identity the rollout and phase derive", event.EventID)
				}
				if event.OccurredAt != announcedAt() {
					t.Errorf("occurred at = %v, want the workflow's %v", event.OccurredAt, announcedAt())
				}
				want := telemetry.RolloutAnnouncement{
					RolloutID: "ro-1", FirmwareID: "fw-1", FirmwareVersion: "2.0.0",
					Region: "eu-west", Model: "oak-s3", WaveID: "ro-1-w1-5",
					Outcome: string(OutcomeUnhealthyWave),
					Decision: &telemetry.RollbackDecision{
						Verdict: "unhealthy", SuccessRatio: 0.4, SampleSize: 120,
						WindowStart: announcedAt().Add(-5 * time.Minute), WindowEnd: announcedAt(),
					},
					PlanSteps: 4, PlanDevices: 10,
				}
				if diff := cmp.Diff(want, event.Rollout); diff != "" {
					t.Errorf("announcement mismatch (-want +got):\n%s", diff)
				}
				if event.Progress != nil {
					t.Errorf("started announcement carries progress %+v, want none", event.Progress)
				}
			},
		},
		{
			name: "the completed phase reports what the compensations achieved",
			req: func() *AnnounceRollbackRequest {
				req := rollbackAnnouncementRequest(telemetry.RollbackCompleted)
				return &req
			}(),
			check: func(t *testing.T, event telemetry.RollbackEvent) {
				t.Helper()
				if event.Phase != telemetry.RollbackCompleted {
					t.Fatalf("phase = %s, want completed", event.Phase)
				}
				if event.Progress == nil {
					t.Fatal("the completed announcement carries no progress")
				}
				// The totals are summed over the plan's steps, so a consumer sees what the
				// rollback achieved without walking the plan.
				if event.Progress.Restored != 8 || event.Progress.Skipped != 1 ||
					event.Progress.Unreported != 1 {
					t.Errorf("device totals = %+v, want 8 restored, 1 skipped, 1 unreported", event.Progress)
				}
				if event.Progress.Agreed != 1 || event.Progress.Corrected != 8 || event.Progress.Unverified != 1 {
					t.Errorf("record totals = %+v, want 1 agreed, 8 corrected, 1 unverified", event.Progress)
				}
				wantInventory := []telemetry.FirmwareInventoryEntry{
					{Version: "1.0.0", Devices: 8},
					{Version: "2.0.0", Devices: 1},
				}
				if diff := cmp.Diff(wantInventory, event.Progress.Inventory); diff != "" {
					t.Errorf("inventory mismatch (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff([]string{"dev-8"}, event.Progress.UnrestoredDeviceIDs); diff != "" {
					t.Errorf("unrestored devices mismatch (-want +got):\n%s", diff)
				}
				if len(event.Progress.Steps) != 5 {
					t.Fatalf("step outcomes = %d, want the plan's 5", len(event.Progress.Steps))
				}
				if event.Progress.Steps[1].WaveID != "ro-1-w1-5" || event.Progress.Steps[1].Restored != 7 {
					t.Errorf("first downgrade outcome = %+v, want the second wave's seven restores",
						event.Progress.Steps[1])
				}
			},
		},
		{
			name: "a wave that ended the rollout unmeasured carries no decision",
			req: func() *AnnounceRollbackRequest {
				req := rollbackAnnouncementRequest(telemetry.RollbackStarted)
				req.Decision = nil
				req.Outcome = OutcomeDispatchFailed
				return &req
			}(),
			check: func(t *testing.T, event telemetry.RollbackEvent) {
				t.Helper()
				if event.Rollout.Decision != nil {
					t.Errorf("decision = %+v, want none for an unmeasured wave", event.Rollout.Decision)
				}
				if event.Rollout.Outcome != string(OutcomeDispatchFailed) {
					t.Errorf("outcome = %q, want %q", event.Rollout.Outcome, OutcomeDispatchFailed)
				}
			},
		},
		{
			name:        "a publication failure is reported",
			notifierErr: errors.New("broker is unreachable"),
			wantErr:     "broker is unreachable",
		},
		{
			name:    "a request without a rollout is refused",
			req:     &AnnounceRollbackRequest{Phase: telemetry.RollbackStarted},
			wantErr: "rollout id and phase required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := rollbackAnnouncementRequest(telemetry.RollbackStarted)
			if tc.req != nil {
				req = *tc.req
			}
			notifier := &fakeNotifier{err: tc.notifierErr}
			activity := NewAnnounceRollbackActivity(notifier)

			err := activity(context.Background(), req)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("announce error = %v, want it to name %q", err, tc.wantErr)
				}
				if len(notifier.recorded()) != 0 {
					t.Error("an announcement was published although the request was refused")
				}
				return
			}
			if err != nil {
				t.Fatalf("announce error = %v", err)
			}
			events := notifier.recorded()
			if len(events) != 1 {
				t.Fatalf("announcements = %d, want 1", len(events))
			}
			tc.check(t, events[0])
		})
	}
}

// TestAnnounceRollbackEventIdentityIsStable pins that a retried publication repeats the event's
// identity: the same request published twice is one event, which is what lets a consumer
// deduplicate it.
func TestAnnounceRollbackEventIdentityIsStable(t *testing.T) {
	t.Parallel()

	notifier := &fakeNotifier{}
	activity := NewAnnounceRollbackActivity(notifier)
	req := rollbackAnnouncementRequest(telemetry.RollbackStarted)
	for attempt := 1; attempt <= 2; attempt++ {
		if err := activity(context.Background(), req); err != nil {
			t.Fatalf("attempt %d error = %v", attempt, err)
		}
	}
	events := notifier.recorded()
	if len(events) != 2 {
		t.Fatalf("announcements = %d, want the retry published too", len(events))
	}
	if events[0].EventID != events[1].EventID {
		t.Errorf("event ids = %q and %q, want the retry to repeat the identity",
			events[0].EventID, events[1].EventID)
	}
}
