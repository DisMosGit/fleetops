package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

func TestApplyUpdateStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		phase        UpdatePhase
		wantSnapshot bool
	}{
		{name: "downloading rides the periodic snapshot", phase: PhaseDownloading},
		{name: "applying rides the periodic snapshot", phase: PhaseApplying},
		{name: "completed persists immediately", phase: PhaseCompleted, wantSnapshot: true},
		{name: "failed persists immediately", phase: PhaseFailed, wantSnapshot: true},
		{name: "rolled back persists immediately", phase: PhaseRolledBack, wantSnapshot: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newDeviceState("dev-1", testSettings())
			tr := s.applyUpdateStatus(UpdateStatusSignal{
				DeviceID: "dev-1", FirmwareID: "fw-2",
				Phase: tc.phase, ProgressPercent: 55, Detail: "why",
			})
			if tr.Update != tc.wantSnapshot {
				t.Errorf("needsSnapshot = %v, want %v", tr.Update, tc.wantSnapshot)
			}
			want := &UpdateStatus{
				FirmwareID: "fw-2", Phase: tc.phase, ProgressPercent: 55, Detail: "why",
			}
			if diff := cmp.Diff(want, s.Update); diff != "" {
				t.Errorf("recorded update status mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("latest report wins over earlier ones", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
		s.applyUpdateStatus(UpdateStatusSignal{
			DeviceID: "dev-1", FirmwareID: "fw-2", Phase: PhaseDownloading, ProgressPercent: 40,
		})
		s.applyUpdateStatus(UpdateStatusSignal{
			DeviceID: "dev-1", FirmwareID: "fw-2", Phase: PhaseFailed, Detail: "checksum mismatch",
		})
		want := &UpdateStatus{FirmwareID: "fw-2", Phase: PhaseFailed, Detail: "checksum mismatch"}
		if diff := cmp.Diff(want, s.Update); diff != "" {
			t.Errorf("recorded update status mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDeviceWorkflowUpdateStatus(t *testing.T) {
	t.Parallel()

	issued := CommandIssuedSignal{
		CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
		FirmwareID: "fw-2", Version: "fw-2",
	}
	signals := []testSignal{
		{CommandIssuedSignalName, issued},
		{UpdateStatusSignalName, UpdateStatusSignal{
			DeviceID: "dev-1", FirmwareID: "fw-2", Phase: PhaseDownloading, ProgressPercent: 40,
		}},
		{UpdateStatusSignalName, UpdateStatusSignal{
			DeviceID: "dev-1", FirmwareID: "fw-2", Phase: PhaseApplying, ProgressPercent: 80,
		}},
		{UpdateStatusSignalName, UpdateStatusSignal{
			DeviceID: "dev-1", FirmwareID: "fw-2", Phase: PhaseFailed, Detail: "checksum mismatch",
		}},
	}

	t.Run("latest reported status lands in state and snapshots", func(t *testing.T) {
		t.Parallel()
		snaps := &snapshotRecorder{}
		state := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, snaps, signals...)

		want := &UpdateStatus{FirmwareID: "fw-2", Phase: PhaseFailed, Detail: "checksum mismatch"}
		if diff := cmp.Diff(want, state.Update); diff != "" {
			t.Errorf("update status mismatch (-want +got):\n%s", diff)
		}
		// An update status concludes nothing: the command stays pending until its result.
		if state.Pending == nil || state.Pending.Command.CommandID != "cmd-1" {
			t.Errorf("pending command = %+v, want it still pending", state.Pending)
		}
		saved := snaps.recorded()
		if len(saved) == 0 {
			t.Fatal("no snapshot was persisted")
		}
		// The recorder stands in front of the snapshot activity, so what it sees is the
		// workflow's own snapshot payload.
		wantStored := &UpdateStatus{FirmwareID: "fw-2", Phase: PhaseFailed, Detail: "checksum mismatch"}
		if diff := cmp.Diff(wantStored, saved[len(saved)-1].Update); diff != "" {
			t.Errorf("snapshot update status mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("same signals replay to the same state and snapshots", func(t *testing.T) {
		t.Parallel()
		// The decision timestamp reads the workflow clock, which the test environment seeds
		// from wall-clock time per run: what must replay identically is the projected state,
		// not the instant the run happened to start.
		normalize := func(snaps []Snapshot) []Snapshot {
			out := append([]Snapshot(nil), snaps...)
			for i := range out {
				out[i].SnapshotAt = time.Time{}
			}
			return out
		}
		firstSnaps, secondSnaps := &snapshotRecorder{}, &snapshotRecorder{}
		first := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, firstSnaps, signals...)
		second := runDevice(t, newDeviceState("dev-1", testSettings()), &dispatchRecorder{}, secondSnaps, signals...)
		if diff := cmp.Diff(first.view(), second.view()); diff != "" {
			t.Errorf("replayed state differs (-first +second):\n%s", diff)
		}
		if diff := cmp.Diff(normalize(firstSnaps.recorded()), normalize(secondSnaps.recorded())); diff != "" {
			t.Errorf("replayed snapshots differ (-first +second):\n%s", diff)
		}
	})
}

func TestSignalerSignalUpdateStatus(t *testing.T) {
	t.Parallel()

	phases := []struct {
		wire agentv1.UpdatePhase
		want UpdatePhase
	}{
		{agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING, PhaseDownloading},
		{agentv1.UpdatePhase_UPDATE_PHASE_APPLYING, PhaseApplying},
		{agentv1.UpdatePhase_UPDATE_PHASE_REBOOTING, PhaseRebooting},
		{agentv1.UpdatePhase_UPDATE_PHASE_COMPLETED, PhaseCompleted},
		{agentv1.UpdatePhase_UPDATE_PHASE_FAILED, PhaseFailed},
		{agentv1.UpdatePhase_UPDATE_PHASE_ROLLED_BACK, PhaseRolledBack},
	}
	for _, tc := range phases {
		t.Run(string(tc.want), func(t *testing.T) {
			t.Parallel()
			fc := &fakeSignalClient{}
			signaler := NewSignaler(fc, "fleetops", testSettings())
			if err := signaler.SignalUpdateStatus(context.Background(), &agentv1.UpdateStatusRequest{
				DeviceId: "dev-1", FirmwareId: "fw-2",
				Phase: tc.wire, ProgressPercent: 70, Detail: "why",
			}); err != nil {
				t.Fatalf("SignalUpdateStatus: %v", err)
			}
			calls := fc.recorded()
			if len(calls) != 1 {
				t.Fatalf("signal-with-start calls = %d, want 1", len(calls))
			}
			if calls[0].signalName != UpdateStatusSignalName {
				t.Errorf("signal name = %q, want %q", calls[0].signalName, UpdateStatusSignalName)
			}
			if calls[0].workflowID != "device-dev-1" {
				t.Errorf("workflow id = %q, want device-dev-1", calls[0].workflowID)
			}
			want := UpdateStatusSignal{
				DeviceID: "dev-1", FirmwareID: "fw-2",
				Phase: tc.want, ProgressPercent: 70, Detail: "why",
			}
			if diff := cmp.Diff(want, calls[0].signalArg); diff != "" {
				t.Errorf("signal payload mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("unsupported phase signals nothing", func(t *testing.T) {
		t.Parallel()
		fc := &fakeSignalClient{}
		signaler := NewSignaler(fc, "fleetops", testSettings())
		err := signaler.SignalUpdateStatus(context.Background(), &agentv1.UpdateStatusRequest{
			DeviceId: "dev-1", FirmwareId: "fw-2",
		})
		if err == nil {
			t.Fatal("SignalUpdateStatus() error = nil, want an unsupported-phase error")
		}
		if calls := fc.recorded(); len(calls) != 0 {
			t.Errorf("signal-with-start calls = %d, want none", len(calls))
		}
	})
}
