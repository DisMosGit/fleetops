package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/devices"
)

// fakeSnapshotter is a hand-written StateSnapshotter double recording what it saves.
type fakeSnapshotter struct {
	saved []devices.Snapshot
	err   error
}

func (s *fakeSnapshotter) SaveSnapshot(_ context.Context, snap devices.Snapshot) error {
	if s.err != nil {
		return s.err
	}
	s.saved = append(s.saved, snap)
	return nil
}

func TestSnapshotActivityPayload(t *testing.T) {
	t.Parallel()

	decided := time.Unix(5000, 0).UTC()
	heard := time.Unix(4000, 0).UTC()

	t.Run("complete state projects onto the stored record", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
		s.Region, s.Model, s.CurrentFw, s.Online = "eu-west", "oak-s3", "fw-2", true
		s.LastHeartbeatAt = heard
		s.Pending = &PendingCommand{
			Command: CommandIssuedSignal{
				CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
				FirmwareID: "fw-2", Version: "fw-2", Checksum: "sum",
			},
			Dispatched: true,
		}
		s.Config = ConfigSnapshot{Version: 7, Data: json.RawMessage(`{"interval":"5s","n":3}`)}

		store := &fakeSnapshotter{}
		if err := NewSnapshotActivity(store)(context.Background(), snapshotOf(s, decided)); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if len(store.saved) != 1 {
			t.Fatalf("store received %d snapshots, want 1", len(store.saved))
		}
		want := devices.Snapshot{
			DeviceID:        "dev-1",
			Region:          "eu-west",
			Model:           "oak-s3",
			CurrentFw:       "fw-2",
			Online:          true,
			LastHeartbeatAt: heard,
			Pending: &devices.SnapshotCommand{
				CommandID:  "cmd-1",
				DeviceID:   "dev-1",
				Kind:       string(CommandKindUpdate),
				FirmwareID: "fw-2",
				Version:    "fw-2",
				Checksum:   "sum",
				Dispatched: true,
			},
			Config:     devices.SnapshotConfig{Version: 7, Data: map[string]any{"interval": "5s", "n": float64(3)}},
			SnapshotAt: decided,
		}
		if diff := cmp.Diff(want, store.saved[0]); diff != "" {
			t.Errorf("stored snapshot mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("state without pending or configuration stays sparse", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())

		store := &fakeSnapshotter{}
		if err := NewSnapshotActivity(store)(context.Background(), snapshotOf(s, decided)); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		want := devices.Snapshot{
			DeviceID:   "dev-1",
			Config:     devices.SnapshotConfig{},
			SnapshotAt: decided,
		}
		if diff := cmp.Diff(want, store.saved[0]); diff != "" {
			t.Errorf("stored snapshot mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSnapshotActivityErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("mongo is down")
	decided := time.Unix(5000, 0).UTC()

	t.Run("write errors are wrapped", func(t *testing.T) {
		t.Parallel()
		store := &fakeSnapshotter{err: boom}
		err := NewSnapshotActivity(store)(context.Background(),
			snapshotOf(newDeviceState("dev-1", testSettings()), decided))
		if !errors.Is(err, boom) {
			t.Errorf("snapshot error = %v, want it wrapped", err)
		}
	})

	t.Run("unparseable configuration is refused before any write", func(t *testing.T) {
		t.Parallel()
		s := newDeviceState("dev-1", testSettings())
		s.Config = ConfigSnapshot{Version: 1, Data: json.RawMessage(`{not json`)}
		store := &fakeSnapshotter{}
		err := NewSnapshotActivity(store)(context.Background(), snapshotOf(s, decided))
		if err == nil {
			t.Fatal("snapshot with unparseable configuration returned nil")
		}
		if len(store.saved) != 0 {
			t.Errorf("store received %d snapshots, want none", len(store.saved))
		}
	})
}
