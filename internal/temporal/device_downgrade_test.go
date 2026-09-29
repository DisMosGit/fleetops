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

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// fakeVersions is a hand-written FirmwareVersions double: the records it holds keyed by version,
// the versions it was asked for, and one scripted failure.
type fakeVersions struct {
	mu      sync.Mutex
	records map[string]firmware.Record
	err     error
	lookups []string
}

// newFakeVersions returns a registry holding the given records, keyed by version.
func newFakeVersions(records ...firmware.Record) *fakeVersions {
	byVersion := make(map[string]firmware.Record, len(records))
	for _, rec := range records {
		byVersion[rec.Version] = rec
	}
	return &fakeVersions{records: byVersion}
}

// MetadataByVersion answers the record carrying version.
func (f *fakeVersions) MetadataByVersion(_ context.Context, version string) (firmware.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups = append(f.lookups, version)
	if f.err != nil {
		return firmware.Record{}, f.err
	}
	rec, ok := f.records[version]
	if !ok {
		return firmware.Record{}, fmt.Errorf("find firmware version %s: %w", version, firmware.ErrNotFound)
	}
	return rec, nil
}

// asked returns the versions the lookup was asked for, in order.
func (f *fakeVersions) asked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.lookups...)
}

// deployedFw is the firmware version the rollouts in these tests deploy, and the one a downgrade
// rolls a device back from.
func deployedFw() string { return "2.0.0" }

// previousFirmware is the registry record a device's previous version resolves to.
func previousFirmware() firmware.Record {
	return firmware.Record{
		ID: "fw-0", Version: "1.0.0", Models: []string{"oak-s3"}, Checksum: "sha256:0f1e",
	}
}

// restoreCommand is the command one rollback's downgrade activity delivers to dev-1.
func restoreCommand() CommandIssuedSignal {
	return CommandIssuedSignal{
		CommandID:  rollbackCommandID("ro-1", "dev-1"),
		DeviceID:   "dev-1",
		Kind:       CommandKindUpdate,
		FirmwareID: previousFirmware().ID,
		Version:    previousFirmware().Version,
		Checksum:   previousFirmware().Checksum,
	}
}

// deviceOn returns the authoritative state of a device running current with the given history.
func deviceOn(current, previous, model string) State {
	return State{DeviceID: "dev-1", CurrentFw: current, PreviousFw: previous, Model: model}
}

// restorePending returns the state with the rollback's restore command still outstanding.
func restorePending(s State) State {
	s.Pending = &PendingCommand{Command: restoreCommand(), Dispatched: true}
	return s
}

// restoreConcluded returns the state with the rollback's restore command concluded.
func restoreConcluded(s State, outcome CommandOutcome, detail string) State {
	s.LastCommand = &ConcludedCommand{
		Command: restoreCommand(), Outcome: outcome, Detail: detail,
	}
	return s
}

// laterCommandConcluded returns the state with a newer command concluded instead of the restore:
// what a restore superseded before it concluded looks like.
func laterCommandConcluded(s State) State {
	s.LastCommand = &ConcludedCommand{
		Command: CommandIssuedSignal{
			CommandID: "some-later-command", DeviceID: "dev-1", Kind: CommandKindUpdate,
			FirmwareID: "fw-3", Version: "3.0.0",
		},
		Outcome: OutcomeSucceeded,
	}
	return s
}

// downgradeRequest is the request one device's downgrade activity is asked to run.
func downgradeRequest(deadline time.Time) DowngradeDeviceRequest {
	return DowngradeDeviceRequest{
		RolloutID:  "ro-1",
		WaveID:     "ro-1-w0-1",
		DeviceID:   "dev-1",
		DeployedFw: deployedFw(),
		Deadline:   deadline,
	}
}

// runningDeployed is a device that took the deployed firmware and remembers what it ran before, the
// starting point of every restore that has something to do.
func runningDeployed() State {
	return deviceOn(deployedFw(), previousFirmware().Version, "oak-s3")
}

func TestDowngradeDeviceActivity(t *testing.T) {
	t.Parallel()

	// A generous deadline for the cases that converge on a scripted observation.
	future := func() time.Time { return time.Now().Add(time.Minute) }
	past := func() time.Time { return time.Now().Add(-time.Second) }
	// A deadline a few polls away, for the cases whose script never converges: a device that
	// never reports is what the deadline exists for.
	soon := func() time.Time { return time.Now().Add(20 * time.Millisecond) }

	tests := []struct {
		name string
		// deadline is the request's deadline.
		deadline time.Time
		// script prepares the device-state reader.
		script func(*fakeDeviceStates)
		// versions is the registry the activity resolves the previous version in; nil means the
		// registry that holds the previous firmware.
		versions *fakeVersions
		// registryErr fails every registry lookup.
		registryErr error
		// deliverErr fails every command delivery.
		deliverErr error
		want       DeviceRestore
		wantErr    string
		// wantLookup is the version the registry should have been asked for; empty means the
		// activity should not have asked at all.
		wantLookup string
		// wantDelivered is how many commands the seam accepted.
		wantDelivered int
	}{
		{
			name:     "a device that took the firmware is restored to the version it ran before",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(
					runningDeployed(),
					restorePending(runningDeployed()),
					restoreConcluded(runningDeployed(), OutcomeSucceeded, ""),
				)
			},
			want:          DeviceRestore{DeviceID: "dev-1", Outcome: RestoreRestored, FoundFw: "2.0.0"},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a device that concludes after a few observations is still restored",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(
					runningDeployed(),
					restorePending(runningDeployed()),
					restorePending(runningDeployed()),
					restorePending(runningDeployed()),
					restoreConcluded(runningDeployed(), OutcomeSucceeded, ""),
				)
			},
			want:          DeviceRestore{DeviceID: "dev-1", Outcome: RestoreRestored, FoundFw: "2.0.0"},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a failed restore carries the device's own detail",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(
					runningDeployed(),
					restoreConcluded(runningDeployed(), OutcomeFailed, "flash error"),
				)
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreFailed, Detail: "flash error", FoundFw: "2.0.0",
			},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a device that never reports runs out of time",
			deadline: soon(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(runningDeployed(), restorePending(runningDeployed()))
			},
			want:          DeviceRestore{DeviceID: "dev-1", Outcome: RestoreUnreported, FoundFw: "2.0.0"},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a deadline that has already passed is unreported",
			deadline: past(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(runningDeployed(), restorePending(runningDeployed()))
			},
			want:          DeviceRestore{DeviceID: "dev-1", Outcome: RestoreUnreported, FoundFw: "2.0.0"},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a superseded restore is not a success",
			deadline: soon(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(runningDeployed(), laterCommandConcluded(runningDeployed()))
			},
			want:          DeviceRestore{DeviceID: "dev-1", Outcome: RestoreUnreported, FoundFw: "2.0.0"},
			wantLookup:    "1.0.0",
			wantDelivered: 1,
		},
		{
			name:     "a device that never took the firmware is skipped",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(deviceOn("1.0.0", "", "oak-s3"))
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreSkipped, FoundFw: "1.0.0",
				Detail: "device runs 1.0.0, not the rolled-back firmware 2.0.0",
			},
		},
		{
			name:     "a device an earlier attempt restored is skipped",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(restoreConcluded(deviceOn("1.0.0", "2.0.0", "oak-s3"), OutcomeSucceeded, ""))
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreSkipped, FoundFw: "1.0.0",
				Detail: "device runs 1.0.0, not the rolled-back firmware 2.0.0",
			},
		},
		{
			name:     "a device with no previous version is unavailable",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(deviceOn("2.0.0", "", "oak-s3"))
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreUnavailable, FoundFw: "2.0.0",
				Detail: "device dev-1 records no previous firmware version",
			},
		},
		{
			name:     "a previous version the registry does not hold is unavailable",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(deviceOn("2.0.0", "9.9.9", "oak-s3"))
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreUnavailable, FoundFw: "2.0.0",
				Detail: "firmware version 9.9.9 is unknown to the registry",
			},
			wantLookup: "9.9.9",
		},
		{
			name:     "a previous version that does not target the device's model is unavailable",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(deviceOn("2.0.0", "1.0.0", "birch-x1"))
			},
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreUnavailable, FoundFw: "2.0.0",
				Detail: "firmware version 1.0.0 does not target model birch-x1",
			},
			wantLookup: "1.0.0",
		},
		{
			name:     "a restore that cannot be delivered is reported rather than raised",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(runningDeployed())
			},
			deliverErr: errors.New("device workflow is unreachable"),
			want: DeviceRestore{
				DeviceID: "dev-1", Outcome: RestoreUnavailable, FoundFw: "2.0.0",
				Detail: "deliver restore command: device workflow is unreachable",
			},
			wantLookup: "1.0.0",
		},
		{
			name:     "a state read that fails is an error rather than an outcome",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.err = fmt.Errorf("read device dev-1 state: %w", ErrDeviceNotFound)
			},
			wantErr: "device workflow not found",
		},
		{
			name:     "a registry lookup that fails is an error rather than an outcome",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(runningDeployed())
			},
			registryErr: errors.New("mongo is unavailable"),
			wantErr:     "mongo is unavailable",
		},
		{
			name:     "a request without a device is refused",
			deadline: future(),
			script:   func(*fakeDeviceStates) {},
			wantErr:  "device id required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := &fakeDeviceStates{}
			tc.script(reader)
			commander := &fakeCommander{err: tc.deliverErr}
			versions := tc.versions
			if versions == nil {
				versions = newFakeVersions(previousFirmware())
			}
			versions.err = tc.registryErr
			// The poll interval is shrunk so the cases that observe more than once do not
			// spend the production interval sleeping.
			restore := NewDowngradeDeviceActivity(
				commander, reader, versions, WithDowngradePollInterval(time.Millisecond),
			)

			// A context bound is a second, independent stop: a wait that ignored its deadline
			// would hang the suite instead of failing it.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			req := downgradeRequest(tc.deadline)
			if tc.wantErr == "device id required" {
				req.DeviceID = ""
			}
			got, err := restore(ctx, req)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("restore() error = nil, want %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("restore() error = %v, want it to name %q", err, tc.wantErr)
				}
				if got != (DeviceRestore{}) {
					t.Errorf("restore() outcome = %+v, want none alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("restore() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("restore outcome mismatch (-want +got):\n%s", diff)
			}
			if tc.wantLookup == "" {
				if asked := versions.asked(); len(asked) != 0 {
					t.Errorf("registry lookups = %v, want none", asked)
				}
			} else if diff := cmp.Diff([]string{tc.wantLookup}, versions.asked()); diff != "" {
				t.Errorf("registry lookups mismatch (-want +got):\n%s", diff)
			}
			delivered := commander.recorded()
			if len(delivered) != tc.wantDelivered {
				t.Fatalf("commands delivered = %d, want %d", len(delivered), tc.wantDelivered)
			}
			if tc.wantDelivered == 1 {
				if diff := cmp.Diff(restoreCommand(), delivered[0]); diff != "" {
					t.Errorf("delivered command mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// fakeRestoreDevice is a stateful device double for the retry story: it answers as a device still
// running the deployed firmware with the rollback's restore command outstanding, until the test
// restores it. Unlike a scripted sequence its answer does not advance per read, which is what makes
// the number of observations a wait makes irrelevant to the test.
type fakeRestoreDevice struct {
	mu       sync.Mutex
	restored bool
}

// restore marks the device as back on the firmware it ran before.
func (d *fakeRestoreDevice) restore() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.restored = true
}

// State implements DeviceStateReader.
func (d *fakeRestoreDevice) State(_ context.Context, deviceID string) (State, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.restored {
		return restoreConcluded(deviceOn("1.0.0", "2.0.0", "oak-s3"), OutcomeSucceeded, ""), nil
	}
	return restorePending(deviceOn(deployedFw(), previousFirmware().Version, "oak-s3")), nil
}

// TestDowngradeDeviceActivityIsIdempotent pins the retry story of one device's restore: every
// attempt addresses the device under the same command id, and an attempt that finds the device
// already back on its previous firmware commands nothing at all.
func TestDowngradeDeviceActivityIsIdempotent(t *testing.T) {
	t.Parallel()

	device := &fakeRestoreDevice{}
	commander := &fakeCommander{}
	restore := NewDowngradeDeviceActivity(
		commander, device, newFakeVersions(previousFirmware()),
		WithDowngradePollInterval(time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two attempts run while the device is still on the deployed firmware and never reports its
	// restore: each delivers the restore, and each delivers the same one.
	unreported := DeviceRestore{DeviceID: "dev-1", Outcome: RestoreUnreported, FoundFw: "2.0.0"}
	for attempt := 1; attempt <= 2; attempt++ {
		got, err := restore(ctx, downgradeRequest(time.Now().Add(10*time.Millisecond)))
		if err != nil {
			t.Fatalf("attempt %d error = %v", attempt, err)
		}
		if diff := cmp.Diff(unreported, got); diff != "" {
			t.Fatalf("attempt %d outcome mismatch (-want +got):\n%s", attempt, diff)
		}
	}

	// The device reports its restore, and is then found already restored — including by an
	// attempt that follows a redelivered result.
	device.restore()
	skipped := DeviceRestore{
		DeviceID: "dev-1", Outcome: RestoreSkipped, FoundFw: "1.0.0",
		Detail: "device runs 1.0.0, not the rolled-back firmware 2.0.0",
	}
	for _, attempt := range []string{"after the restore concluded", "on a redelivered result"} {
		got, err := restore(ctx, downgradeRequest(time.Now().Add(time.Minute)))
		if err != nil {
			t.Fatalf("attempt %s error = %v", attempt, err)
		}
		if diff := cmp.Diff(skipped, got); diff != "" {
			t.Errorf("attempt %s outcome mismatch (-want +got):\n%s", attempt, diff)
		}
	}

	delivered := commander.recorded()
	if len(delivered) != 2 {
		t.Fatalf("commands delivered = %d, want 2 (one per waiting attempt)", len(delivered))
	}
	want := restoreCommand()
	for i, cmd := range delivered {
		if diff := cmp.Diff(want, cmd); diff != "" {
			t.Errorf("delivery %d mismatch (-want +got):\n%s", i+1, diff)
		}
	}
}
