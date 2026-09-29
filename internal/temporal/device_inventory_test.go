package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/devices"
)

// fakeInventory is a hand-written FirmwareInventory double: the state it answers each reconciliation
// with, the devices and versions it was asked about, and one scripted failure.
type fakeInventory struct {
	mu sync.Mutex
	// answers is the state the fake reports per device; devices absent from it take the default.
	answers map[string]devices.FirmwareState
	// defaultState is the state the fake reports for a device with no scripted answer.
	defaultState devices.FirmwareState
	err          error
	calls        []inventoryCall
}

// inventoryCall is one reconciliation the fake was asked for.
type inventoryCall struct {
	deviceID string
	version  string
}

// newFakeInventory returns an inventory answering the given state for every device.
func newFakeInventory(state devices.FirmwareState) *fakeInventory {
	return &fakeInventory{answers: map[string]devices.FirmwareState{}, defaultState: state}
}

// ReconcileFirmware records the call and answers the scripted state.
func (f *fakeInventory) ReconcileFirmware(_ context.Context, deviceID, version string) (devices.FirmwareState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, inventoryCall{deviceID: deviceID, version: version})
	if f.err != nil {
		return "", f.err
	}
	if state, ok := f.answers[deviceID]; ok {
		return state, nil
	}
	return f.defaultState, nil
}

// recorded returns the reconciliations the fake was asked for, in order.
func (f *fakeInventory) recorded() []inventoryCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]inventoryCall(nil), f.calls...)
}

func TestReconcileInventoryActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// state is the device state the reader answers with.
		state State
		// readErr fails the state read.
		readErr error
		// inventory is the fleet the activity reconciles against; nil means one that agrees.
		inventory *fakeInventory
		// deviceID overrides the request's device, for the refusal.
		deviceID string
		want     DeviceInventory
		wantErr  string
		// wantReconciled is the version the fleet should have been asked to reconcile; empty
		// means it should not have been asked at all.
		wantReconciled string
	}{
		{
			name:           "a record that already agrees is reported as agreed",
			state:          State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			want:           DeviceInventory{DeviceID: "dev-1", Outcome: InventoryAgreed, Version: "1.0.0"},
			wantReconciled: "1.0.0",
		},
		{
			name:           "a record that disagreed is reported as corrected",
			state:          State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			inventory:      newFakeInventory(devices.FirmwareCorrected),
			want:           DeviceInventory{DeviceID: "dev-1", Outcome: InventoryCorrected, Version: "1.0.0"},
			wantReconciled: "1.0.0",
		},
		{
			name:  "a device the fleet holds no record for is unverified",
			state: State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			inventory: func() *fakeInventory {
				return newFakeInventory(devices.FirmwareMissing)
			}(),
			want: DeviceInventory{
				DeviceID: "dev-1", Outcome: InventoryUnverified,
				Detail: "the fleet holds no record for device dev-1",
			},
			wantReconciled: "1.0.0",
		},
		{
			name:  "a device whose workflow holds no version is unverified and never written",
			state: State{DeviceID: "dev-1"},
			want: DeviceInventory{
				DeviceID: "dev-1", Outcome: InventoryUnverified,
				Detail: "device dev-1 holds no firmware version",
			},
		},
		{
			name:    "a state read that fails is an error rather than an outcome",
			state:   State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			readErr: fmt.Errorf("read device dev-1 state: %w", ErrDeviceNotFound),
			wantErr: "device workflow not found",
		},
		{
			name:  "a reconciliation that fails is an error rather than an outcome",
			state: State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			inventory: func() *fakeInventory {
				f := newFakeInventory(devices.FirmwareAgreed)
				f.err = errors.New("mongo is unavailable")
				return f
			}(),
			wantErr:        "mongo is unavailable",
			wantReconciled: "1.0.0",
		},
		{
			name:     "a request without a device is refused",
			state:    State{DeviceID: "dev-1", CurrentFw: "1.0.0"},
			deviceID: "",
			wantErr:  "device id required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := &fakeDeviceStates{}
			if tc.readErr != nil {
				reader.err = tc.readErr
			} else {
				reader.scriptStates(tc.state)
			}
			inventory := tc.inventory
			if inventory == nil {
				inventory = newFakeInventory(devices.FirmwareAgreed)
			}
			reconcile := NewReconcileInventoryActivity(reader, inventory)

			req := ReconcileInventoryRequest{RolloutID: "ro-1", WaveID: "ro-1-w0-1", DeviceID: "dev-1"}
			if tc.deviceID != "" || tc.name == "a request without a device is refused" {
				req.DeviceID = tc.deviceID
			}
			got, err := reconcile(context.Background(), req)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("reconcile() error = nil, want %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("reconcile() error = %v, want it to name %q", err, tc.wantErr)
				}
				if got != (DeviceInventory{}) {
					t.Errorf("reconcile() outcome = %+v, want none alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("reconcile() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("reconcile outcome mismatch (-want +got):\n%s", diff)
			}
			calls := inventory.recorded()
			if tc.wantReconciled == "" {
				if len(calls) != 0 {
					t.Errorf("reconciliations = %v, want none", calls)
				}
				return
			}
			want := []inventoryCall{{deviceID: "dev-1", version: tc.wantReconciled}}
			if diff := cmp.Diff(want, calls, cmp.AllowUnexported(inventoryCall{})); diff != "" {
				t.Errorf("reconciliations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
