package devices

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeFirmwareRecords is a hand-written firmwareRecords double: the version each device's record
// holds, and scriptable read and write failures.
type fakeFirmwareRecords struct {
	// recorded is the version each device's record holds. A device absent from it has no record.
	recorded map[string]string
	// readErr fails every read.
	readErr error
	// writeErr fails every conditional write.
	writeErr error
	// matchedButUnchanged makes the conditional write report that it matched nothing, which is
	// what a concurrent writer having already corrected the record looks like.
	matchedButUnchanged bool
	// writes are the corrections the fake was asked to make, in order.
	writes []string
	// writeCalls counts the conditional writes.
	writeCalls int
}

// currentFirmware answers the version the fake records for a device.
func (f *fakeFirmwareRecords) currentFirmware(_ context.Context, deviceID string) (string, error) {
	if f.readErr != nil {
		return "", f.readErr
	}
	version, ok := f.recorded[deviceID]
	if !ok {
		return "", ErrNoRecord
	}
	return version, nil
}

// correctFirmware writes version where the record disagrees with it.
func (f *fakeFirmwareRecords) correctFirmware(_ context.Context, deviceID, version string) (bool, error) {
	f.writeCalls++
	if f.writeErr != nil {
		return false, f.writeErr
	}
	if f.matchedButUnchanged || f.recorded[deviceID] == version {
		return false, nil
	}
	f.recorded[deviceID] = version
	f.writes = append(f.writes, version)
	return true, nil
}

// newFakeFirmwareRecords returns a fake fleet recording the given version for dev-1.
func newFakeFirmwareRecords(version string) *fakeFirmwareRecords {
	return &fakeFirmwareRecords{recorded: map[string]string{"dev-1": version}}
}

func TestReconcileFirmware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// records is the fake fleet the reconciliation runs against.
		records *fakeFirmwareRecords
		// deviceID and version are the arguments; empty means the default dev-1 / 1.0.0.
		deviceID  string
		version   string
		want      FirmwareState
		wantErr   string
		wantWrote string
		// wantWriteCalls is how many conditional writes should have happened.
		wantWriteCalls int
	}{
		{
			name:    "a matching record agrees and is not written",
			records: newFakeFirmwareRecords("1.0.0"),
			want:    FirmwareAgreed,
		},
		{
			name:           "a disagreeing record is corrected to the reported version",
			records:        newFakeFirmwareRecords("2.0.0"),
			want:           FirmwareCorrected,
			wantWrote:      "1.0.0",
			wantWriteCalls: 1,
		},
		{
			name:    "a device the fleet holds no record for is missing",
			records: &fakeFirmwareRecords{recorded: map[string]string{}},
			want:    FirmwareMissing,
		},
		{
			name:    "a record a concurrent writer already corrected agrees",
			records: &fakeFirmwareRecords{recorded: map[string]string{"dev-1": "2.0.0"}, matchedButUnchanged: true},
			want:    FirmwareAgreed,
			// The write was attempted and matched nothing: the record moved under us.
			wantWriteCalls: 1,
		},
		{
			name:     "an empty device id is refused",
			records:  newFakeFirmwareRecords("1.0.0"),
			deviceID: "",
			wantErr:  "device id and version required",
		},
		{
			name:    "an empty version is refused rather than written",
			records: newFakeFirmwareRecords("1.0.0"),
			version: "",
			wantErr: "device id and version required",
		},
		{
			name:    "a read failure is wrapped",
			records: &fakeFirmwareRecords{readErr: errors.New("mongo is unavailable")},
			wantErr: "mongo is unavailable",
		},
		{
			name: "a write failure is wrapped",
			records: &fakeFirmwareRecords{
				recorded: map[string]string{"dev-1": "2.0.0"},
				writeErr: errors.New("write concern failed"),
			},
			wantErr:        "write concern failed",
			wantWriteCalls: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deviceID, version := "dev-1", "1.0.0"
			if tc.deviceID != "" || tc.name == "an empty device id is refused" {
				deviceID = tc.deviceID
			}
			if tc.version != "" || tc.name == "an empty version is refused rather than written" {
				version = tc.version
			}

			got, err := (&Store{firmware: tc.records}).ReconcileFirmware(
				context.Background(), deviceID, version)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ReconcileFirmware() error = nil, want %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("ReconcileFirmware() error = %v, want it to name %q", err, tc.wantErr)
				}
				if got != "" {
					t.Errorf("ReconcileFirmware() state = %q, want none alongside an error", got)
				}
				if tc.records.writeCalls != tc.wantWriteCalls {
					t.Errorf("conditional writes = %d, want %d", tc.records.writeCalls, tc.wantWriteCalls)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReconcileFirmware() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("ReconcileFirmware() state = %q, want %q", got, tc.want)
			}
			if tc.records.writeCalls != tc.wantWriteCalls {
				t.Errorf("conditional writes = %d, want %d", tc.records.writeCalls, tc.wantWriteCalls)
			}
			if tc.wantWrote != "" {
				if held := tc.records.recorded["dev-1"]; held != tc.wantWrote {
					t.Errorf("recorded version = %q, want %q", held, tc.wantWrote)
				}
			}
			if tc.wantWriteCalls == 1 && tc.wantWrote != "" {
				if len(tc.records.writes) != 1 || tc.records.writes[0] != tc.wantWrote {
					t.Errorf("corrections = %v, want exactly [%s]", tc.records.writes, tc.wantWrote)
				}
			}
		})
	}
}
