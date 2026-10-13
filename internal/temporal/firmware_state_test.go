package temporal

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/temporal"

	"github.com/DisMosGit/fleetops/internal/firmware"
)

// TestFirmwareWorkflowID pins the upload's identity: one version derives one workflow id, so a
// retried upload reaches the execution that already owns the version.
func TestFirmwareWorkflowID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		want    string
	}{
		{name: "a plain version", version: "2.0.0", want: "firmware-upload-2.0.0"},
		{name: "a version with a suffix", version: "2.0.0-rc.1", want: "firmware-upload-2.0.0-rc.1"},
		{name: "a build-numbered version", version: "1.2.3+build.7", want: "firmware-upload-1.2.3+build.7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FirmwareWorkflowID(tc.version); got != tc.want {
				t.Errorf("FirmwareWorkflowID(%q) = %q, want %q", tc.version, got, tc.want)
			}
		})
	}
}

// TestFirmwareUploadStateTransitions pins the state machine the upload's document and its state
// query both report: the states an upload reaches in order, and the rule that a concluded upload
// is terminal — no later transition changes its recorded state.
func TestFirmwareUploadStateTransitions(t *testing.T) {
	t.Parallel()

	record := firmware.Record{ID: "fw-1", Version: "2.0.0", Checksum: "sum", Size: 7}

	tests := []struct {
		name  string
		apply func(*firmwareUploadState)
		want  firmware.UploadState
	}{
		{
			name:  "an accepted upload reports acceptance",
			apply: func(*firmwareUploadState) {},
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadAccepted, Models: []string{"oak-s3"},
			},
		},
		{
			name:  "storing is reported while the binary streams",
			apply: func(s *firmwareUploadState) { s.storing() },
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadStoring, Models: []string{"oak-s3"},
			},
		},
		{
			name: "available carries the stored firmware",
			apply: func(s *firmwareUploadState) {
				s.storing()
				s.available(record)
			},
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadAvailable, Concluded: true,
				FirmwareID: "fw-1", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
			},
		},
		{
			name: "a refused upload carries its class and reason",
			apply: func(s *firmwareUploadState) {
				s.fail(firmware.FailureRefused, `unknown model "walnut-9"`)
			},
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadFailed, Concluded: true,
				Models: []string{"oak-s3"}, Failure: firmware.FailureRefused,
				Reason: `unknown model "walnut-9"`,
			},
		},
		{
			name: "a concluded upload does not become available later",
			apply: func(s *firmwareUploadState) {
				s.fail(firmware.FailureInternal, "storage failed")
				s.storing()
				s.available(record)
			},
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadFailed, Concluded: true,
				Models: []string{"oak-s3"}, Failure: firmware.FailureInternal,
				Reason: "storage failed",
			},
		},
		{
			name: "an available upload is not overwritten by a later failure",
			apply: func(s *firmwareUploadState) {
				s.available(record)
				s.fail(firmware.FailureInternal, "storage failed")
			},
			want: firmware.UploadState{
				Version: "2.0.0", State: firmware.UploadAvailable, Concluded: true,
				FirmwareID: "fw-1", Models: []string{"oak-s3"}, Checksum: "sum", Size: 7,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := firmwareUploadState{
				Version: "2.0.0", Models: []string{"oak-s3"}, SpoolPath: "/spool/upload-1",
				WorkflowID: "firmware-upload-2.0.0", State: firmware.UploadAccepted,
			}
			tc.apply(&state)
			if diff := cmp.Diff(tc.want, state.view()); diff != "" {
				t.Errorf("state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestFirmwareUploadStateValidate pins what the workflow refuses before it starts: an upload that
// names no version, no target model, or no spooled binary is not one it can drive.
func TestFirmwareUploadStateValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state firmwareUploadState
		valid bool
	}{
		{
			name: "a complete upload",
			state: firmwareUploadState{
				Version: "2.0.0", Models: []string{"oak-s3"}, SpoolPath: "/spool/upload-1",
			},
			valid: true,
		},
		{name: "no version", state: firmwareUploadState{Models: []string{"oak-s3"}, SpoolPath: "/s"}},
		{name: "no models", state: firmwareUploadState{Version: "2.0.0", SpoolPath: "/s"}},
		{name: "no spooled binary", state: firmwareUploadState{Version: "2.0.0", Models: []string{"oak-s3"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.state.validate()
			if tc.valid && err != nil {
				t.Errorf("validate() error = %v, want nil", err)
			}
			if !tc.valid && err == nil {
				t.Error("validate() error = nil, want the incomplete upload refused")
			}
		})
	}
}

// TestUploadFailure pins the classification an upload concludes with: the step's own refusal names
// the class and supplies the reason, and a failure that names no refusal is internal with a
// description that exposes nothing about the step that failed.
func TestUploadFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantClass  string
		wantReason string
	}{
		{
			name: "a refused model",
			err: temporal.NewNonRetryableApplicationError(
				`firmware incompatible with target device models: unknown model "walnut-9"`,
				firmwareModelsRefusedErrorType, nil),
			wantClass:  firmware.FailureRefused,
			wantReason: `firmware incompatible with target device models: unknown model "walnut-9"`,
		},
		{
			name: "a version another upload owns",
			err: temporal.NewNonRetryableApplicationError(
				"firmware version already registered: 2.0.0", firmwareConflictErrorType, nil),
			wantClass:  firmware.FailureConflict,
			wantReason: "firmware version already registered: 2.0.0",
		},
		{
			name:       "a step that could not be completed",
			err:        errors.New("activity error (type: StoreFirmwareBinary, scheduledEventID: 7)"),
			wantClass:  firmware.FailureInternal,
			wantReason: "the upload could not be completed",
		},
		{
			name: "an application error without a message",
			err: temporal.NewNonRetryableApplicationError(
				"", firmwareModelsRefusedErrorType, nil),
			wantClass:  firmware.FailureRefused,
			wantReason: "the upload could not be completed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			class, reason := uploadFailure(tc.err)
			if class != tc.wantClass || reason != tc.wantReason {
				t.Errorf("uploadFailure() = (%q, %q), want (%q, %q)",
					class, reason, tc.wantClass, tc.wantReason)
			}
		})
	}
}
