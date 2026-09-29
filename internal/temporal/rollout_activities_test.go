package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/sdk/temporal"

	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

func TestLoadFirmwareActivity(t *testing.T) {
	t.Parallel()

	t.Run("returns the metadata the update commands need", func(t *testing.T) {
		t.Parallel()

		fakes := newRolloutFakes(rolloutTestSettings(), rolloutTestFirmware())
		activity := NewLoadFirmwareActivity(fakes)
		got, err := activity(context.Background(), LoadFirmware{FirmwareID: "fw-1", Model: "oak-s3"})
		if err != nil {
			t.Fatalf("load firmware: %v", err)
		}
		want := Firmware{ID: "fw-1", Version: "2.0.0", Checksum: "sha256:0f1e2d", Models: []string{"oak-s3"}}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("loaded firmware mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("an unknown firmware is a non-retryable refusal", func(t *testing.T) {
		t.Parallel()

		fakes := newRolloutFakes(rolloutTestSettings(), rolloutTestFirmware())
		activity := NewLoadFirmwareActivity(fakes)
		_, err := activity(context.Background(), LoadFirmware{FirmwareID: "fw-missing", Model: "oak-s3"})
		assertApplicationError(t, err, firmwareUnknownErrorType)
		if !errors.Is(err, firmware.ErrNotFound) {
			t.Errorf("error = %v, want it to carry the registry's not-found sentinel", err)
		}
	})

	t.Run("a model the firmware does not target is a non-retryable refusal", func(t *testing.T) {
		t.Parallel()

		fakes := newRolloutFakes(rolloutTestSettings(), rolloutTestFirmware())
		activity := NewLoadFirmwareActivity(fakes)
		_, err := activity(context.Background(), LoadFirmware{FirmwareID: "fw-1", Model: "oak-s3-mini"})
		assertApplicationError(t, err, firmwareMismatchErrorType)
	})

	t.Run("a registry failure is left retryable", func(t *testing.T) {
		t.Parallel()

		fakes := newRolloutFakes(rolloutTestSettings(), rolloutTestFirmware())
		fakes.firmwareErr = errors.New("mongo is down")
		activity := NewLoadFirmwareActivity(fakes)
		_, err := activity(context.Background(), LoadFirmware{FirmwareID: "fw-1", Model: "oak-s3"})
		if err == nil {
			t.Fatal("load firmware = nil error, want the registry failure")
		}
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) {
			t.Errorf("error = %v, want a retryable error rather than an application refusal", appErr)
		}
	})
}

func TestResolveWaveActivity(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	settings := rolloutTestSettings()
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1", "dev-2")
	activity := NewResolveWaveActivity(fakes)

	got, err := activity(context.Background(), ResolveWaveRequest{
		RolloutID: "ro-1", WaveID: rollout.WaveID("ro-1", 0, 50),
		Percent: 50, Region: "eu-west", Model: "oak-s3", StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("resolve wave: %v", err)
	}
	want := ResolvedWave{
		WaveID:    rollout.WaveID("ro-1", 0, 50),
		DeviceIDs: []string{"dev-1"},
		StartedAt: startedAt,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("resolved wave mismatch (-want +got):\n%s", diff)
	}

	// The recorded document is the wave's membership and start: everything downstream reads it
	// rather than re-deriving the target group.
	rec, ok := fakes.recordedWave(want.WaveID)
	if !ok {
		t.Fatal("the wave was not recorded")
	}
	if rec.RolloutID != "ro-1" || rec.Percent != 50 || rec.Status != rollout.WaveDispatching {
		t.Errorf("recorded wave = %+v, want rollout ro-1 at 50%% dispatching", rec)
	}

	t.Run("a store failure is reported", func(t *testing.T) {
		t.Parallel()

		broken := newRolloutFakes(settings, rolloutTestFirmware())
		broken.resolveErr = errors.New("mongo is down")
		_, err := NewResolveWaveActivity(broken)(context.Background(), ResolveWaveRequest{
			RolloutID: "ro-1", WaveID: "ro-1-w0-1", Percent: 1,
		})
		if err == nil || !strings.Contains(err.Error(), "mongo is down") {
			t.Errorf("resolve wave error = %v, want the store failure", err)
		}
	})
}

func TestRecordActivities(t *testing.T) {
	t.Parallel()

	settings := rolloutTestSettings()
	fakes := newRolloutFakes(settings, rolloutTestFirmware())
	ctx := context.Background()

	if err := NewRecordRolloutActivity(fakes)(ctx, RecordRolloutRequest{
		RolloutID: "ro-1", FirmwareID: "fw-1", WorkflowID: "rollout-ro-1",
		Region: "eu-west", Model: "oak-s3", Status: rollout.RolloutAwaitingApproval,
	}); err != nil {
		t.Fatalf("record rollout: %v", err)
	}
	want := rollout.RolloutRecord{
		ID: "ro-1", FirmwareID: "fw-1", Status: rollout.RolloutAwaitingApproval,
		WorkflowID: "rollout-ro-1", Region: "eu-west", Model: "oak-s3",
	}
	if got, ok := fakes.recordedRollout("ro-1"); !ok || got != want {
		t.Errorf("recorded rollout = %+v (recorded %v), want %+v", got, ok, want)
	}

	// A wave must be resolved before its state can move, so the recording activity is exercised
	// over a wave the resolver recorded first.
	resolved, err := NewResolveWaveActivity(fakes)(ctx, ResolveWaveRequest{
		RolloutID: "ro-1", WaveID: rollout.WaveID("ro-1", 0, 1), Percent: 1, Region: "eu-west", Model: "oak-s3",
	})
	if err != nil {
		t.Fatalf("resolve wave: %v", err)
	}
	if err := NewRecordWaveActivity(fakes)(ctx, RecordWaveRequest{
		RolloutID: "ro-1", WaveID: resolved.WaveID,
		Status: rollout.WaveHealthy, SuccessRate: 0.98,
	}); err != nil {
		t.Fatalf("record wave: %v", err)
	}
	rec, ok := fakes.recordedWave(resolved.WaveID)
	if !ok {
		t.Fatal("the wave was not recorded")
	}
	if rec.Status != rollout.WaveHealthy || rec.SuccessRate != 0.98 {
		t.Errorf("recorded wave = %+v, want it healthy at 0.98", rec)
	}

	t.Run("a wave recording failure is reported", func(t *testing.T) {
		t.Parallel()

		broken := newRolloutFakes(settings, rolloutTestFirmware())
		resolved, err := NewResolveWaveActivity(broken)(ctx, ResolveWaveRequest{
			RolloutID: "ro-1", WaveID: "ro-1-w0-1", Percent: 1,
		})
		if err != nil {
			t.Fatalf("resolve wave: %v", err)
		}
		broken.waveRecordErr = errors.New("mongo is down")
		err = NewRecordWaveActivity(broken)(ctx, RecordWaveRequest{
			RolloutID: "ro-1", WaveID: resolved.WaveID, Status: rollout.WaveEvaluating,
		})
		if err == nil || !strings.Contains(err.Error(), "mongo is down") {
			t.Errorf("record wave error = %v, want the store failure", err)
		}
	})

	t.Run("recording a wave that was never resolved is reported", func(t *testing.T) {
		t.Parallel()

		fresh := newRolloutFakes(settings, rolloutTestFirmware())
		err := NewRecordWaveActivity(fresh)(ctx, RecordWaveRequest{
			RolloutID: "ro-1", WaveID: "ro-1-w9-100", Status: rollout.WaveHealthy,
		})
		if err == nil {
			t.Error("record wave = nil error, want a wave without a recorded membership refused")
		}
	})

	t.Run("a recording failure is reported", func(t *testing.T) {
		t.Parallel()

		broken := newRolloutFakes(settings, rolloutTestFirmware())
		broken.recordErr = errors.New("mongo is down")
		err := NewRecordRolloutActivity(broken)(ctx, RecordRolloutRequest{
			RolloutID: "ro-1", Status: rollout.RolloutRunning,
		})
		if err == nil || !strings.Contains(err.Error(), "mongo is down") {
			t.Errorf("record rollout error = %v, want the store failure", err)
		}
	})
}

func TestEvaluateWaveActivity(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	settings := rolloutSettingsWith(5*time.Minute, 30*time.Minute, RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), "dev-1")
	// A resolved wave, so the evaluation has a recorded membership and window to read.
	resolved, err := NewResolveWaveActivity(fakes)(context.Background(), ResolveWaveRequest{
		RolloutID: "ro-1", WaveID: rollout.WaveID("ro-1", 0, 100), Percent: 100,
		Region: "eu-west", Model: "oak-s3", StartedAt: at.Add(-5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("resolve wave: %v", err)
	}
	fakes.scriptHealth(healthyAt(0.97))

	got, err := NewEvaluateWaveActivity(fakes)(context.Background(), EvaluateWaveRequest{
		RolloutID: "ro-1", WaveID: resolved.WaveID, At: at,
	})
	if err != nil {
		t.Fatalf("evaluate wave: %v", err)
	}
	want := WaveHealth{
		Verdict:      wavehealth.VerdictHealthy,
		SuccessRatio: 0.97,
		SampleSize:   100,
		WindowStart:  at.Add(-5 * time.Minute),
		WindowEnd:    at,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("evaluated health mismatch (-want +got):\n%s", diff)
	}

	t.Run("an evaluation failure is left retryable", func(t *testing.T) {
		t.Parallel()

		broken := newRolloutFakes(settings, rolloutTestFirmware())
		broken.healthFails = 1
		broken.scriptHealth(healthyAt(0.97))
		_, err := NewEvaluateWaveActivity(broken)(context.Background(), EvaluateWaveRequest{
			RolloutID: "ro-1", WaveID: "ro-1-w0-100", At: at,
		})
		if err == nil {
			t.Fatal("evaluate wave = nil error, want the evaluation failure")
		}
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) {
			t.Errorf("error = %v, want a retryable error", appErr)
		}
	})
}

func TestRolloutStartFailure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		err      error
		want     RolloutOutcome
		wantTerm bool
	}{
		{name: "an unknown firmware", want: OutcomeFirmwareUnknown, wantTerm: true,
			err: temporal.NewNonRetryableApplicationError("unknown", firmwareUnknownErrorType, nil)},
		{name: "a firmware that does not target the model", want: OutcomeFirmwareMismatch, wantTerm: true,
			err: temporal.NewNonRetryableApplicationError("mismatch", firmwareMismatchErrorType, nil)},
		{name: "another application error", err: temporal.NewApplicationError("boom", "something_else")},
		{name: "a plain failure", err: errors.New("mongo is down")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, terminal := rolloutStartFailure(tc.err)
			if terminal != tc.wantTerm || got != tc.want {
				t.Errorf("rolloutStartFailure(%v) = (%q, %v), want (%q, %v)",
					tc.err, got, terminal, tc.want, tc.wantTerm)
			}
		})
	}
}

// assertApplicationError fails the test unless err is a non-retryable application error of the
// given type — how the load-firmware activity reports a refusal nothing can retry away.
func assertApplicationError(t *testing.T, err error, wantType string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want a non-retryable %s refusal", wantType)
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T (%v), want *temporal.ApplicationError", err, err)
	}
	if appErr.Type() != wantType {
		t.Errorf("error type = %q, want %q", appErr.Type(), wantType)
	}
	if !appErr.NonRetryable() {
		t.Error("the refusal is retryable, want it to fail the rollout instead of retrying")
	}
}
