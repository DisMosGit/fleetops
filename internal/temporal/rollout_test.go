package temporal

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// rolloutTestInput is the start input the rollout contract tests build on: a ten-percent canary
// followed by an approval-gated full wave, over a five-minute window and a thirty-minute decision
// timeout.
func rolloutTestInput() RolloutInput {
	return RolloutInput{
		RolloutRequest: RolloutRequest{
			RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
		},
		Settings: RolloutSettings{
			HealthWindow:    5 * time.Minute,
			DecisionTimeout: 30 * time.Minute,
			Waves: []RolloutWave{
				{Percent: 10},
				{Percent: 100, RequireApproval: true},
			},
		},
	}
}

func TestRolloutWorkflowID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		rolloutID string
		want      string
	}{
		{name: "derives from the rollout id", rolloutID: "ro-1", want: "rollout-ro-1"},
		{name: "is stable", rolloutID: "ro-1", want: "rollout-ro-1"},
		{name: "distinct rollouts never share an execution", rolloutID: "ro-2", want: "rollout-ro-2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := RolloutWorkflowID(tc.rolloutID); got != tc.want {
				t.Errorf("RolloutWorkflowID(%q) = %q, want %q", tc.rolloutID, got, tc.want)
			}
		})
	}
}

func TestCommandID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		waveID   string
		deviceID string
		want     string
	}{
		{name: "names the wave and the device", waveID: "ro-1-w0-10", deviceID: "dev-1",
			want: "ro-1-w0-10-dev-1"},
		{name: "a second device of the same wave differs", waveID: "ro-1-w0-10", deviceID: "dev-2",
			want: "ro-1-w0-10-dev-2"},
		{name: "the same device in another wave differs", waveID: "ro-1-w1-100", deviceID: "dev-1",
			want: "ro-1-w1-100-dev-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := CommandID(tc.waveID, tc.deviceID)
			if got != tc.want {
				t.Errorf("CommandID(%q, %q) = %q, want %q", tc.waveID, tc.deviceID, got, tc.want)
			}
			// Stability is what makes a redelivered command a no-op for the device.
			if again := CommandID(tc.waveID, tc.deviceID); again != got {
				t.Errorf("CommandID is not stable: %q then %q", got, again)
			}
		})
	}
}

func TestRolloutStateValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*RolloutInput)
		wantErr string // substring the error must contain; empty means valid
	}{
		{name: "the demo input is valid"},
		{name: "missing rollout id", mutate: func(in *RolloutInput) {
			in.RolloutID = ""
		}, wantErr: "rollout id"},
		{name: "missing firmware id", mutate: func(in *RolloutInput) {
			in.FirmwareID = ""
		}, wantErr: "firmware id"},
		{name: "missing region", mutate: func(in *RolloutInput) {
			in.Region = ""
		}, wantErr: "region"},
		{name: "missing model", mutate: func(in *RolloutInput) {
			in.Model = ""
		}, wantErr: "model"},
		{name: "zero health window", mutate: func(in *RolloutInput) {
			in.Settings.HealthWindow = 0
		}, wantErr: "health window"},
		{name: "negative health window", mutate: func(in *RolloutInput) {
			in.Settings.HealthWindow = -time.Minute
		}, wantErr: "health window"},
		{name: "decision timeout below the window", mutate: func(in *RolloutInput) {
			in.Settings.DecisionTimeout = time.Minute
		}, wantErr: "decision timeout"},
		{name: "empty sequence", mutate: func(in *RolloutInput) {
			in.Settings.Waves = nil
		}, wantErr: "wave sequence"},
		{name: "zero share", mutate: func(in *RolloutInput) {
			in.Settings.Waves = []RolloutWave{{Percent: 0}, {Percent: 100}}
		}, wantErr: "percent 0"},
		{name: "share above the pool", mutate: func(in *RolloutInput) {
			in.Settings.Waves = []RolloutWave{{Percent: 101}}
		}, wantErr: "percent 101"},
		{name: "descending shares", mutate: func(in *RolloutInput) {
			in.Settings.Waves = []RolloutWave{{Percent: 50}, {Percent: 25}, {Percent: 100}}
		}, wantErr: "above the previous"},
		{name: "sequence not ending at the whole pool", mutate: func(in *RolloutInput) {
			in.Settings.Waves = []RolloutWave{{Percent: 10}, {Percent: 50}}
		}, wantErr: "must be 100"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := rolloutTestInput()
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			err := newRolloutState(in).validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validate() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("validate() error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}

	t.Run("a state from a newer schema is refused", func(t *testing.T) {
		t.Parallel()

		state := newRolloutState(rolloutTestInput())
		state.CarryVersion = rolloutCarryVersion + 1
		err := state.validate()
		if err == nil {
			t.Fatal("validate() = nil, want an unsupported-version error")
		}
		if !strings.Contains(err.Error(), "carry version") {
			t.Errorf("validate() error = %q, want it to name the carry version", err)
		}
	})

	t.Run("a state whose waves do not match its sequence is refused", func(t *testing.T) {
		t.Parallel()

		state := newRolloutState(rolloutTestInput())
		state.Waves = state.Waves[:1]
		err := state.validate()
		if err == nil {
			t.Fatal("validate() = nil, want a wave-count error")
		}
		if !strings.Contains(err.Error(), "waves for a sequence") {
			t.Errorf("validate() error = %q, want it to name the wave count", err)
		}
	})
}

func TestRolloutView(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		apply func(*rolloutState)
		want  RolloutView
	}{
		{
			name: "a running rollout reports its position",
			apply: func(s *rolloutState) {
				s.startWave(0, ResolvedWave{WaveID: "ro-1-w0-10", DeviceIDs: []string{"dev-1", "dev-2"}})
				s.recordWave(0, rollout.WaveHealthy, 0.99)
				s.startWave(1, ResolvedWave{WaveID: "ro-1-w1-100", DeviceIDs: []string{"dev-3"}})
				s.recordWave(1, rollout.WaveEvaluating, 0)
			},
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutRunning,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WaveHealthy, SuccessRate: 0.99, TargetCount: 2},
					{Percent: 100, Status: rollout.WaveEvaluating, TargetCount: 1},
				},
				Current: 1,
			},
		},
		{
			name:  "a rollout waiting for approval reports what it waits for",
			apply: func(s *rolloutState) { s.holdForApproval(1) },
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutAwaitingApproval,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WavePending},
					{Percent: 100, Status: rollout.WavePending},
				},
				Current: 1,
			},
		},
		{
			name:  "a banked approval is reported while the rollout runs",
			apply: func(s *rolloutState) { s.applyApproval() },
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutRunning,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WavePending},
					{Percent: 100, Status: rollout.WavePending},
				},
				Current:             noWave,
				ApprovalOutstanding: true,
			},
		},
		{
			name: "a completed rollout reports why it concluded",
			apply: func(s *rolloutState) {
				s.startWave(0, ResolvedWave{WaveID: "ro-1-w0-10", DeviceIDs: []string{"dev-1"}})
				s.recordWave(0, rollout.WaveHealthy, 0.97)
				s.recordWave(1, rollout.WaveSkipped, 0)
				s.complete()
			},
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutCompleted,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WaveHealthy, SuccessRate: 0.97, TargetCount: 1},
					{Percent: 100, Status: rollout.WaveSkipped},
				},
				Current: noWave,
				Outcome: OutcomeCompleted,
			},
		},
		{
			name: "a rolled-back rollout reports the wave that ended it",
			apply: func(s *rolloutState) {
				s.startWave(0, ResolvedWave{WaveID: "ro-1-w0-10", DeviceIDs: []string{"dev-1"}})
				s.recordWave(0, rollout.WaveHealthy, 0.99)
				s.startWave(1, ResolvedWave{WaveID: "ro-1-w1-100", DeviceIDs: []string{"dev-3"}})
				s.recordWave(1, rollout.WaveUnhealthy, 0.4)
				health := WaveHealth{
					Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: 0.4,
					SampleSize: 120, WindowStart: time.Unix(1000, 0), WindowEnd: time.Unix(1300, 0),
				}
				s.rollback(1, OutcomeUnhealthyWave, &health)
			},
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutRolledBack,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WaveHealthy, SuccessRate: 0.99, TargetCount: 1},
					{Percent: 100, Status: rollout.WaveUnhealthy, SuccessRate: 0.4, TargetCount: 1},
				},
				Current: noWave,
				Outcome: OutcomeUnhealthyWave,
				EndedBy: "ro-1-w1-100",
				Decision: &WaveHealth{
					Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: 0.4,
					SampleSize: 120, WindowStart: time.Unix(1000, 0), WindowEnd: time.Unix(1300, 0),
				},
			},
		},
		{
			name: "a rollout that could not start reports the refusal",
			apply: func(s *rolloutState) {
				s.fail(OutcomeFirmwareMismatch)
			},
			want: RolloutView{
				RolloutID: "ro-1", Status: rollout.RolloutFailed,
				FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
				Waves: []WaveView{
					{Percent: 10, Status: rollout.WavePending},
					{Percent: 100, Status: rollout.WavePending},
				},
				Current: noWave,
				Outcome: OutcomeFirmwareMismatch,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := newRolloutState(rolloutTestInput())
			tc.apply(&state)
			if diff := cmp.Diff(tc.want, state.view()); diff != "" {
				t.Errorf("view() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRolloutStateTransitions(t *testing.T) {
	t.Parallel()

	t.Run("an approval before the gate is banked", func(t *testing.T) {
		t.Parallel()

		state := newRolloutState(rolloutTestInput())
		state.applyApproval()
		if !state.ApprovalOutstanding {
			t.Error("applyApproval() left no approval outstanding")
		}
		if state.Status != rollout.RolloutRunning {
			t.Errorf("status = %q, want the rollout still running", state.Status)
		}

		// A repeated approval while one is already outstanding changes nothing: one approval
		// authorizes one wave, so the second adds no credit.
		state.applyApproval()
		state.consumeApproval()
		if state.ApprovalOutstanding {
			t.Error("consumeApproval() left an approval outstanding, want one credit spent per wave")
		}
	})

	t.Run("an approval after a terminal outcome changes nothing", func(t *testing.T) {
		t.Parallel()

		for _, conclude := range []func(*rolloutState){
			func(s *rolloutState) { s.complete() },
			func(s *rolloutState) { s.rollback(0, OutcomeUnhealthyWave, nil) },
			func(s *rolloutState) { s.fail(OutcomeFirmwareUnknown) },
		} {
			state := newRolloutState(rolloutTestInput())
			state.startWave(0, ResolvedWave{WaveID: "ro-1-w0-10"})
			conclude(&state)
			before := state.view()

			state.applyApproval()
			if state.ApprovalOutstanding {
				t.Error("an approval after a terminal outcome is outstanding, want it ignored")
			}
			if diff := cmp.Diff(before, state.view()); diff != "" {
				t.Errorf("state changed after a terminal approval (-before +after):\n%s", diff)
			}
		}
	})

	t.Run("a terminal status is reported as terminal", func(t *testing.T) {
		t.Parallel()

		state := newRolloutState(rolloutTestInput())
		if state.terminal() {
			t.Error("a fresh rollout is terminal, want it running")
		}
		state.complete()
		if !state.terminal() {
			t.Error("a completed rollout is not terminal")
		}
	})
}
