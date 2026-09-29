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
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// fakeCommander is a hand-written DeviceCommander double recording the commands it accepted and
// failing every delivery when err is set.
type fakeCommander struct {
	mu   sync.Mutex
	sent []CommandIssuedSignal
	err  error
}

// SignalCommandIssued records one delivered command.
func (c *fakeCommander) SignalCommandIssued(_ context.Context, cmd CommandIssuedSignal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.sent = append(c.sent, cmd)
	return nil
}

// recorded returns the commands the seam accepted, in delivery order.
func (c *fakeCommander) recorded() []CommandIssuedSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CommandIssuedSignal(nil), c.sent...)
}

// fakeDeviceStates is a hand-written DeviceStateReader double answering with scripted states: one
// answer per observation, the last repeating once the script is exhausted, so a device can be
// made to conclude after a few observations.
type fakeDeviceStates struct {
	mu sync.Mutex
	// states are the answers to return, in order.
	states []State
	// errs are the failures to return, in order, and err is returned by every observation once
	// the scripted ones are exhausted.
	errs []error
	err  error
	// calls counts the observations the activity made.
	calls int
}

// scriptStates queues the states the reader answers with, one per observation.
func (f *fakeDeviceStates) scriptStates(states ...State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = append(f.states, states...)
}

// scriptErrors queues the failures the reader answers with, one per observation.
func (f *fakeDeviceStates) scriptErrors(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, errs...)
}

// State answers the next scripted answer.
func (f *fakeDeviceStates) State(_ context.Context, _ string) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return State{}, err
	}
	if len(f.states) == 0 {
		if f.err != nil {
			return State{}, f.err
		}
		return State{}, nil
	}
	state := f.states[0]
	if len(f.states) > 1 {
		f.states = f.states[1:]
	}
	return state, nil
}

// observed reports how many observations the activity made.
func (f *fakeDeviceStates) observed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// updateRequest is the request one device's update activity is asked to run.
func updateRequest(deadline time.Time) UpdateDeviceRequest {
	return UpdateDeviceRequest{
		RolloutID: "ro-1",
		WaveID:    "ro-1-w0-1",
		DeviceID:  "dev-1",
		Firmware:  Firmware{ID: "fw-1", Version: "2.0.0", Checksum: "sha256:0f1e2d"},
		Deadline:  deadline,
	}
}

// concludedState is a device state whose last concluded command is the wave's command for dev-1,
// with the given outcome and detail.
func concludedState(outcome CommandOutcome, detail string) State {
	return State{
		DeviceID: "dev-1",
		LastCommand: &ConcludedCommand{
			Command: CommandIssuedSignal{
				CommandID: CommandID("ro-1-w0-1", "dev-1"), DeviceID: "dev-1",
				Kind: CommandKindUpdate, FirmwareID: "fw-1", Version: "2.0.0",
			},
			Outcome: outcome,
			Detail:  detail,
		},
	}
}

// pendingState is a device state whose command is still outstanding and whose last concluded
// command — if any — is some other command.
func pendingState() State {
	return State{
		DeviceID: "dev-1",
		Pending: &PendingCommand{Command: CommandIssuedSignal{
			CommandID: CommandID("ro-1-w0-1", "dev-1"), DeviceID: "dev-1", Kind: CommandKindUpdate,
		}},
	}
}

// supersededState is a device state whose last concluded command is a newer command than the
// wave's: the wave's own command never concluded.
func supersededState() State {
	return State{
		DeviceID: "dev-1",
		LastCommand: &ConcludedCommand{
			Command: CommandIssuedSignal{
				CommandID: "some-later-command", DeviceID: "dev-1", Kind: CommandKindAbort,
			},
			Outcome: OutcomeSucceeded,
		},
	}
}

func TestUpdateDeviceActivity(t *testing.T) {
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
		// script prepares the reader.
		script func(*fakeDeviceStates)
		// deliverErr is the command seam's failure, if any.
		deliverErr error
		want       DeviceUpdate
		wantErr    string
		// wantObservations is how many state observations the wait made; -1 skips the check.
		wantObservations int
	}{
		{
			name:     "a device that applies the update reports success",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(concludedState(OutcomeSucceeded, ""))
			},
			want:             DeviceUpdate{DeviceID: "dev-1", Outcome: UpdateSucceeded},
			wantObservations: 1,
		},
		{
			name:     "a device that fails reports its own detail",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(concludedState(OutcomeFailed, "flash error"))
			},
			want: DeviceUpdate{
				DeviceID: "dev-1", Outcome: UpdateFailed, Detail: "flash error",
			},
			wantObservations: 1,
		},
		{
			name:     "a deadline that has already passed still reads the recorded result",
			deadline: past(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(concludedState(OutcomeSucceeded, ""))
			},
			want:             DeviceUpdate{DeviceID: "dev-1", Outcome: UpdateSucceeded},
			wantObservations: 1,
		},
		{
			name:     "a deadline that has already passed without a result is unreported",
			deadline: past(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(pendingState())
			},
			want:             DeviceUpdate{DeviceID: "dev-1", Outcome: UpdateUnreported},
			wantObservations: 1,
		},
		{
			name:     "a device that concludes after a few observations is found",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(pendingState(), pendingState(), pendingState(),
					concludedState(OutcomeSucceeded, ""))
			},
			want:             DeviceUpdate{DeviceID: "dev-1", Outcome: UpdateSucceeded},
			wantObservations: 4,
		},
		{
			name:     "a superseded command is unreported",
			deadline: soon(),
			script: func(f *fakeDeviceStates) {
				f.scriptStates(supersededState())
			},
			want: DeviceUpdate{DeviceID: "dev-1", Outcome: UpdateUnreported},
			// How many observations fit in the deadline is timing, not behaviour: what
			// matters is that the wait kept observing and then stopped.
			wantObservations: -1,
		},
		{
			name:     "an observation failure keeps the wait going",
			deadline: future(),
			script: func(f *fakeDeviceStates) {
				f.scriptErrors(
					errors.New("frontend is unavailable"),
					fmt.Errorf("device dev-1 state: %w", ErrDeviceNotFound),
				)
				f.scriptStates(pendingState(), concludedState(OutcomeFailed, "flash error"))
			},
			want: DeviceUpdate{
				DeviceID: "dev-1", Outcome: UpdateFailed, Detail: "flash error",
			},
			wantObservations: 4,
		},
		{
			name:       "a command that cannot be delivered is an error rather than an outcome",
			deadline:   future(),
			script:     func(*fakeDeviceStates) {},
			deliverErr: errors.New("device workflow is unreachable"),
			wantErr:    "device workflow is unreachable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := &fakeDeviceStates{}
			tc.script(reader)
			commander := &fakeCommander{err: tc.deliverErr}
			// The poll interval is shrunk so the cases that observe more than once do not
			// spend the production interval sleeping.
			updater := deviceUpdater{commander: commander, states: reader, poll: time.Millisecond}

			// A context bound is a second, independent stop: a wait that ignored its
			// deadline would hang the suite instead of failing it.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			req := updateRequest(tc.deadline)
			got, err := updater.update(ctx, req)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("update() error = nil, want %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("update() error = %v, want it to name %q", err, tc.wantErr)
				}
				if got != (DeviceUpdate{}) {
					t.Errorf("update() outcome = %+v, want none alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("update() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("outcome mismatch (-want +got):\n%s", diff)
			}
			if tc.wantObservations >= 0 && reader.observed() != tc.wantObservations {
				t.Errorf("state observations = %d, want %d", reader.observed(), tc.wantObservations)
			}
		})
	}
}

// TestUpdateDeviceActivityCommandsTheDevice pins what the activity delivers: one command per
// attempt, under the wave's derived command id, naming the firmware the wave resolved.
func TestUpdateDeviceActivityCommandsTheDevice(t *testing.T) {
	t.Parallel()

	reader := &fakeDeviceStates{}
	reader.scriptStates(concludedState(OutcomeSucceeded, ""))
	commander := &fakeCommander{}
	updater := deviceUpdater{commander: commander, states: reader, poll: time.Millisecond}

	req := updateRequest(time.Now().Add(time.Minute))
	if _, err := updater.update(context.Background(), req); err != nil {
		t.Fatalf("update() error = %v", err)
	}

	want := []CommandIssuedSignal{{
		CommandID:  "ro-1-w0-1-dev-1",
		DeviceID:   "dev-1",
		Kind:       CommandKindUpdate,
		FirmwareID: "fw-1",
		Version:    "2.0.0",
		Checksum:   "sha256:0f1e2d",
	}}
	if diff := cmp.Diff(want, commander.recorded()); diff != "" {
		t.Errorf("delivered commands mismatch (-want +got):\n%s", diff)
	}
}

// TestUpdateDeviceActivityRetriesRepeatTheCommandID pins the idempotency the device entity's dedup
// rests on: every attempt of one device's update repeats the same command id, so a retry after an
// accepted command is a no-op for the device.
func TestUpdateDeviceActivityRetriesRepeatTheCommandID(t *testing.T) {
	t.Parallel()

	commander := &fakeCommander{}
	reader := &fakeDeviceStates{}
	reader.scriptStates(pendingState(), pendingState())
	reader.err = context.DeadlineExceeded
	updater := deviceUpdater{commander: commander, states: reader, poll: time.Millisecond}

	// Two attempts of one device's update, as the caller's retry policy would run them.
	req := updateRequest(time.Now().Add(-time.Second))
	for range 2 {
		if _, err := updater.update(context.Background(), req); err != nil {
			t.Fatalf("update() error = %v", err)
		}
	}

	delivered := commander.recorded()
	if len(delivered) != 2 {
		t.Fatalf("delivered commands = %d, want 2", len(delivered))
	}
	if delivered[0].CommandID != delivered[1].CommandID {
		t.Errorf("retry delivered command id %q, want the first attempt's %q",
			delivered[1].CommandID, delivered[0].CommandID)
	}
}

// TestUpdateDeviceActivityStopsAtTheDeadline pins the bound: a device that never reports does not
// hold the activity open past the deadline it was given.
func TestUpdateDeviceActivityStopsAtTheDeadline(t *testing.T) {
	t.Parallel()

	reader := &fakeDeviceStates{}
	reader.scriptStates(pendingState())
	reader.err = fmt.Errorf("device dev-1 state: %w", ErrDeviceNotFound)
	updater := deviceUpdater{
		commander: &fakeCommander{}, states: reader, poll: 50 * time.Millisecond,
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	start := time.Now()
	got, err := updater.update(context.Background(), updateRequest(deadline))
	if err != nil {
		t.Fatalf("update() error = %v", err)
	}
	if got.Outcome != UpdateUnreported {
		t.Errorf("outcome = %q, want %q", got.Outcome, UpdateUnreported)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the wait took %s, want it to stop at the deadline", elapsed)
	}
}

// TestUpdateDeviceActivityStopsWhenItsContextIsDone pins the wait's second stop: the activity is
// owned by a workflow attempt, so a cancelled attempt must end the wait rather than let it run to
// the result deadline, and the activity reports that the attempt could not finish rather than an
// outcome no device ever reported.
func TestUpdateDeviceActivityStopsWhenItsContextIsDone(t *testing.T) {
	t.Parallel()

	reader := &fakeDeviceStates{}
	reader.scriptStates(pendingState())
	ctx, cancel := context.WithCancel(context.Background())
	update := NewUpdateDeviceActivity(&fakeCommander{}, reader, WithUpdatePollInterval(time.Minute))
	// The first observation finds nothing, so the wait reaches its sleep with the context
	// already cancelled: the stop is what ends the activity.
	cancel()

	got, err := update(ctx, updateRequest(time.Now().Add(time.Hour)))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("update() error = %v, want %v", err, context.Canceled)
	}
	if got != (DeviceUpdate{}) {
		t.Errorf("update() outcome = %+v, want none alongside a cancelled attempt", got)
	}
	if reader.observed() != 1 {
		t.Errorf("state observations = %d, want the one the cancelled attempt made",
			reader.observed())
	}
}

// TestUpdateDeviceActivityWarnsOnAnUnreadableDevice pins what an unreadable device costs: nothing.
// The observation is logged at the activity boundary and the wait continues to the deadline,
// because a device nobody could read has not said what happened to its update — and reading the
// logger is only available to a real activity context, so this runs the activity in Temporal's own
// activity environment rather than calling it directly.
func TestUpdateDeviceActivityWarnsOnAnUnreadableDevice(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	reader := &fakeDeviceStates{}
	reader.scriptErrors(
		errors.New("mongo is unavailable"),
		fmt.Errorf("device dev-1 state: %w", ErrDeviceNotFound),
	)
	env.RegisterActivityWithOptions(
		NewUpdateDeviceActivity(&fakeCommander{}, reader, WithUpdatePollInterval(5*time.Millisecond)),
		activity.RegisterOptions{Name: UpdateDeviceActivityName},
	)

	value, err := env.ExecuteActivity(UpdateDeviceActivityName,
		updateRequest(time.Now().Add(50*time.Millisecond)))
	if err != nil {
		t.Fatalf("execute activity: %v", err)
	}
	var got DeviceUpdate
	if err := value.Get(&got); err != nil {
		t.Fatalf("decode activity result: %v", err)
	}
	if got.Outcome != UpdateUnreported {
		t.Errorf("outcome = %q, want %q for a device that never reported", got.Outcome, UpdateUnreported)
	}
	// Both scripted failures were observed inside the activity and neither ended the wait: had
	// one, the outcome would carry that reading rather than the deadline's.
	if reader.observed() < 2 {
		t.Errorf("state observations = %d, want the wait to keep observing after a failure",
			reader.observed())
	}
}

// TestSleepStopsAtItsContextAndNeverWaitsForNoTime pins the wait's two boundaries: a duration that
// has already elapsed returns at once, and a longer one ends when the context does.
func TestSleepStopsAtItsContextAndNeverWaitsForNoTime(t *testing.T) {
	t.Parallel()

	if err := sleep(context.Background(), 0); err != nil {
		t.Errorf("sleep(0) error = %v, want no error for a wait nothing needs", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("sleep(cancelled ctx) error = %v, want %v", err, context.Canceled)
	}
}
