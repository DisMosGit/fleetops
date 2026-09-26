package agent

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// fakeFetcher is a hand-written FirmwareFetcher double with scripted outcomes.
type fakeFetcher struct {
	mu       sync.Mutex
	calls    []string // device ids fetched for
	path     string
	err      error
	progress []int32 // percents pushed through onProgress before returning
}

// Fetch records the call, replays the scripted progress, and returns the scripted outcome.
func (f *fakeFetcher) Fetch(
	_ context.Context,
	deviceID, _, _ string,
	onProgress func(int32),
) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deviceID)
	for _, percent := range f.progress {
		if onProgress != nil {
			onProgress(percent)
		}
	}
	return f.path, f.err
}

// fakeApplier is a hand-written FirmwareApply double with a scripted outcome and an optional
// hook that runs when it is called.
type fakeApplier struct {
	mu      sync.Mutex
	calls   []string // device:version pairs
	err     error
	onApply func()
}

// Apply records the call, runs the hook, and returns the scripted outcome.
func (f *fakeApplier) Apply(_ context.Context, deviceID, version string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deviceID+":"+version)
	if f.onApply != nil {
		f.onApply()
	}
	return f.err
}

// fakeAdopter is a hand-written FirmwareAdopter double recording adoptions.
type fakeAdopter struct {
	mu        sync.Mutex
	adoptions map[string]string
	err       error
}

// SetFirmware records the adoption and returns the scripted outcome.
func (f *fakeAdopter) SetFirmware(deviceID, version string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.adoptions == nil {
		f.adoptions = make(map[string]string)
	}
	f.adoptions[deviceID] = version
	return nil
}

// updateCommand returns one StartUpdate command as the control plane dispatches it.
func updateCommand() *agentv1.Command {
	return &agentv1.Command{
		CommandId: "cmd-1",
		DeviceId:  "dev-1",
		Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
			FirmwareId: "fw-1", Version: "2.0.0", Checksum: "sum",
		}},
	}
}

// phases returns the update phases one fake status reporter received, with their percentages.
func phases(status *fakeStatusReporter) []string {
	var out []string
	for _, req := range status.recorded() {
		out = append(out, req.GetPhase().String())
	}
	return out
}

func TestUpdateHandlerSuccess(t *testing.T) {
	t.Parallel()

	status := &fakeStatusReporter{}
	results := &fakeResults{}
	fetcher := &fakeFetcher{path: "/tmp/fw-1.bin", progress: []int32{35, 80}}
	applier := &fakeApplier{}
	adopt := &fakeAdopter{}
	handler := NewUpdateHandler(fetcher, applier, adopt, NewUpdateReporter(status, results), slog.New(slog.DiscardHandler))

	if err := handler.Handle(context.Background(), updateCommand()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	wantPhases := []string{
		"UPDATE_PHASE_DOWNLOADING", "UPDATE_PHASE_DOWNLOADING", "UPDATE_PHASE_DOWNLOADING",
		"UPDATE_PHASE_APPLYING", "UPDATE_PHASE_COMPLETED",
	}
	gotPhases := phases(status)
	if len(gotPhases) != len(wantPhases) {
		t.Fatalf("reported phases = %v, want %v", gotPhases, wantPhases)
	}
	for i := range wantPhases {
		if gotPhases[i] != wantPhases[i] {
			t.Fatalf("reported phases = %v, want %v", gotPhases, wantPhases)
		}
	}
	if applier.calls[0] != "dev-1:2.0.0" {
		t.Errorf("apply calls = %v, want the commanded version on the device", applier.calls)
	}
	if adopt.adoptions["dev-1"] != "2.0.0" {
		t.Errorf("adoptions = %v, want dev-1 on 2.0.0", adopt.adoptions)
	}

	reports := results.recorded()
	if len(reports) != 1 {
		t.Fatalf("terminal reports = %d, want exactly one", len(reports))
	}
	if reports[0].GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED ||
		reports[0].GetCommandId() != "cmd-1" ||
		reports[0].GetIdempotencyKey() != reportKey("cmd-1") {
		t.Errorf("terminal report = %+v, want the succeeded result under its derived key", reports[0])
	}
}

func TestUpdateHandlerFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		fetcher    *fakeFetcher
		applier    *fakeApplier
		adopt      *fakeAdopter
		wantDetail string
	}{
		{
			name:       "checksum mismatch fails the download",
			fetcher:    &fakeFetcher{err: ErrChecksumMismatch},
			applier:    &fakeApplier{},
			adopt:      &fakeAdopter{},
			wantDetail: "checksum mismatch",
		},
		{
			name:       "truncated transfer fails the download",
			fetcher:    &fakeFetcher{err: ErrTruncatedTransfer},
			applier:    &fakeApplier{},
			adopt:      &fakeAdopter{},
			wantDetail: "truncated transfer",
		},
		{
			name:       "any other download failure reports the generic reason",
			fetcher:    &fakeFetcher{err: ErrTransfer},
			applier:    &fakeApplier{},
			adopt:      &fakeAdopter{},
			wantDetail: "download failed",
		},
		{
			name:       "apply failure keeps the previous version",
			fetcher:    &fakeFetcher{path: "/tmp/fw-1.bin", progress: []int32{100}},
			applier:    &fakeApplier{err: ErrApplyFailed},
			adopt:      &fakeAdopter{},
			wantDetail: "firmware apply failed",
		},
		{
			name:       "adoption failure is an update failure",
			fetcher:    &fakeFetcher{path: "/tmp/fw-1.bin"},
			applier:    &fakeApplier{},
			adopt:      &fakeAdopter{err: errors.New("unknown device")},
			wantDetail: "device did not adopt the firmware",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status := &fakeStatusReporter{}
			results := &fakeResults{}
			handler := NewUpdateHandler(tc.fetcher, tc.applier, tc.adopt,
				NewUpdateReporter(status, results), slog.New(slog.DiscardHandler))

			if err := handler.Handle(context.Background(), updateCommand()); err != nil {
				t.Fatalf("Handle() error = %v (a failed update is an outcome, not a fault)", err)
			}

			got := status.recorded()
			last := got[len(got)-1]
			if last.GetPhase() != agentv1.UpdatePhase_UPDATE_PHASE_FAILED ||
				last.GetDetail() != tc.wantDetail {
				t.Errorf("last status = %+v, want the failed phase with detail %q", last, tc.wantDetail)
			}
			reports := results.recorded()
			if len(reports) != 1 || reports[0].GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_FAILED {
				t.Fatalf("terminal reports = %+v, want exactly one failed result", reports)
			}
			if reports[0].GetDetail() != tc.wantDetail {
				t.Errorf("terminal detail = %q, want %q", reports[0].GetDetail(), tc.wantDetail)
			}
			if tc.name == "apply failure keeps the previous version" && len(tc.adopt.adoptions) != 0 {
				t.Errorf("adoptions = %v, want none after a failed apply", tc.adopt.adoptions)
			}
		})
	}
}

func TestUpdateHandlerShutdownReportsNothingMore(t *testing.T) {
	t.Parallel()

	// Shutdown interrupts the apply step: with the context gone there is nothing left to
	// report to, so the handler stops instead of manufacturing a failure report.
	status := &fakeStatusReporter{}
	results := &fakeResults{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	applier := &fakeApplier{err: context.Canceled, onApply: cancel}
	handler := NewUpdateHandler(&fakeFetcher{path: "/tmp/fw-1.bin"}, applier, &fakeAdopter{},
		NewUpdateReporter(status, results), slog.New(slog.DiscardHandler))

	if err := handler.Handle(ctx, updateCommand()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle() error = %v, want the interruption", err)
	}
	if reports := results.recorded(); len(reports) != 0 {
		t.Errorf("terminal reports = %d, want none once the context is gone", len(reports))
	}
	want := []string{"UPDATE_PHASE_DOWNLOADING", "UPDATE_PHASE_APPLYING"}
	if got := phases(status); len(got) != len(want) {
		t.Errorf("reported phases = %v, want %v", got, want)
	}
}

func TestUpdateHandlerNonUpdateCommands(t *testing.T) {
	t.Parallel()

	status := &fakeStatusReporter{}
	results := &fakeResults{}
	handler := NewUpdateHandler(&fakeFetcher{}, &fakeApplier{}, &fakeAdopter{},
		NewUpdateReporter(status, results), slog.New(slog.DiscardHandler))

	abort := &agentv1.Command{
		CommandId: "cmd-2",
		DeviceId:  "dev-1",
		Kind:      &agentv1.Command_AbortUpdate{AbortUpdate: &agentv1.AbortUpdate{Reason: "operator"}},
	}
	if err := handler.Handle(context.Background(), abort); err != nil {
		t.Fatalf("Handle(abort) error = %v, want the log-and-ignore acknowledgment", err)
	}
	if err := handler.Handle(context.Background(), &agentv1.Command{CommandId: "cmd-3", DeviceId: "dev-1"}); err != nil {
		t.Fatalf("Handle(kindless) error = %v", err)
	}
	if len(status.recorded()) != 0 || len(results.recorded()) != 0 {
		t.Errorf("reports = %d statuses, %d results, want none for non-update commands",
			len(status.recorded()), len(results.recorded()))
	}
}

func TestDetailClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		want string
	}{
		{err: ErrChecksumMismatch, want: "checksum mismatch"},
		{err: ErrTruncatedTransfer, want: "truncated transfer"},
		{err: ErrTransfer, want: "download failed"},
		{err: errors.New("other"), want: "download failed"},
	} {
		if got := downloadDetail(tc.err); got != tc.want {
			t.Errorf("downloadDetail(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{err: ErrApplyFailed, want: "firmware apply failed"},
		{err: context.Canceled, want: "firmware apply failed"},
	} {
		if got := applyDetail(tc.err); got != tc.want {
			t.Errorf("applyDetail(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// reportingHandler reports update status back over the stream while handling a command — the
// exchange the receive loop must keep serving.
type reportingHandler struct {
	client *Client
	done   chan struct{}
	once   sync.Once
}

// Handle reports one update status through the client and marks the handler done.
func (h *reportingHandler) Handle(ctx context.Context, _ *agentv1.Command) error {
	_, err := h.client.ReportUpdateStatus(ctx, &agentv1.UpdateStatusRequest{
		DeviceId: "device-0", FirmwareId: "fw-1",
		Phase: agentv1.UpdatePhase_UPDATE_PHASE_DOWNLOADING,
	})
	h.once.Do(func() { close(h.done) })
	return err
}

func TestCommandHandlerCanReportWhileHandling(t *testing.T) {
	t.Parallel()

	// The update handler talks back to the control plane over the very stream that delivers
	// its commands. Handling one inline on the receive loop would deadlock the exchange;
	// handling it off the loop lets the loop deliver the acknowledgment.
	handler := &reportingHandler{done: make(chan struct{})}
	c := newTestClient(t, &fakeService{}, Options{Handler: handler})
	handler.client = c
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newFakeStream(ctx)
	stream.auto = func(env *agentv1.AgentEnvelope) *agentv1.ControlEnvelope {
		if env.GetUpdateStatus() == nil {
			return nil
		}
		return &agentv1.ControlEnvelope{
			CorrelationId: env.CorrelationId,
			Payload: &agentv1.ControlEnvelope_UpdateStatus{UpdateStatus: &agentv1.UpdateStatusResponse{
				Accepted: true,
			}},
		}
	}
	ready := make(chan struct{})
	close(ready)
	writeDone := make(chan error, 1)
	go func() { writeDone <- c.writeLoop(ctx, stream, ready) }()
	readDone := make(chan error, 1)
	go func() { readDone <- c.readLoop(ctx, stream) }()

	stream.recvs <- recvResult{env: &agentv1.ControlEnvelope{
		Payload: &agentv1.ControlEnvelope_Command{Command: &agentv1.Command{
			CommandId: "cmd-1", DeviceId: "device-0",
			Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
				FirmwareId: "fw-1", Version: "2.0.0", Checksum: "sum",
			}},
		}},
	}}

	select {
	case <-handler.done:
	case <-time.After(5 * time.Second):
		t.Fatal("command handler never finished: the stream exchange deadlocked")
	}
	cancel()
	<-readDone
	<-writeDone
}

func TestFleetSetFirmware(t *testing.T) {
	t.Parallel()

	ids, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	fleet, err := NewFleet(1, FleetOptions{
		IDs:    ids,
		Source: NewSimulation(rand.NewSource(1)),
		Period: time.Second,
	})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	if err := fleet.SetFirmware("device-unknown", "2.0.0"); err == nil {
		t.Error("SetFirmware(unknown) error = nil, want an error")
	}
	if err := fleet.SetFirmware("device-0", "2.0.0"); err != nil {
		t.Fatalf("SetFirmware: %v", err)
	}

	// The adopted version is what every later heartbeat reports.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time, 1)
	out := make(chan *agentv1.Heartbeat, 1)
	done := make(chan error, 1)
	go func() { done <- fleet.devices[0].Run(ctx, ticks, out) }()
	ticks <- time.Unix(1000, 0)
	hb := <-out
	cancel()
	<-done

	if hb.GetCurrentFw() != "2.0.0" {
		t.Errorf("heartbeat current_fw = %q, want the adopted version", hb.GetCurrentFw())
	}
	if got := fleet.Identities()[0].Firmware; got != "2.0.0" {
		t.Errorf("identity firmware = %q, want the adopted version", got)
	}
}
