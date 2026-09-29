package rolloutapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
	"github.com/DisMosGit/fleetops/internal/wavehealth"
)

// fakeStarter is a hand-written Starter double recording the requests it accepted and reporting
// the failures a test scripts.
type fakeStarter struct {
	mu       sync.Mutex
	requests []temporal.RolloutRequest
	err      error
}

func (s *fakeStarter) Start(_ context.Context, req temporal.RolloutRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.requests = append(s.requests, req)
	return nil
}

func (s *fakeStarter) recorded() []temporal.RolloutRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]temporal.RolloutRequest(nil), s.requests...)
}

// fakeStates is a hand-written States double answering with a scripted view or failure.
type fakeStates struct {
	mu   sync.Mutex
	view temporal.RolloutView
	err  error
	// reads counts the state reads, so a test can prove a command consulted the state first.
	reads int
}

func (s *fakeStates) State(_ context.Context, _ string) (temporal.RolloutView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.err != nil {
		return temporal.RolloutView{}, s.err
	}
	return s.view, nil
}

func (s *fakeStates) observed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// fakeSignals is a hand-written Signals double recording the commands delivered and reporting
// scripted failures.
type fakeSignals struct {
	mu       sync.Mutex
	approved []string
	paused   []string
	resumed  []string
	err      error
}

func (s *fakeSignals) Approve(_ context.Context, rolloutID string) error {
	return s.record(&s.approved, rolloutID)
}

func (s *fakeSignals) Pause(_ context.Context, rolloutID string) error {
	return s.record(&s.paused, rolloutID)
}

func (s *fakeSignals) Resume(_ context.Context, rolloutID string) error {
	return s.record(&s.resumed, rolloutID)
}

func (s *fakeSignals) record(into *[]string, rolloutID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	*into = append(*into, rolloutID)
	return nil
}

func (s *fakeSignals) delivered() (approved, paused, resumed []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.approved...),
		append([]string(nil), s.paused...),
		append([]string(nil), s.resumed...)
}

// testLogger is a logger that discards its output: the API logs every rejection at the boundary,
// and that noise would drown the test output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestHandler returns the handler under test over the three fakes and the fakes themselves.
func newTestHandler() (http.Handler, *fakeStarter, *fakeStates, *fakeSignals) {
	starter, states, signals := &fakeStarter{}, &fakeStates{}, &fakeSignals{}
	return NewHandler(starter, states, signals, testLogger()), starter, states, signals
}

// runningView is the state a running rollout reports, with the fields the API answers verbatim.
func runningView() temporal.RolloutView {
	return temporal.RolloutView{
		RolloutID:  "ro-1",
		Status:     rollout.RolloutRunning,
		FirmwareID: "fw-1",
		Region:     "eu-west",
		Model:      "oak-s3",
		Waves: []temporal.WaveView{
			{Percent: 25, Status: rollout.WaveHealthy, SuccessRate: 0.99, TargetCount: 4},
			{Percent: 100, Status: rollout.WaveEvaluating, TargetCount: 12,
				FailedCount: 1, UnreportedCount: 2},
		},
		Current: 1,
	}
}

// do runs one request against the handler and returns the recorded response.
func do(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decode decodes a response body into out, failing the test when it is not the expected shape.
func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestHandlerStart(t *testing.T) {
	t.Parallel()

	t.Run("a start request begins a rollout", func(t *testing.T) {
		t.Parallel()

		handler, starter, _, _ := newTestHandler()
		rec := do(t, handler, http.MethodPost, "/api/rollouts",
			`{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
		}
		body := decode[startResponse](t, rec)
		want := startResponse{
			RolloutID:  "ro-1",
			WorkflowID: temporal.RolloutWorkflowID("ro-1"),
			Status:     string(rollout.RolloutRunning),
		}
		if diff := cmp.Diff(want, body); diff != "" {
			t.Errorf("response mismatch (-want +got):\n%s", diff)
		}
		wantRequests := []temporal.RolloutRequest{{
			RolloutID: "ro-1", FirmwareID: "fw-1", Region: "eu-west", Model: "oak-s3",
		}}
		if diff := cmp.Diff(wantRequests, starter.recorded()); diff != "" {
			t.Errorf("started rollouts mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("surrounding whitespace in the body is trimmed", func(t *testing.T) {
		t.Parallel()

		handler, starter, _, _ := newTestHandler()
		rec := do(t, handler, http.MethodPost, "/api/rollouts",
			`{"rollout_id":" ro-1 ","firmware_id":" fw-1 ","region":" eu-west ","model":" oak-s3 "}`)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
		}
		got := starter.recorded()
		if len(got) != 1 || got[0].RolloutID != "ro-1" || got[0].Model != "oak-s3" {
			t.Errorf("started rollouts = %+v, want trimmed values", got)
		}
	})

	t.Run("a repeated start is refused", func(t *testing.T) {
		t.Parallel()

		handler, starter, _, _ := newTestHandler()
		starter.err = fmt.Errorf("start rollout ro-1: %w", temporal.ErrRolloutExists)

		rec := do(t, handler, http.MethodPost, "/api/rollouts",
			`{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`)

		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusConflict, rec.Body)
		}
		body := decode[errorResponse](t, rec)
		if !strings.Contains(body.Error, "ro-1") {
			t.Errorf("error = %q, want it to name the rollout", body.Error)
		}
	})

	tests := []struct {
		name string
		body string
	}{
		{name: "a body that is not valid JSON", body: `{"rollout_id":`},
		{name: "an empty body", body: " "},
		{name: "a missing rollout id", body: `{"firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`},
		{name: "a blank rollout id", body: `{"rollout_id":"  ","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`},
		{name: "a missing firmware id", body: `{"rollout_id":"ro-1","region":"eu-west","model":"oak-s3"}`},
		{name: "an empty region", body: `{"rollout_id":"ro-1","firmware_id":"fw-1","region":"","model":"oak-s3"}`},
		{name: "an empty model", body: `{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":""}`},
		{name: "an unknown field", body: `{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3","waves":[]}`},
	}
	for _, tc := range tests {
		t.Run("an incomplete request is rejected: "+tc.name, func(t *testing.T) {
			t.Parallel()

			handler, starter, _, _ := newTestHandler()
			rec := do(t, handler, http.MethodPost, "/api/rollouts", tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
			if got := starter.recorded(); len(got) != 0 {
				t.Errorf("started rollouts = %+v, want none for a rejected request", got)
			}
			body := decode[errorResponse](t, rec)
			if body.Error == "" {
				t.Error("rejection body carries no reason")
			}
		})
	}

	t.Run("a rollout that cannot start is still accepted", func(t *testing.T) {
		t.Parallel()

		// The starter accepted it: the firmware's fate is the rollout's own recorded outcome,
		// not a start refusal, so the endpoint answers 202.
		handler, _, _, _ := newTestHandler()
		rec := do(t, handler, http.MethodPost, "/api/rollouts",
			`{"rollout_id":"ro-1","firmware_id":"fw-unknown","region":"eu-west","model":"oak-s3"}`)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
		}
	})

	t.Run("a backend failure is answered without internal detail", func(t *testing.T) {
		t.Parallel()

		handler, starter, _, _ := newTestHandler()
		starter.err = errors.New("temporal frontend is unavailable at 10.0.0.5:7233")

		rec := do(t, handler, http.MethodPost, "/api/rollouts",
			`{"rollout_id":"ro-1","firmware_id":"fw-1","region":"eu-west","model":"oak-s3"}`)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
		if body := rec.Body.String(); strings.Contains(body, "10.0.0.5") ||
			strings.Contains(body, "frontend") {
			t.Errorf("body = %q, want no internal detail", body)
		}
	})
}

func TestHandlerState(t *testing.T) {
	t.Parallel()

	t.Run("a running rollout reports where it stands", func(t *testing.T) {
		t.Parallel()

		handler, _, states, _ := newTestHandler()
		states.view = runningView()

		rec := do(t, handler, http.MethodGet, "/api/rollouts/ro-1", "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusOK, rec.Body)
		}
		// The body is the workflow's view verbatim: the state query is the contract.
		body := decode[temporal.RolloutView](t, rec)
		if diff := cmp.Diff(runningView(), body); diff != "" {
			t.Errorf("view mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a concluded rollout reports why it ended", func(t *testing.T) {
		t.Parallel()

		handler, _, states, _ := newTestHandler()
		states.view = temporal.RolloutView{
			RolloutID: "ro-1",
			Status:    rollout.RolloutRolledBack,
			Outcome:   temporal.OutcomeUnhealthyWave,
			EndedBy:   "ro-1-w1-100",
			Decision: &temporal.WaveHealth{
				Verdict: wavehealth.VerdictUnhealthy, SuccessRatio: 0.4, SampleSize: 120,
			},
			Waves: []temporal.WaveView{{Percent: 100, Status: rollout.WaveUnhealthy, SuccessRate: 0.4}},
		}

		rec := do(t, handler, http.MethodGet, "/api/rollouts/ro-1", "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		body := decode[temporal.RolloutView](t, rec)
		if body.Status != rollout.RolloutRolledBack || body.Outcome != temporal.OutcomeUnhealthyWave {
			t.Errorf("view = %+v, want the terminal outcome", body)
		}
		if body.Decision == nil || body.Decision.SuccessRatio != 0.4 {
			t.Errorf("decision = %+v, want the failing wave's measurement", body.Decision)
		}
	})

	t.Run("an unknown rollout is not found", func(t *testing.T) {
		t.Parallel()

		handler, _, states, _ := newTestHandler()
		states.err = fmt.Errorf("read rollout ro-9 state: %w", temporal.ErrRolloutNotFound)

		rec := do(t, handler, http.MethodGet, "/api/rollouts/ro-9", "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusNotFound, rec.Body)
		}
		body := decode[errorResponse](t, rec)
		if !strings.Contains(body.Error, "ro-9") {
			t.Errorf("error = %q, want it to name the rollout", body.Error)
		}
	})

	t.Run("a failed read is answered without internal detail", func(t *testing.T) {
		t.Parallel()

		handler, _, states, _ := newTestHandler()
		states.err = errors.New("mongo: connection refused by 10.0.0.7:27017")

		rec := do(t, handler, http.MethodGet, "/api/rollouts/ro-1", "")

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
		if body := rec.Body.String(); strings.Contains(body, "10.0.0.7") ||
			strings.Contains(body, "mongo") {
			t.Errorf("body = %q, want no internal detail", body)
		}
	})
}

func TestHandlerCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		signal string
		want   func(approved, paused, resumed []string) bool
	}{
		{
			name: "an approval is delivered", path: "/api/rollouts/ro-1/approve",
			signal: temporal.ApproveNextWaveSignalName,
			want: func(approved, _, _ []string) bool {
				return len(approved) == 1 && approved[0] == "ro-1"
			},
		},
		{
			name: "a pause is delivered", path: "/api/rollouts/ro-1/pause",
			signal: temporal.PauseRolloutSignalName,
			want: func(_, paused, _ []string) bool {
				return len(paused) == 1 && paused[0] == "ro-1"
			},
		},
		{
			name: "a resume is delivered", path: "/api/rollouts/ro-1/resume",
			signal: temporal.ResumeRolloutSignalName,
			want: func(_, _, resumed []string) bool {
				return len(resumed) == 1 && resumed[0] == "ro-1"
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler, _, states, signals := newTestHandler()
			states.view = runningView()

			rec := do(t, handler, http.MethodPost, tc.path, "")

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
			}
			body := decode[commandResponse](t, rec)
			want := commandResponse{RolloutID: "ro-1", Signal: tc.signal}
			if diff := cmp.Diff(want, body); diff != "" {
				t.Errorf("response mismatch (-want +got):\n%s", diff)
			}
			approved, paused, resumed := signals.delivered()
			if !tc.want(approved, paused, resumed) {
				t.Errorf("delivered approve=%v pause=%v resume=%v, want exactly this command",
					approved, paused, resumed)
			}
			// The command read the state first: that is what refuses a concluded rollout
			// instead of accepting a signal that could no longer change anything.
			if states.observed() == 0 {
				t.Error("the command delivered a signal without reading the rollout's state")
			}
		})
	}

	t.Run("commanding a concluded rollout is refused", func(t *testing.T) {
		t.Parallel()

		for _, terminal := range []rollout.RolloutStatus{
			rollout.RolloutCompleted, rollout.RolloutRolledBack, rollout.RolloutFailed,
		} {
			t.Run(string(terminal), func(t *testing.T) {
				t.Parallel()

				handler, _, states, signals := newTestHandler()
				states.view = temporal.RolloutView{RolloutID: "ro-1", Status: terminal}

				rec := do(t, handler, http.MethodPost, "/api/rollouts/ro-1/pause", "")

				if rec.Code != http.StatusConflict {
					t.Fatalf("status = %d, want %d (body %s)",
						rec.Code, http.StatusConflict, rec.Body)
				}
				body := decode[errorResponse](t, rec)
				if !strings.Contains(body.Error, string(terminal)) {
					t.Errorf("error = %q, want it to name the status %q", body.Error, terminal)
				}
				approved, paused, resumed := signals.delivered()
				if len(approved)+len(paused)+len(resumed) != 0 {
					t.Errorf("delivered approve=%v pause=%v resume=%v, want nothing",
						approved, paused, resumed)
				}
			})
		}
	})

	t.Run("commanding an unknown rollout is not found", func(t *testing.T) {
		t.Parallel()

		handler, _, states, signals := newTestHandler()
		states.err = fmt.Errorf("read rollout ro-9 state: %w", temporal.ErrRolloutNotFound)

		rec := do(t, handler, http.MethodPost, "/api/rollouts/ro-9/resume", "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusNotFound, rec.Body)
		}
		approved, paused, resumed := signals.delivered()
		if len(approved)+len(paused)+len(resumed) != 0 {
			t.Errorf("delivered approve=%v pause=%v resume=%v, want nothing",
				approved, paused, resumed)
		}
	})

	t.Run("a signal that cannot be delivered is answered without internal detail", func(t *testing.T) {
		t.Parallel()

		handler, _, states, signals := newTestHandler()
		states.view = runningView()
		signals.err = errors.New("temporal: signal to rollout-ro-1 on 10.0.0.5:7233 failed")

		rec := do(t, handler, http.MethodPost, "/api/rollouts/ro-1/pause", "")

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
		if body := rec.Body.String(); strings.Contains(body, "10.0.0.5") {
			t.Errorf("body = %q, want no internal detail", body)
		}
	})

	t.Run("a signal that finds no execution is not found", func(t *testing.T) {
		t.Parallel()

		handler, _, states, signals := newTestHandler()
		states.view = runningView()
		signals.err = fmt.Errorf("signal pause_rollout: %w", temporal.ErrRolloutNotFound)

		rec := do(t, handler, http.MethodPost, "/api/rollouts/ro-1/pause", "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusNotFound, rec.Body)
		}
	})
}

func TestHandlerRoutes(t *testing.T) {
	t.Parallel()

	t.Run("an unsupported method is rejected", func(t *testing.T) {
		t.Parallel()

		handler, starter, _, signals := newTestHandler()
		for _, tc := range []struct{ method, path string }{
			{http.MethodGet, "/api/rollouts"},
			{http.MethodDelete, "/api/rollouts/ro-1"},
			{http.MethodPut, "/api/rollouts/ro-1/pause"},
			{http.MethodGet, "/api/rollouts/ro-1/approve"},
		} {
			t.Run(tc.method+" "+tc.path, func(t *testing.T) {
				t.Parallel()

				rec := do(t, handler, tc.method, tc.path, "")
				if rec.Code != http.StatusMethodNotAllowed {
					t.Errorf("status = %d, want %d (body %s)",
						rec.Code, http.StatusMethodNotAllowed, rec.Body)
				}
			})
		}
		if got := starter.recorded(); len(got) != 0 {
			t.Errorf("started rollouts = %+v, want none", got)
		}
		approved, paused, resumed := signals.delivered()
		if len(approved)+len(paused)+len(resumed) != 0 {
			t.Errorf("delivered commands, want none")
		}
	})

	t.Run("an unknown path is rejected", func(t *testing.T) {
		t.Parallel()

		handler, _, _, _ := newTestHandler()
		rec := do(t, handler, http.MethodPost, "/api/rollouts/ro-1/rollback", "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("a rejection carries an operator-safe body", func(t *testing.T) {
		t.Parallel()

		handler, _, _, _ := newTestHandler()
		rec := do(t, handler, http.MethodGet, "/api/rollouts", "")

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
			t.Errorf("content type = %q, want the mux's plain-text rejection", ct)
		}
		if rec.Body.Len() == 0 {
			t.Error("rejection body is empty, want an operator-readable reason")
		}
	})
}
