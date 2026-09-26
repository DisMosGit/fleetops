package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// fakeCheck is a hand-written Check double whose behavior can flip between requests. Flips are
// safe: the handler joins every check goroutine before responding, so a between-request write
// is ordered after the previous reads and before the next ones.
type fakeCheck struct {
	name  string
	fails bool // when true, Check returns an error
	block bool // when true, Check waits for ctx cancellation
}

func (f *fakeCheck) Name() string { return f.name }

func (f *fakeCheck) Check(ctx context.Context) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.fails {
		return errors.New("connection refused")
	}
	return nil
}

// request runs one probe request against handler and returns the recorder.
func request(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// asChecks widens fake checks to the Check slice NewHandler takes.
func asChecks(fakes ...fakeCheck) []Check {
	checks := make([]Check, len(fakes))
	for i := range fakes {
		checks[i] = &fakes[i]
	}
	return checks
}

// decodeReadiness parses the /readyz body for assertions.
func decodeReadiness(t *testing.T, rec *httptest.ResponseRecorder) readinessResponse {
	t.Helper()
	var got readinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode readiness body %q: %v", rec.Body.String(), err)
	}
	return got
}

func TestLiveness(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		checks []fakeCheck
	}{
		{name: "dependencies up", checks: []fakeCheck{{name: "mongodb"}, {name: "rabbitmq"}}},
		{
			name:   "dependencies down",
			checks: []fakeCheck{{name: "mongodb", fails: true}, {name: "temporal", block: true}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := NewHandler(asChecks(tc.checks...)...)
			rec := request(t, handler, http.MethodGet, "/healthz")

			if rec.Code != http.StatusOK {
				t.Errorf("GET /healthz status = %d, want %d", rec.Code, http.StatusOK)
			}
			if body := rec.Body.String(); body != `{"status":"ok"}` {
				t.Errorf("GET /healthz body = %q, want %q", body, `{"status":"ok"}`)
			}
		})
	}
}

func TestLivenessRejectsNonGet(t *testing.T) {
	t.Parallel()

	rec := request(t, NewHandler(), http.MethodPost, "/healthz")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestReadinessAllGreen(t *testing.T) {
	t.Parallel()

	handler := NewHandler(asChecks(
		fakeCheck{name: "mongodb"},
		fakeCheck{name: "rabbitmq"},
		fakeCheck{name: "temporal"},
	)...)
	rec := request(t, handler, http.MethodGet, "/readyz")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /readyz status = %d, want %d", rec.Code, http.StatusOK)
	}
	got := decodeReadiness(t, rec)
	want := readinessResponse{
		Status: "ready",
		Checks: map[string]checkResult{
			"mongodb":  {Status: statusOK},
			"rabbitmq": {Status: statusOK},
			"temporal": {Status: statusOK},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GET /readyz body mismatch (-want +got):\n%s", diff)
	}
}

func TestReadinessOneDown(t *testing.T) {
	t.Parallel()

	handler := NewHandler(asChecks(
		fakeCheck{name: "mongodb"},
		fakeCheck{name: "rabbitmq", fails: true},
		fakeCheck{name: "temporal"},
	)...)
	rec := request(t, handler, http.MethodGet, "/readyz")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	got := decodeReadiness(t, rec)
	if got.Checks["mongodb"].Status != statusOK || got.Checks["temporal"].Status != statusOK {
		t.Errorf("healthy checks = %+v, want both ok", got.Checks)
	}
	if got.Checks["rabbitmq"].Status != statusError {
		t.Errorf("rabbitmq check = %+v, want status error", got.Checks["rabbitmq"])
	}
	if got.Checks["rabbitmq"].Summary == "" {
		t.Error("rabbitmq check summary is empty, want the check error")
	}
}

func TestReadinessBoundedWhenAllDown(t *testing.T) {
	t.Parallel()

	handler := NewHandler(asChecks(
		fakeCheck{name: "mongodb", block: true},
		fakeCheck{name: "rabbitmq", block: true},
		fakeCheck{name: "temporal", block: true},
	)...)

	start := time.Now()
	rec := request(t, handler, http.MethodGet, "/readyz")
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if elapsed > 2*checkTimeout {
		t.Errorf("GET /readyz took %v, want it bounded by the per-check timeout %v", elapsed, checkTimeout)
	}
	got := decodeReadiness(t, rec)
	for name, res := range got.Checks {
		if res.Status != statusError || res.Summary == "" {
			t.Errorf("check %q = %+v, want error status with a summary", name, res)
		}
	}
}

func TestReadinessRecoversWithoutRestart(t *testing.T) {
	t.Parallel()

	check := &fakeCheck{name: "mongodb", fails: true}
	handler := NewHandler(check)

	if rec := request(t, handler, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("first GET /readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	check.fails = false
	if rec := request(t, handler, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("second GET /readyz status = %d, want %d after recovery", rec.Code, http.StatusOK)
	}
}

func TestReadinessSummariesNeverLeakCredentials(t *testing.T) {
	t.Parallel()

	checks, err := NewDependencyChecks(
		"mongodb://operator:secret@"+unusedPort(t),
		"amqp://operator:secret@"+unusedPort(t)+"/",
		unusedPort(t),
	)
	if err != nil {
		t.Fatalf("NewDependencyChecks() error = %v", err)
	}
	rec := request(t, NewHandler(checks...), http.MethodGet, "/readyz")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	body := rec.Body.String()
	for _, secret := range []string{"operator", "secret"} {
		if strings.Contains(body, secret) {
			t.Errorf("GET /readyz body = %q, want it not to contain %q", body, secret)
		}
	}
	if !strings.Contains(body, statusError) {
		t.Errorf("GET /readyz body = %q, want it to report an error", body)
	}
}
