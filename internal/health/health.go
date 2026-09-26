// Package health serves the HTTP liveness and readiness probes of the FleetOps services.
//
// Liveness reports only that the process is serving; readiness runs the dependency
// connectivity checks on every request and reports each dependency separately.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Check is one dependency connectivity probe, named for the readiness report.
type Check interface {
	// Name is the dependency key the readiness report uses, e.g. "mongodb".
	Name() string
	// Check returns nil when the dependency is reachable and must respect ctx.
	Check(ctx context.Context) error
}

// checkTimeout bounds each readiness check so /readyz answers even when every dependency is
// down. It is deliberately not configurable: nothing depends on tuning it.
const checkTimeout = 2 * time.Second

// Check status values in the readiness report.
const (
	statusOK    = "ok"
	statusError = "error"
)

// NewHandler returns the probe handler serving GET /healthz (liveness) and GET /readyz
// (readiness over the given checks).
func NewHandler(checks ...Check) http.Handler {
	h := &handler{checks: checks}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.handleLiveness)
	mux.HandleFunc("GET /readyz", h.handleReadiness)
	return mux
}

// handler serves the probe endpoints.
type handler struct {
	checks []Check
}

// livenessResponse is the /healthz body.
type livenessResponse struct {
	Status string `json:"status"`
}

// checkResult is one dependency's readiness outcome.
type checkResult struct {
	Status  string `json:"status"`
	Summary string `json:"summary,omitempty"`
}

// readinessResponse is the /readyz body.
type readinessResponse struct {
	Status string                 `json:"status"`
	Checks map[string]checkResult `json:"checks"`
}

// handleLiveness reports that the process is serving; it never consults dependencies.
func (h *handler) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, livenessResponse{Status: statusOK})
}

// handleReadiness runs every check and answers 200 only when all of them pass.
func (h *handler) handleReadiness(w http.ResponseWriter, r *http.Request) {
	results := h.runChecks(r.Context())
	code := http.StatusOK
	resp := readinessResponse{Status: "ready", Checks: results}
	for _, res := range results {
		if res.Status != statusOK {
			resp.Status = "not_ready"
			code = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, resp)
}

// runChecks runs all checks concurrently — each under its own timeout — and returns their
// outcomes by dependency name.
func (h *handler) runChecks(ctx context.Context) map[string]checkResult {
	outcomes := make([]checkResult, len(h.checks))
	names := make([]string, len(h.checks))
	var wg sync.WaitGroup
	for i, check := range h.checks {
		names[i] = check.Name()
		wg.Add(1)
		go func() {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
			defer cancel()
			if err := check.Check(checkCtx); err != nil {
				outcomes[i] = checkResult{Status: statusError, Summary: err.Error()}
				return
			}
			outcomes[i] = checkResult{Status: statusOK}
		}()
	}
	wg.Wait()
	results := make(map[string]checkResult, len(outcomes))
	for i, name := range names {
		results[name] = outcomes[i]
	}
	return results
}

// writeJSON writes body as the JSON response, logging — never swallowing — a write error.
func writeJSON[T any](w http.ResponseWriter, code int, body T) {
	data, err := json.Marshal(body)
	if err != nil {
		slog.Error("encode probe response", "err", err)
		http.Error(w, "encode probe response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(data); err != nil {
		slog.Error("write probe response", "err", err)
	}
}
