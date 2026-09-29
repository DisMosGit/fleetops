// Package rolloutapi serves the operator's HTTP surface over a rollout: starting one, reading its
// current state, and sending the approve, pause, and resume commands.
//
// It is a thin adapter over three seams — a starter, a state reader, and a signal sender — and it
// answers with the rollout workflow's own state view and delivers the workflow's own signals, so
// what an operator reads and sends over HTTP is what the rollout decides and receives. It holds no
// state of its own and knows nothing about Temporal: the entrypoint wires adapters for it.
package rolloutapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/DisMosGit/fleetops/internal/rollout"
	"github.com/DisMosGit/fleetops/internal/temporal"
)

// backendTimeout bounds every call this package makes to the workflow backend. A query against an
// execution nothing is running blocks until its own deadline, so the surface answers a bounded
// failure instead of holding the operator's request open.
const backendTimeout = 5 * time.Second

// Starter begins one rollout. *temporal.RolloutStarter satisfies it.
type Starter interface {
	// Start begins the rollout the request names, reporting an error wrapping
	// temporal.ErrRolloutExists when that rollout id already has a workflow execution.
	Start(ctx context.Context, req temporal.RolloutRequest) error
}

// States reads one rollout's authoritative state. *temporal.RolloutStates satisfies it.
type States interface {
	// State returns the state the rollout's workflow execution reports, reporting an error
	// wrapping temporal.ErrRolloutNotFound when the rollout has no execution.
	State(ctx context.Context, rolloutID string) (temporal.RolloutView, error)
}

// Signals delivers the operator's commands to a rollout. *temporal.RolloutSignals satisfies it.
type Signals interface {
	// Approve authorizes the rollout's next gated wave.
	Approve(ctx context.Context, rolloutID string) error
	// Pause holds the rollout at its next wave boundary.
	Pause(ctx context.Context, rolloutID string) error
	// Resume lets a paused rollout continue from where it stopped.
	Resume(ctx context.Context, rolloutID string) error
}

// Handler serves the rollout API.
type Handler struct {
	starter Starter
	states  States
	signals Signals
	log     *slog.Logger
}

// NewHandler returns the rollout API over the three seams it consumes, serving:
//
//	POST /api/rollouts                 start a rollout
//	GET  /api/rollouts/{id}            read a rollout's current state
//	POST /api/rollouts/{id}/approve    authorize the next gated wave
//	POST /api/rollouts/{id}/pause      hold the rollout
//	POST /api/rollouts/{id}/resume     continue a held rollout
//
// The routes are method-scoped patterns, so the mux answers a mismatched method with 405 and an
// unregistered path with 404 without this package spelling either case out.
func NewHandler(starter Starter, states States, signals Signals, log *slog.Logger) http.Handler {
	h := &Handler{starter: starter, states: states, signals: signals, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/rollouts", h.handleStart)
	mux.HandleFunc("GET /api/rollouts/{id}", h.handleState)
	mux.HandleFunc("POST /api/rollouts/{id}/approve", h.command(temporal.ApproveNextWaveSignalName, signals.Approve))
	mux.HandleFunc("POST /api/rollouts/{id}/pause", h.command(temporal.PauseRolloutSignalName, signals.Pause))
	mux.HandleFunc("POST /api/rollouts/{id}/resume", h.command(temporal.ResumeRolloutSignalName, signals.Resume))
	return mux
}

// startRequest is the POST /api/rollouts body: the rollout to start and what it deploys to whom.
type startRequest struct {
	// RolloutID is the rollout to start; it is the request's idempotency key.
	RolloutID string `json:"rollout_id"`
	// FirmwareID is the firmware to deploy.
	FirmwareID string `json:"firmware_id"`
	// Region is the target selector's region.
	Region string `json:"region"`
	// Model is the target selector's device model.
	Model string `json:"model"`
}

// startResponse is the 202 body of a start: what the operator needs to watch or command the
// rollout it just began.
type startResponse struct {
	// RolloutID is the rollout that was started.
	RolloutID string `json:"rollout_id"`
	// WorkflowID is the workflow execution driving it.
	WorkflowID string `json:"workflow_id"`
	// Status is the status the rollout starts in.
	Status string `json:"status"`
}

// commandResponse is the 202 body of an approve, pause, or resume: which rollout the command
// reached and which signal carried it.
type commandResponse struct {
	// RolloutID is the rollout the command was delivered to.
	RolloutID string `json:"rollout_id"`
	// Signal is the workflow signal the command delivered.
	Signal string `json:"signal"`
}

// errorResponse is the body of every rejection. Message is always operator-safe: it names what the
// operator got wrong, never a driver or backend internal.
type errorResponse struct {
	// Error is the operator-safe reason.
	Error string `json:"error"`
}

// handleStart serves one rollout start. The rollout id is the idempotency key: a start for an id
// that already has a workflow execution is refused with 409 rather than beginning a second run
// chain beside the first one's records. The firmware is deliberately not verified here — an
// unknown or mismatched firmware is the rollout's own recorded failure, not a start refusal — so
// this path only decides whether the request is well-formed and the id is free.
func (h *Handler) handleStart(w http.ResponseWriter, r *http.Request) {
	var body startRequest
	if err := decodeBody(r, &body); err != nil {
		h.reject(w, r, http.StatusBadRequest, err)
		return
	}
	req := temporal.RolloutRequest{
		RolloutID:  strings.TrimSpace(body.RolloutID),
		FirmwareID: strings.TrimSpace(body.FirmwareID),
		Region:     strings.TrimSpace(body.Region),
		Model:      strings.TrimSpace(body.Model),
	}
	for _, required := range []struct {
		field string
		value string
	}{
		{"rollout_id", req.RolloutID},
		{"firmware_id", req.FirmwareID},
		{"region", req.Region},
		{"model", req.Model},
	} {
		if required.value == "" {
			h.reject(w, r, http.StatusBadRequest, fmt.Errorf("%s is required", required.field))
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	if err := h.starter.Start(ctx, req); err != nil {
		if errors.Is(err, temporal.ErrRolloutExists) {
			h.reject(w, r, http.StatusConflict, fmt.Errorf("rollout %s already exists", req.RolloutID))
			return
		}
		h.fail(w, r, "start rollout", err)
		return
	}
	h.log.Info("rollout started",
		"rollout_id", req.RolloutID, "firmware_id", req.FirmwareID,
		"region", req.Region, "model", req.Model)
	writeJSON(w, h.log, http.StatusAccepted, startResponse{
		RolloutID:  req.RolloutID,
		WorkflowID: temporal.RolloutWorkflowID(req.RolloutID),
		// A rollout is recorded running before it does anything else, so a start the backend
		// accepted has that status to report.
		Status: string(rollout.RolloutRunning),
	})
}

// handleState serves one rollout's current state, answering with the workflow's own view verbatim:
// the state query is the contract, so the API cannot drift from what the rollout reports.
func (h *Handler) handleState(w http.ResponseWriter, r *http.Request) {
	rolloutID := r.PathValue("id")
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()
	view, err := h.states.State(ctx, rolloutID)
	if err != nil {
		if errors.Is(err, temporal.ErrRolloutNotFound) {
			h.reject(w, r, http.StatusNotFound, fmt.Errorf("rollout %s not found", rolloutID))
			return
		}
		h.fail(w, r, "read rollout state", err)
		return
	}
	writeJSON(w, h.log, http.StatusOK, view)
}

// command returns the handler for one operator command. The command reads the rollout's state
// first, so a rollout that has already concluded is refused with 409 instead of accepting a signal
// that could no longer change anything.
func (h *Handler) command(signal string, deliver func(context.Context, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rolloutID := r.PathValue("id")
		ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
		defer cancel()

		view, err := h.states.State(ctx, rolloutID)
		if err != nil {
			if errors.Is(err, temporal.ErrRolloutNotFound) {
				h.reject(w, r, http.StatusNotFound, fmt.Errorf("rollout %s not found", rolloutID))
				return
			}
			h.fail(w, r, "read rollout state before "+signal, err)
			return
		}
		if view.Status.Terminal() {
			h.reject(w, r, http.StatusConflict, fmt.Errorf(
				"rollout %s has concluded as %s", rolloutID, view.Status))
			return
		}
		if err := deliver(ctx, rolloutID); err != nil {
			if errors.Is(err, temporal.ErrRolloutNotFound) {
				h.reject(w, r, http.StatusNotFound, fmt.Errorf("rollout %s not found", rolloutID))
				return
			}
			h.fail(w, r, "deliver "+signal, err)
			return
		}
		h.log.Info("rollout command delivered",
			"rollout_id", rolloutID, "signal", signal, "status", view.Status)
		writeJSON(w, h.log, http.StatusAccepted, commandResponse{RolloutID: rolloutID, Signal: signal})
	}
}

// decodeBody decodes one JSON request body into body, refusing a body that is not valid JSON or
// carries fields beyond the contract.
func decodeBody(r *http.Request, body any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(body); err != nil {
		return fmt.Errorf("request body: %w", err)
	}
	return nil
}

// reject answers a request the operator got wrong: the reason is the operator's to act on, so it
// is echoed, and the rejection is logged at this boundary.
func (h *Handler) reject(w http.ResponseWriter, r *http.Request, code int, err error) {
	h.log.Warn("rollout request rejected",
		"method", r.Method, "path", r.URL.Path, "status", code, "reason", err)
	writeJSON(w, h.log, code, errorResponse{Error: err.Error()})
}

// fail answers a backend failure: the operator is told the request failed and nothing about the
// backend, and the detail is logged here rather than sent.
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, action string, err error) {
	h.log.Error("rollout request failed",
		"action", action, "method", r.Method, "path", r.URL.Path, "err", err)
	writeJSON(w, h.log, http.StatusInternalServerError, errorResponse{Error: "internal error"})
}

// writeJSON writes body as the response, logging — never swallowing — a write error.
func writeJSON(w http.ResponseWriter, log *slog.Logger, code int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		log.Error("encode rollout response", "status", code, "err", err)
		http.Error(w, "encode rollout response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(data); err != nil {
		log.Error("write rollout response", "status", code, "err", err)
	}
}
