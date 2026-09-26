package firmware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// Registry is the firmware registry the upload endpoint records into. *Store satisfies it.
type Registry interface {
	// Save stores one firmware binary and its metadata record.
	Save(ctx context.Context, version string, models []string, binary io.Reader) (Record, error)
}

// Handler serves the firmware upload API: one multipart POST per firmware, validated before
// anything is stored and answered with the recorded firmware record.
type Handler struct {
	registry Registry
	catalog  ModelCatalog
	log      *slog.Logger
}

// NewHandler returns the firmware upload API on POST /api/firmwares, recording uploads in
// registry and validating their target models against catalog.
func NewHandler(registry Registry, catalog ModelCatalog, log *slog.Logger) http.Handler {
	h := &Handler{registry: registry, catalog: catalog, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/firmwares", h.handleUpload)
	return mux
}

// uploadResponse is the 201 body: the recorded firmware record as the operator needs it.
type uploadResponse struct {
	// ID is the firmware identity the rollout will reference.
	ID string `json:"id"`
	// Version is the recorded firmware version.
	Version string `json:"version"`
	// Models are the target device models the upload declared.
	Models []string `json:"models"`
	// Checksum is the lowercase hexadecimal SHA-256 digest of the stored binary.
	Checksum string `json:"checksum"`
	// Size is the stored binary's size in bytes.
	Size int64 `json:"size"`
}

// errorResponse is the body of every rejected upload.
type errorResponse struct {
	// Error is the operator-safe rejection reason.
	Error string `json:"error"`
}

// handleUpload serves one firmware upload: parse and validate first, store second, and map
// every rejection onto its status code with an operator-safe body.
func (h *Handler) handleUpload(w http.ResponseWriter, r *http.Request) {
	rec, err := h.upload(r)
	if err != nil {
		h.reject(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, uploadResponse{
		ID:       rec.ID,
		Version:  rec.Version,
		Models:   rec.Models,
		Checksum: rec.Checksum,
		Size:     rec.Size,
	})
}

// upload parses, validates, and stores one upload. Its errors wrap the sentinel they belong
// to — ErrInvalidMetadata, ErrIncompatible, or ErrVersionConflict — or carry a store failure
// the endpoint must not echo.
func (h *Handler) upload(r *http.Request) (Record, error) {
	binary, _, err := r.FormFile("binary")
	if err != nil {
		return Record{}, fmt.Errorf("%w: binary part required", ErrInvalidMetadata)
	}
	defer func() {
		if err := binary.Close(); err != nil {
			h.log.Error("close uploaded firmware", "err", err)
		}
	}()

	version := strings.TrimSpace(r.FormValue("version"))
	if version == "" {
		return Record{}, fmt.Errorf("%w: version required", ErrInvalidMetadata)
	}
	values, ok := r.MultipartForm.Value["models"]
	if !ok {
		return Record{}, fmt.Errorf("%w: models field required", ErrInvalidMetadata)
	}
	models := ParseModels(values)
	if err := ValidateUpload(r.Context(), h.catalog, version, models); err != nil {
		return Record{}, err
	}
	return h.registry.Save(r.Context(), version, models, binary)
}

// reject answers a rejected upload: sentinel failures carry their operator-safe reason, and a
// failure that is not one of the expected outcomes is logged at this boundary and echoed as a
// bare internal error, never as driver detail.
func (h *Handler) reject(w http.ResponseWriter, err error) {
	code, msg := http.StatusInternalServerError, "internal error"
	switch {
	case errors.Is(err, ErrInvalidMetadata):
		code, msg = http.StatusBadRequest, err.Error()
	case errors.Is(err, ErrIncompatible):
		code, msg = http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, ErrVersionConflict):
		code, msg = http.StatusConflict, err.Error()
	default:
		h.log.Error("store firmware upload", "err", err)
	}
	writeJSON(w, code, errorResponse{Error: msg})
}

// writeJSON writes body as the response, logging — never swallowing — a write error.
func writeJSON[T any](w http.ResponseWriter, code int, body T) {
	data, err := json.Marshal(body)
	if err != nil {
		slog.Error("encode firmware response", "err", err)
		http.Error(w, "encode firmware response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(data); err != nil {
		slog.Error("write firmware response", "err", err)
	}
}
