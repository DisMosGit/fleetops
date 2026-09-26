package firmware

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// fakeRegistry records Save calls and returns a queued error first.
type fakeRegistry struct {
	calls   int
	saved   []byte
	version string
	models  []string
	rec     Record
	err     error
}

// Save records the call and returns the queued outcome.
func (f *fakeRegistry) Save(_ context.Context, version string, models []string, binary io.Reader) (Record, error) {
	f.calls++
	f.version, f.models = version, models
	data, err := io.ReadAll(binary)
	if err != nil {
		return Record{}, err
	}
	f.saved = data
	if f.err != nil {
		return Record{}, f.err
	}
	rec := f.rec
	rec.Version = version
	rec.Models = models
	return rec, nil
}

// uploadRequest builds one multipart upload request for the handler under test.
func uploadRequest(t *testing.T, fields map[string][]string, withBinary bool, content []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for name, values := range fields {
		for _, value := range values {
			if err := mw.WriteField(name, value); err != nil {
				t.Fatalf("write field %s: %v", name, err)
			}
		}
	}
	if withBinary {
		part, err := mw.CreateFormFile("binary", "fw.bin")
		if err != nil {
			t.Fatalf("create binary part: %v", err)
		}
		if _, err := part.Write(content); err != nil {
			t.Fatalf("write binary part: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/firmwares", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// serve runs one request through the handler and decodes its JSON response.
func serve(t *testing.T, handler http.Handler, req *http.Request) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec, body
}

func TestUpload(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	catalog := fakeCatalog{models: []string{"oak-s3", "oak-s5", "birch-x1"}}
	fields := map[string][]string{
		"version": {"2.0.0"},
		"models":  {"oak-s3, birch-x1"},
	}

	t.Run("accepted upload records the firmware", func(t *testing.T) {
		t.Parallel()
		registry := &fakeRegistry{rec: Record{ID: "fw-1", Checksum: "sum", Size: 3}}
		handler := NewHandler(registry, catalog, logger)

		rec, body := serve(t, handler,
			uploadRequest(t, fields, true, []byte("bin")))

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
		}
		want := map[string]any{
			"id": "fw-1", "version": "2.0.0", "models": []any{"oak-s3", "birch-x1"},
			"checksum": "sum", "size": float64(3),
		}
		if diff := cmp.Diff(want, body); diff != "" {
			t.Errorf("response body mismatch (-want +got):\n%s", diff)
		}
		if registry.calls != 1 || registry.version != "2.0.0" {
			t.Fatalf("registry calls = %d (version %q), want one call with version 2.0.0",
				registry.calls, registry.version)
		}
		if len(registry.models) != 2 || registry.models[0] != "oak-s3" || registry.models[1] != "birch-x1" {
			t.Errorf("stored models = %v, want [oak-s3 birch-x1]", registry.models)
		}
		if !bytes.Equal(registry.saved, []byte("bin")) {
			t.Errorf("stored binary = %q, want %q", registry.saved, "bin")
		}
	})

	t.Run("upload larger than the transfer buffer keeps its checksum", func(t *testing.T) {
		t.Parallel()
		content := payload(1 << 20)
		store := &Store{meta: newFakeMetadata(), binaries: newFakeBinaries()}
		handler := NewHandler(store, catalog, logger)

		rec, body := serve(t, handler,
			uploadRequest(t, fields, true, content))

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
		}
		if got, _ := body["checksum"].(string); got != checksumOf(content) {
			t.Errorf("checksum = %q, want %q", got, checksumOf(content))
		}
		if got, _ := body["size"].(float64); int64(got) != int64(len(content)) {
			t.Errorf("size = %v, want %d", got, len(content))
		}
	})

	rejections := []struct {
		name        string
		fields      map[string][]string
		withBinary  bool
		content     []byte
		registryErr error
		wantStatus  int
		wantDetail  string
	}{
		{
			name:       "no binary part",
			fields:     fields,
			withBinary: false,
			wantStatus: http.StatusBadRequest,
			wantDetail: "binary part",
		},
		{
			name:       "no version",
			fields:     map[string][]string{"models": {"oak-s3"}},
			withBinary: true,
			wantStatus: http.StatusBadRequest,
			wantDetail: "version",
		},
		{
			name:       "empty version",
			fields:     map[string][]string{"version": {"  "}, "models": {"oak-s3"}},
			withBinary: true,
			wantStatus: http.StatusBadRequest,
			wantDetail: "version",
		},
		{
			name:       "no models field",
			fields:     map[string][]string{"version": {"2.0.0"}},
			withBinary: true,
			wantStatus: http.StatusBadRequest,
			wantDetail: "models",
		},
		{
			name:       "empty model list",
			fields:     map[string][]string{"version": {"2.0.0"}, "models": {" "}},
			withBinary: true,
			wantStatus: http.StatusUnprocessableEntity,
			wantDetail: "target model",
		},
		{
			name:       "duplicate models",
			fields:     map[string][]string{"version": {"2.0.0"}, "models": {"oak-s3,oak-s3"}},
			withBinary: true,
			wantStatus: http.StatusUnprocessableEntity,
			wantDetail: "duplicate",
		},
		{
			name:       "unknown model",
			fields:     map[string][]string{"version": {"2.0.0"}, "models": {"walnut-9"}},
			withBinary: true,
			wantStatus: http.StatusUnprocessableEntity,
			wantDetail: "walnut-9",
		},
		{
			name:        "version conflict",
			fields:      fields,
			withBinary:  true,
			registryErr: fmt.Errorf("save firmware: %w: 2.0.0", ErrVersionConflict),
			wantStatus:  http.StatusConflict,
			wantDetail:  "2.0.0",
		},
		{
			name:        "store failure hides its detail",
			fields:      fields,
			withBinary:  true,
			registryErr: errors.New("mongo: connection refused at 10.0.0.5"),
			wantStatus:  http.StatusInternalServerError,
			wantDetail:  "internal error",
		},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := &fakeRegistry{err: tc.registryErr}
			handler := NewHandler(registry, catalog, logger)

			rec, body := serve(t, handler,
				uploadRequest(t, tc.fields, tc.withBinary, tc.content))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			got, _ := body["error"].(string)
			if !strings.Contains(got, tc.wantDetail) {
				t.Errorf("error = %q, want it to contain %q", got, tc.wantDetail)
			}
			if tc.wantStatus == http.StatusInternalServerError {
				if strings.Contains(got, "mongo") {
					t.Errorf("error = %q, want no implementation detail", got)
				}
				return
			}
			if tc.registryErr == nil && registry.calls != 0 {
				t.Errorf("registry calls = %d, want 0 (a rejected upload stores nothing)", registry.calls)
			}
		})
	}
}
