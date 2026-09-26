//go:build integration

package firmware_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/firmware"
	"github.com/DisMosGit/fleetops/internal/mongotest"
)

// recordingSignaler is a hand-written agentserver.DeviceSignaler double recording the update
// reports and command results the control plane forwards.
type recordingSignaler struct {
	mu      sync.Mutex
	updates []*agentv1.UpdateStatusRequest
	results []*agentv1.ReportRequest
}

// SignalHeartbeat is unused in this path.
func (s *recordingSignaler) SignalHeartbeat(context.Context, devices.Record, *agentv1.Heartbeat) error {
	return nil
}

// SignalCommandResult records the terminal result.
func (s *recordingSignaler) SignalCommandResult(_ context.Context, res *agentv1.ReportRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, res)
	return nil
}

// SignalUpdateStatus records the update progress report.
func (s *recordingSignaler) SignalUpdateStatus(_ context.Context, req *agentv1.UpdateStatusRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, req)
	return nil
}

// recorded returns the update phases forwarded and the terminal results forwarded.
func (s *recordingSignaler) recorded() ([]string, []*agentv1.ReportRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var phases []string
	for _, u := range s.updates {
		phases = append(phases, u.GetPhase().String())
	}
	return phases, append([]*agentv1.ReportRequest(nil), s.results...)
}

// nopSink swallows routed heartbeats: this path is firmware delivery, not telemetry.
type nopSink struct{}

// Handle discards one routed heartbeat.
func (nopSink) Handle(context.Context, *agentv1.Heartbeat, devices.Record) error { return nil }

// TestFirmwarePipeline walks the whole firmware path against the real schema: an operator
// upload through the HTTP endpoint, chunked delivery over DownloadFirmware, the agent's
// download and checksum verification to disk, the deterministic apply stub, and the update
// reports and terminal result back into the control plane.
func TestFirmwarePipeline(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	log := slog.New(slog.DiscardHandler)

	registry := devices.NewStore(harness.DB)
	store := firmware.NewStore(harness.DB)
	signals := &recordingSignaler{}
	hub := agentserver.NewHub(nopSink{}, log)

	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(server,
		agentserver.NewServer(hub, registry, signals, store, log))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close conn: %v", err)
		}
	})
	svc := agentv1.NewAgentServiceClient(conn)

	// The emulator fleet and its update cycle: one device that downloads to a per-test
	// directory and applies through the deterministic stub at a certain success rate.
	ids, err := agent.NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	fleet, err := agent.NewFleet(1, agent.FleetOptions{
		IDs: ids, Source: agent.NewSimulation(rand.NewSource(1)), Period: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	downloadDir := t.TempDir()
	downloader, err := agent.NewDownloader(svc, downloadDir)
	if err != nil {
		t.Fatalf("NewDownloader: %v", err)
	}
	applier, err := agent.NewApplier(time.Millisecond, 1.0, rand.NewSource(1))
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	relay := &agent.StatusRelay{}
	handler := agent.NewUpdateHandler(downloader, applier, fleet,
		agent.NewUpdateReporter(relay, svc), log)
	client, err := agent.NewClient(svc, agent.Options{
		Devices: fleet.Identities(), Handler: handler, IDs: ids, Log: log,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	relay.Bind(client)

	heartbeats := make(chan *agentv1.Heartbeat, 1)
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx, heartbeats) }()
	t.Cleanup(func() {
		cancel()
		if err := <-runDone; err != nil {
			t.Logf("stream client: %v", err)
		}
	})

	// The device registers on the stream first: upload validation checks target models
	// against the registered fleet.
	waitFor(t, "device registration", func() bool {
		rec, err := registry.DeviceModels(ctx)
		return err == nil && len(rec) > 0
	})

	// Operator upload: the binary streams into GridFS with its checksum computed on the way.
	binary := make([]byte, 1<<20) // several GridFS chunks
	for i := range binary {
		binary[i] = byte(i % 251)
	}
	upload := uploadFirmware(t, firmware.NewHandler(store, registry, log), binary, "2.0.0", "oak-s3")
	wantSum := sha256.Sum256(binary)
	if upload.Checksum != hex.EncodeToString(wantSum[:]) {
		t.Fatalf("upload checksum = %q, want the SHA-256 of the binary", upload.Checksum)
	}
	if upload.Size != int64(len(binary)) {
		t.Errorf("upload size = %d, want %d", upload.Size, len(binary))
	}

	// The control plane dispatches the update command; the agent's whole cycle runs from
	// there: download, verify to disk, apply, adopt, and report.
	cmd := &agentv1.Command{
		CommandId: "cmd-1",
		DeviceId:  fleet.Identities()[0].ID,
		Kind: &agentv1.Command_StartUpdate{StartUpdate: &agentv1.StartUpdate{
			FirmwareId: upload.ID,
			Version:    upload.Version,
			Checksum:   upload.Checksum,
		}},
	}
	waitFor(t, "command dispatch", func() bool {
		return hub.Send(ctx, cmd) == nil
	})

	waitFor(t, "terminal command result", func() bool {
		_, results := signals.recorded()
		return len(results) > 0
	})

	path := filepath.Join(downloadDir, upload.ID+".bin")
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded firmware: %v", err)
	}
	gotSum := sha256.Sum256(stored)
	if hex.EncodeToString(gotSum[:]) != upload.Checksum {
		t.Error("downloaded binary does not match the uploaded checksum")
	}
	if !bytes.Equal(stored, binary) {
		t.Error("downloaded binary differs from the uploaded content")
	}

	phases, results := signals.recorded()
	sawDownloading, sawApplying, sawCompleted := false, false, false
	for _, phase := range phases {
		switch phase {
		case "UPDATE_PHASE_DOWNLOADING":
			sawDownloading = true
		case "UPDATE_PHASE_APPLYING":
			sawApplying = true
		case "UPDATE_PHASE_COMPLETED":
			sawCompleted = true
		}
	}
	if !sawDownloading || !sawApplying || !sawCompleted {
		t.Errorf("update phases = %v, want downloading, applying, and completed", phases)
	}
	if len(results) != 1 || results[0].GetOutcome() != agentv1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED ||
		results[0].GetIdempotencyKey() == "" {
		t.Errorf("terminal results = %+v, want one accepted success under its idempotency key", results)
	}
	if got := fleet.Identities()[0].Firmware; got != "2.0.0" {
		t.Errorf("device firmware = %q, want the applied version", got)
	}
}

// uploadResponse is the 201 body of the upload endpoint.
type uploadResponse struct {
	ID       string   `json:"id"`
	Version  string   `json:"version"`
	Models   []string `json:"models"`
	Checksum string   `json:"checksum"`
	Size     int64    `json:"size"`
}

// uploadFirmware posts one binary to the upload endpoint and returns its record.
func uploadFirmware(
	t *testing.T,
	handler http.Handler,
	binary []byte,
	version, models string,
) uploadResponse {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("version", version); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := mw.WriteField("models", models); err != nil {
		t.Fatalf("write models: %v", err)
	}
	part, err := mw.CreateFormFile("binary", "fw.bin")
	if err != nil {
		t.Fatalf("create binary part: %v", err)
	}
	if _, err := part.Write(binary); err != nil {
		t.Fatalf("write binary part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/firmwares", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var out uploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode upload response %q: %v", rec.Body.String(), err)
	}
	return out
}

// waitFor polls until ok or the deadline passes — for the asynchronous edges of the cycle
// (registration, dispatch, reporting) that complete in their own goroutines.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
