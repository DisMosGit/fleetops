//go:build integration

package agentserver_test

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agentserver"
	"github.com/DisMosGit/fleetops/internal/devices"
	"github.com/DisMosGit/fleetops/internal/mongotest"
	"github.com/DisMosGit/fleetops/internal/telemetry"
)

// TestPersistencePathEndToEnd spans the whole change against the real schema: a stream
// registration upserts the device record, heartbeats land as batched telemetry documents with
// measurement times and identity meta, and once the device goes silent past the threshold the
// sweep marks it offline and counts exactly one transition.
func TestPersistencePathEndToEnd(t *testing.T) {
	t.Parallel()

	harness := mongotest.Start(t)
	log := slog.New(slog.DiscardHandler)
	store := devices.NewStore(harness.DB)
	// One flush per full batch and never on a timer: the stored events must arrive as a
	// single batched write, not one round trip per heartbeat.
	ingest := telemetry.NewWriter(harness.DB.Collection("telemetry"), store, 5, time.Hour, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingestDone := make(chan error, 1)
	go func() { ingestDone <- ingest.Run(ctx) }()

	hub := agentserver.NewHub(ingest, log)
	server := grpc.NewServer(agentserver.ServerOptions(log)...)
	agentv1.RegisterAgentServiceServer(server, agentserver.NewServer(hub, store, nopSignaler{}, log))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = server.Serve(lis) // ends at Stop
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
	})
	stream, err := agentv1.NewAgentServiceClient(conn).Connect(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// Registration upserts the device record with the registered identity.
	if err := stream.Send(&agentv1.AgentEnvelope{
		CorrelationId: "corr-1",
		Payload: &agentv1.AgentEnvelope_RegisterDevice{RegisterDevice: &agentv1.RegisterDeviceRequest{
			DeviceId: "dev-e2e", Model: "oak-s3", Region: "eu-west", CurrentFw: "1.0.0",
		}},
	}); err != nil {
		t.Fatalf("send registration: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive registration response: %v", err)
	}
	if !resp.GetRegisterDevice().GetAccepted() {
		t.Fatalf("registration rejected: %s", resp.GetRegisterDevice().GetReason())
	}

	deviceDocs := harness.DB.Collection("devices")
	var device struct {
		Model     string    `bson:"model"`
		Region    string    `bson:"region"`
		CurrentFw string    `bson:"current_fw"`
		Status    string    `bson:"status"`
		LastSeen  time.Time `bson:"last_heartbeat"`
	}
	if err := deviceDocs.FindOne(ctx, map[string]any{"_id": "dev-e2e"}).Decode(&device); err != nil {
		t.Fatalf("find device record: %v", err)
	}
	if device.Model != "oak-s3" || device.Region != "eu-west" || device.CurrentFw != "1.0.0" {
		t.Errorf("device record identity = %+v, want the registered identity", device)
	}
	if device.Status != devices.StatusOnline || device.LastSeen.IsZero() {
		t.Errorf("device record = %+v, want online with a last-seen timestamp", device)
	}

	// Heartbeats land as one batched write of telemetry documents.
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 5; i++ {
		hb := &agentv1.Heartbeat{
			EventId:   "ev-e2e-" + string(rune('a'+i)),
			DeviceId:  "dev-e2e",
			CurrentFw: "1.0.0",
			Status:    "online",
			Ts:        timestamppb.New(base.Add(time.Duration(i) * time.Second)),
			Cpu:       0.5,
			Mem:       0.4,
			Health:    0.9,
		}
		if err := stream.Send(&agentv1.AgentEnvelope{
			Payload: &agentv1.AgentEnvelope_Heartbeat{Heartbeat: hb},
		}); err != nil {
			t.Fatalf("send heartbeat %d: %v", i, err)
		}
	}

	telemetryDocs := harness.DB.Collection("telemetry")
	deadline := time.Now().Add(5 * time.Second)
	for {
		count, err := telemetryDocs.CountDocuments(ctx, map[string]any{"meta.device_id": "dev-e2e"})
		if err != nil {
			t.Fatalf("count telemetry: %v", err)
		}
		if count == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stored telemetry documents = %d, want 5", count)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := harness.InsertCommands("telemetry"); got != 1 {
		t.Errorf("telemetry insert commands = %d, want 1 batched write for 5 heartbeats", got)
	}

	var sample struct {
		TS   time.Time `bson:"ts"`
		Meta struct {
			DeviceID string `bson:"device_id"`
			Region   string `bson:"region"`
			Model    string `bson:"model"`
		} `bson:"meta"`
	}
	err = telemetryDocs.FindOne(ctx, map[string]any{"_id": "ev-e2e-a"}).Decode(&sample)
	if err != nil {
		t.Fatalf("find telemetry document: %v", err)
	}
	if !sample.TS.Equal(base) {
		t.Errorf("stored ts = %v, want the measurement time %v", sample.TS, base)
	}
	if sample.Meta.DeviceID != "dev-e2e" || sample.Meta.Region != "eu-west" || sample.Meta.Model != "oak-s3" {
		t.Errorf("stored meta = %+v, want the registered identity of dev-e2e", sample.Meta)
	}

	// Silence past the threshold: the sweep marks the device offline and counts one transition.
	var transitions atomic.Int64
	sweeper := devices.NewSweeper(store, 50*time.Millisecond, 10*time.Millisecond,
		func(n int64) { transitions.Add(n) }, log)
	sweepCtx, stopSweep := context.WithCancel(ctx)
	sweepDone := make(chan error, 1)
	go func() { sweepDone <- sweeper.Run(sweepCtx) }()

	deadline = time.Now().Add(5 * time.Second)
	for {
		var doc struct {
			Status string `bson:"status"`
		}
		if err := deviceDocs.FindOne(ctx, map[string]any{"_id": "dev-e2e"}).Decode(&doc); err != nil {
			t.Fatalf("find device record: %v", err)
		}
		if doc.Status == devices.StatusOffline {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("device was never marked offline after its heartbeats stopped")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Further sweep passes must not count the same transition again.
	time.Sleep(50 * time.Millisecond)
	stopSweep()
	if err := <-sweepDone; err != nil {
		t.Errorf("sweeper Run() error = %v, want nil", err)
	}
	if got := transitions.Load(); got != 1 {
		t.Errorf("offline transitions = %d, want exactly 1", got)
	}

	cancel()
	if err := <-ingestDone; err != nil {
		t.Errorf("ingest Run() error = %v, want nil", err)
	}
}
