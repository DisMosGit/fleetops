// Command agent is the FleetOps device-agent emulator: one process runs N simulated devices as
// goroutines, each heartbeating to the control plane over the AgentService bidirectional
// stream, receiving commands, and (at their stage) applying firmware. It loads the shared
// configuration and runs the fleet until shutdown.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
	"github.com/DisMosGit/fleetops/internal/agent"
	"github.com/DisMosGit/fleetops/internal/config"
)

func main() {
	configPath := flag.String(
		"config", "", "path to the YAML configuration file (defaults apply when omitted)",
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("agent stopped", "err", err)
		os.Exit(1)
	}
}

// run runs the simulated fleet against the control plane until ctx is cancelled: the fleet
// emits heartbeats into the stream client, which delivers them over one multiplexed Connect
// stream and reconnects with backoff when it breaks.
func run(ctx context.Context, cfg config.Config) error {
	seed := time.Now().UnixNano()
	slog.Info("starting emulator",
		"control_plane", cfg.GRPC.ControlPlaneAddr,
		"fleet_size", cfg.Simulation.FleetSize,
		"simulation_seed", seed,
	)

	conn, err := agent.Dial(cfg.GRPC.ControlPlaneAddr)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("close control-plane connection", "err", err)
		}
	}()

	ids, err := agent.NewIDGen()
	if err != nil {
		return fmt.Errorf("id generator: %w", err)
	}
	fleet, err := agent.NewFleet(cfg.Simulation.FleetSize, agent.FleetOptions{
		IDs:    ids,
		Source: agent.NewSimulation(rand.NewSource(seed)),
		Period: agent.DefaultPeriod,
	})
	if err != nil {
		return fmt.Errorf("build fleet: %w", err)
	}
	client, err := agent.NewClient(agentv1.NewAgentServiceClient(conn), agent.Options{
		Devices: fleet.Identities(),
		Handler: logHandler{},
		IDs:     ids,
	})
	if err != nil {
		return fmt.Errorf("build stream client: %w", err)
	}

	// One bounded channel of heartbeats from the fleet to the client: when the client is
	// disconnected, the fleet waits here instead of buffering without limit.
	heartbeats := make(chan *agentv1.Heartbeat, cfg.Simulation.FleetSize)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return client.Run(gctx, heartbeats) })
	g.Go(func() error { return fleet.Run(gctx, heartbeats) })
	if err := g.Wait(); err != nil {
		return fmt.Errorf("run emulator: %w", err)
	}
	slog.Info("emulator stopped")
	return nil
}

// logHandler is the CommandHandler seam until firmware apply lands: a dispatched command is
// acknowledged at the log boundary and otherwise dropped, because nothing can act on it yet.
type logHandler struct{}

// Handle logs one command received over the stream.
func (logHandler) Handle(_ context.Context, cmd *agentv1.Command) error {
	slog.Info("command received",
		"command_id", cmd.GetCommandId(), "device_id", cmd.GetDeviceId())
	return nil
}
