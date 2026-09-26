// Command agent is the FleetOps device-agent emulator: one process runs N simulated devices as
// goroutines, each heartbeating to the control plane and applying firmware on command. It loads
// the shared configuration today; the emulation itself joins with the agent stage.
package main

import (
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/DisMosGit/fleetops/internal/config"
)

// errNotImplemented marks a stage-gated entrypoint whose wire-up does not exist yet.
var errNotImplemented = errors.New("not implemented yet")

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

	if err := run(cfg.GRPC.ControlPlaneAddr, cfg.Simulation.FleetSize); err != nil {
		slog.Error("agent stopped", "err", err)
		os.Exit(1)
	}
}

// run starts fleetSize simulated device goroutines against the control plane.
func run(controlPlane string, fleetSize int) error {
	slog.Info("emulator configured", "control_plane", controlPlane, "fleet_size", fleetSize)
	// TODO(stage 1): start fleetSize device goroutines with a ctx-bound stop condition.
	return errNotImplemented
}
