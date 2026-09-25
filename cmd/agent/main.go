// Command agent is the FleetOps device-agent emulator: one process runs N simulated devices as
// goroutines, each heartbeating to the control plane and applying firmware on command. Not
// implemented yet — delivered at stage 1.
package main

import (
	"errors"
	"flag"
	"log/slog"
	"os"
)

// errNotImplemented marks a stage-gated entrypoint whose wire-up does not exist yet.
var errNotImplemented = errors.New("not implemented yet")

func main() {
	controlPlane := flag.String("control-plane", "localhost:9090", "control plane gRPC address")
	fleetSize := flag.Int("fleet-size", 100, "simulated devices in this process")
	flag.Parse()

	if err := run(*controlPlane, *fleetSize); err != nil {
		slog.Error("agent stopped", "control_plane", *controlPlane, "fleet_size", *fleetSize, "err", err)
		os.Exit(1)
	}
}

// run starts fleetSize simulated device goroutines against the control plane.
func run(controlPlane string, fleetSize int) error {
	// TODO(stage 1): start fleetSize device goroutines with a ctx-bound stop condition.
	return errNotImplemented
}
