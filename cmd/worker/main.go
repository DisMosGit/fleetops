// Command worker runs the FleetOps Temporal workers: it hosts the DeviceWorkflow,
// RolloutWorkflow, and FirmwareWorkflow implementations together with their activities. Not
// implemented yet — delivered at stage 2.
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
	temporalAddr := flag.String("temporal-address", "localhost:7233", "Temporal frontend address")
	taskQueue := flag.String("task-queue", "fleetops", "Temporal task queue to poll")
	flag.Parse()

	if err := run(*temporalAddr, *taskQueue); err != nil {
		slog.Error("worker stopped", "temporal_addr", *temporalAddr, "task_queue", *taskQueue, "err", err)
		os.Exit(1)
	}
}

// run connects to Temporal and starts the workflow and activity workers.
func run(temporalAddr, taskQueue string) error {
	// TODO(stage 2): build the Temporal client and register workflows and activities.
	return errNotImplemented
}
