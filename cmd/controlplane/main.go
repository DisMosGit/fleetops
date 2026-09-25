// Command controlplane is the FleetOps control plane: the gRPC agent server and the HTTP/SSE
// gateway. Not implemented yet — delivered at stage 1.
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
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address for AgentService")
	httpAddr := flag.String("http-addr", ":8080", "HTTP gateway listen address")
	flag.Parse()

	if err := run(*grpcAddr, *httpAddr); err != nil {
		slog.Error("controlplane stopped", "grpc_addr", *grpcAddr, "http_addr", *httpAddr, "err", err)
		os.Exit(1)
	}
}

// run wires the control-plane dependencies and serves the listeners.
func run(grpcAddr, httpAddr string) error {
	// TODO(stage 1): construct MongoDB, Temporal, and RabbitMQ clients and serve the listeners.
	return errNotImplemented
}
