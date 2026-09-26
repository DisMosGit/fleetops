// Package agent implements the FleetOps device-agent emulator: N simulated IoT devices that
// heartbeat to the control plane over the AgentService bidirectional stream, receive commands,
// and (at their stage) apply firmware.
//
// The pieces this package provides:
//
//   - the simulated fleet (Fleet, Device): one goroutine per device, each emitting a periodic
//     heartbeat with event id, device id, current firmware, status, timestamp, and cpu / mem /
//     health samples produced by an injectable simulation Source;
//   - the stream client (Client): one long-lived AgentService.Connect stream per emulator
//     process multiplexing every device, registering them before heartbeats flow, sending under
//     backpressure through bounded queues, and reconnecting with capped exponential backoff
//     without losing an accepted message;
//   - Dial: the control-plane connection carrying the keepalive parameters that turn a silently
//     dead connection into a reconnect instead of an invisible stall.
//
// cmd/agent wires these together; commands land on the CommandHandler seam until firmware
// apply arrives with its stage.
package agent
