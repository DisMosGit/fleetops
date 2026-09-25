// Package agent implements the FleetOps device-agent emulator: N simulated IoT devices that
// heartbeat to the control plane over the AgentService bidirectional stream, receive commands,
// and apply firmware. Not implemented yet — the heartbeat skeleton lands at stage 1 and the
// stream client at stage 3.
package agent
