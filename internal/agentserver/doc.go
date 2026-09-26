// Package agentserver is the control-plane side of the AgentService bidirectional stream: it
// accepts one stream per agent, completes the registration exchange, routes every enrolled
// device's heartbeat to the telemetry path, and pushes commands back over the same stream.
//
// The pieces this package provides:
//
//   - Server: the gRPC AgentService implementation for Connect (Report joins with the
//     command-result stage), with the safe status mapping stream input gets;
//   - Hub: the in-memory registry of enrolled devices and their sessions — heartbeat routing
//     to the HeartbeatSink seam and command dispatch (Send) to one device's live stream;
//   - ServerOptions: the interceptor chain in its documented order (recovery, then logging;
//     metrics and tracing join at their stage) plus the keepalive policy that accepts the
//     agent's liveness pings.
//
// cmd/controlplane wires a Hub and a Server onto the gRPC listener; later stages plug the
// RabbitMQ publisher and Temporal delivery behind HeartbeatSink and Hub.Send.
package agentserver
