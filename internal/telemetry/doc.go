// Package telemetry implements the FleetOps telemetry pipeline over RabbitMQ: the heartbeat
// publisher, the idempotent consumers fanning out to analytics, alerting, and rollout health,
// and DLQ handling for poison messages. Not implemented yet — delivered at stage 3.
package telemetry
