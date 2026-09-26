// Package telemetry implements the FleetOps telemetry pipeline: the batched, idempotent
// heartbeat ingestion into the time-series `telemetry` collection that the gRPC stream feeds
// today, and — at stage 3 — the RabbitMQ publisher, the idempotent consumers fanning out to
// analytics, alerting, and rollout health, and DLQ handling for poison messages.
package telemetry
