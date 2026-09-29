// Package telemetry implements the FleetOps telemetry pipeline: the batched, idempotent heartbeat
// ingestion into the `telemetry` collection, the RabbitMQ event fan-out published beside it, and
// the consumers that act on those events.
//
// The pipeline's shape is:
//
//   - Writer stores accepted heartbeats in batches, and is the source of truth for them.
//   - Fanout calls the Writer first and the Publisher second, so a heartbeat storage refused is
//     never published and broker trouble never fails heartbeat acceptance.
//   - Publisher declares the broker topology and publishes persistent, versioned events with
//     publisher confirms, dropping only with a counter.
//   - Consumer consumes one work queue, deduplicating by (consumer, event id) against the
//     processed_events ledger, acknowledging only after its side effect is durable, and driving
//     failures through a per-attempt retry ladder into a dead-letter queue.
//   - Alerting is the heartbeat consumer's side effect: one durable alert per degraded device.
//   - QueueSampler keeps the queue-depth metric current.
//
// Analytics and rollout-health consumers are not implemented yet; they are one work-queue binding
// and one Handler away. The layout, envelope, and consumer semantics are documented in
// docs/telemetry.md.
package telemetry
