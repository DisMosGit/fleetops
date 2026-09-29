# Notes

Implementation notes for `add-telemetry-event-pipeline`: what landed, how it was verified, and the
one archive-order constraint this change inherits.

## What landed

- **Config**: `rabbitmq.{prefetch,publish_buffer,max_attempts,retry_base,retry_max,queue_depth_interval}`
  and a new `alerting.health_threshold`, with defaults, validation, and the documented sample in
  `deploy/config.yaml`.
- **Data model**: `processed_events` (unique `(consumer, event_id)`, TTL on `claimed_at` from
  `FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS`) and `device_alerts` (one document per device, no
  TTL), in `deploy/mongo/init.js`, `deploy/mongo/verify.sh`, and `deploy/README.md`.
- **Pipeline**: `internal/telemetry` gains `topology.go` (declared layout), `broker.go` (supervised
  connection with capped reconnect backoff and loud topology conflicts), `confirm.go` (publish with
  confirms, returns, and settlement), `envelope.go` (versioned JSON contract),
  `publisher.go` + `fanout.go` (publication beside the durable ingest write), `dedup.go`,
  `alerting.go`, `consumer.go`, `metrics.go`, and `sampler.go`.
- **Wiring**: `cmd/controlplane` builds the pipeline from configuration (`newPipeline`), routes the
  agent hub's heartbeats through the ordered fan-out, and runs the publisher, the alerting
  consumer, and the queue-depth sampler in its errgroup.
- **Docs**: `docs/telemetry.md` (layout, envelope, consumer semantics, metric catalogue, operator
  notes); the `docs/README.md` backlog row is marked written.
- **Tests**: `internal/rabbittest` (RabbitMQ container harness with a host port that survives a
  restart), unit tests for every decision (topology layout and ladder, envelope, publisher,
  fan-out, consumer, dedup states, alerting operators, lag, metrics, sampler), and integration
  tests for everything that is genuinely broker or store behavior (topology contract, retry
  ladder, dead-letter path, reconnection across a broker restart, ledger uniqueness, alert upsert,
  consumer retry/DLQ flow, dead-letter depth sampling, and the wired pipeline end to end).

## Verification evidence

- `goimports -w .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...` — the
  Definition of Done bar.
- `go test -race -tags integration -count=1 ./...` — the integration bar (Docker required).
- `deploy/mongo/verify.sh` against a container bootstrapped from `deploy/mongo/init.js`: every
  check passes, including the new collections, the unique ledger index, both TTL indexes, and the
  ledger's duplicate refusal.

## Archive order

This change MODIFIES `runtime-config` and `mongo-data-model`, whose main specs are created by
older, still-unarchived changes. `openspec validate add-telemetry-event-pipeline` passes, but
`openspec archive` would refuse this delta with "target spec does not exist" until those changes
are archived first (or together):

1. `add-runtime-config-and-health-endpoints` and `add-repo-scaffold-and-data-model` (the base
   specs), then
2. the changes that later modified those requirements —
   `add-heartbeat-persistence-and-device-liveness`,
   `add-device-search-attributes-snapshot-and-worker`, `add-firmware-pipeline` —
3. then this change.

The same constraint was recorded by the heartbeat-persistence change; `design.md`'s Migration Plan
carries it too.
