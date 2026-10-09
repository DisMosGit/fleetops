# Tasks

## 1. Configuration and data-model groundwork

- [x] 1.1 Add `github.com/rabbitmq/amqp091-go` to `go.mod` (one-line justification for the PR
      description: the AMQP 0-9-1 client the broker topology is declared and consumed through) and
      verify `go mod tidy` plus `go build ./...` succeed
- [x] 1.2 Extend `internal/config`'s `RabbitMQ` with `prefetch` (32), `publish_buffer` (1024),
      `max_attempts` (3), `retry_base` (`"5s"`), `retry_max` (`"1m"`), `queue_depth_interval`
      (`"15s"`) and add an `Alerting` section with `health_threshold` (0.6), populated in
      `Defaults()` and decoded strictly like the existing fields; verify table-driven tests in
      `internal/config` assert every new default for an absent field
- [x] 1.3 Extend `Config.Validate` with positive-integer checks for the four counts, positive-duration
      checks for `rabbitmq.retry_base`/`retry_max`/`queue_depth_interval`, a `retry_max >= retry_base`
      check, and a `[0, 1]` check for `alerting.health_threshold`, each error naming its field; verify
      new subtests (`prefetch: 0`, `max_attempts: -1`, `retry_base: "soon"`, `retry_max` below
      `retry_base`, `health_threshold: 1.5`) fail with the field name and `go test ./internal/config/...`
      stays green
- [x] 1.4 Document the seven keys in `deploy/config.yaml` beside `rabbitmq` and the new `alerting`
      section, and verify a `config.Load("deploy/config.yaml")` assertion in `internal/config` shows
      the documented values parse and validate
- [x] 1.5 Declare the `processed_events` and `device_alerts` collections in `deploy/mongo/init.js`
      with their validators, the unique (`consumer`, `event_id`) index, the `device_id` and
      `region` + `model` indexes, and the ledger's TTL on `claimed_at` driven by a named
      `FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS` value (default 7); verify `deploy/mongo/verify.sh`
      checks both collections and `go test -tags integration ./internal/mongotest/...` passes with
      schema subtests for the required fields, the unique index, and the TTL index

## 2. Broker topology and connection supervision

- [x] 2.1 Define the topology in `internal/telemetry` (new `topology.go`): the three exchanges, the
      two work queues with their `.retry.<n>` and `.dlq` names, the bindings that implement the
      routing-key grammar and the `retry.<work queue>` return path, the per-attempt backoff ladder
      `min(retry_base × 2^(n-1), retry_max)`, and an idempotent `Declare` over a channel; verify
      table-driven unit tests assert every declared name, argument (durable, TTL, dead-letter
      exchange and routing key) and the ladder for `max_attempts` 1, 3, and 5
- [x] 2.2 Build the `//go:build integration` RabbitMQ harness in `internal/rabbittest` with
      `testcontainers-go` (boots a broker, waits for AMQP readiness, returns a channel and a
      per-test cleanup) and verify `go test -tags integration ./internal/rabbittest/...` boots the
      container and a trivial declare/publish/consume round trip passes
- [x] 2.3 Implement the supervised broker connection (`internal/telemetry`, new `broker.go`): dial,
      declare the topology, hand the channel to the caller, and on connection or channel loss back
      off with the doubling-capped schedule and re-enter until `ctx` is done; verify unit tests
      assert the backoff schedule and that a lost channel triggers a re-dial, plus an integration
      test that restarts the broker container and observes the connection re-established with the
      topology re-declared
- [x] 2.4 Verify the topology contract against a real broker: declaring twice leaves waiting messages
      untouched, an event published under `heartbeat.<region>.<model>` reaches only the heartbeat
      work queue, a narrow `heartbeat.<region>.#` binding is honoured, and a conflicting pre-existing
      element makes the declaration fail with an error naming it

## 3. Heartbeat publication alongside durable ingest

- [x] 3.1 Implement the event envelope (`internal/telemetry`, new `envelope.go`): the versioned
      struct, JSON encode/decode, message-property construction (persistent, `application/json`,
      `event_id`, `event_type`, `schema_version`, `attempt`, original routing key), and validation
      that rejects a missing event id, an unknown schema version, and a missing measurement time;
      verify table-driven unit tests cover a full round trip, each rejection reason, and that
      decoding never needs the gRPC messages
- [x] 3.2 Implement the publisher (`internal/telemetry`, new `publisher.go`): a bounded buffer fed by
      `Handle` (which never blocks and never fails on broker trouble), a publisher goroutine that
      owns the connection, stamps a per-event-type monotonic `sequence`, publishes `mandatory` with
      confirms, drains returned unroutable messages, counts published/dropped/failed events, and logs
      the first drop of a burst; verify unit tests with a fake channel interface assert the sequence
      ordering, the drop-on-full-buffer path, the nack and returned-message failure paths, and that
      `Handle` returns promptly while the broker is unavailable
- [x] 3.3 Add the ordered fan-out sink (`internal/telemetry`, new `fanout.go`) that calls the ingest
      writer first and the publisher second, returning the ingest error and publishing nothing when
      ingest refuses the heartbeat; verify unit tests with fake sinks assert the call order, that a
      refused heartbeat is never published, and that a publication drop does not fail the heartbeat
- [x] 3.4 Wire the publisher, the fan-out, and its lifecycle into `cmd/controlplane`'s errgroup and
      replace the bare ingest sink passed to `agentserver.NewHub`; verify `go build ./cmd/controlplane`
      and `go test ./cmd/controlplane/...` pass, with a test asserting the hub's sink is the fan-out
- [x] 3.5 Verify publication against a real broker end to end: an accepted heartbeat yields one
      persistent event on the heartbeat work queue with the expected envelope, properties, and
      routing key, an unroutable routing key is returned and counted, and events keep flowing after
      the broker restarts

## 4. Idempotent consumption and alerting

- [x] 4.1 Implement the dedup ledger (`internal/telemetry`, new `dedup.go`): claim a
      (`consumer`, `event_id`) document, mark it processed, distinguish a duplicate-key refusal from
      every other write error, and report whether an existing record is processed or unfinished;
      verify integration tests against the harness’s Mongo assert one document per pair, a second
      claim refused as a duplicate rather than an error, and `processed_at` absent until marked
- [x] 4.2 Implement the alerting handler (`internal/telemetry`, new `alerting.go`): reject heartbeats
      at or above `alerting.health_threshold`, and upsert one `device_alerts` document per device with
      `$min` `first_seen_at`/`min_health`, `$max` `last_seen_at`, and `$set` identity plus threshold;
      verify unit tests cover below/at/above threshold and integration tests assert one document per
      device, a widening window with a non-increasing minimum, no write for a healthy heartbeat, and
      an identical document after replaying the same event
- [x] 4.3 Implement the consumer (`internal/telemetry`, new `consumer.go`): consume with
      `rabbitmq.prefetch` bounds, decode and dead-letter an unusable event without burning attempts,
      claim-then-apply-then-complete, acknowledge only after the side effect is durable, republish a
      failed delivery to its attempt's retry queue (`<work queue>.retry.<attempt>`) with the attempt
      header incremented to the next attempt and acknowledge only on confirm, publish a final failure to the dead-letter exchange with reason and
      attempt count, and reject without requeue when the republish itself fails; verify unit tests
      with fake deliveries, ledger, and publisher cover every one of those decisions, including the
      duplicate ack and the unfinished-claim resume
- [x] 4.4 Verify the consumer against a real broker: a degraded heartbeat produces the alert and one
      processed ledger document; redelivering the same event changes nothing and counts a duplicate;
      a handler that fails twice then succeeds returns through the retry queues with the configured
      delays; a handler that always fails lands on `<work queue>.dlq` with its reason and attempt
      count; a malformed payload and an unsupported schema version are dead-lettered with no retry

## 5. Queue, lag, and outcome metrics plus operator documentation

- [x] 5.1 Add the pipeline collectors (`internal/telemetry`, new `metrics.go`):
      `fleetops_queue_depth{queue,kind}`, `fleetops_consumer_lag_events{consumer}`,
      `fleetops_events_published_total{event_type}`, `fleetops_events_dropped_total{event_type}`,
      and `fleetops_events_consumed_total{consumer,outcome}`, registered on the registry the control
      plane already serves; verify unit tests assert the label values, that lag is zero on catch-up,
      never negative, and reported per consumer, and that every outcome carries a source
- [x] 5.2 Implement the queue-depth sampler (passive queue declare at
      `rabbitmq.queue_depth_interval`, ctx-owned stop condition, failure logged without publishing a
      fabricated zero) and verify a unit test with a fake inspector covers the sampled values and the
      unavailable-broker path, plus an integration test that dead-letters a message and observes the
      `kind="dead_letter"` depth rise
- [x] 5.3 Wire the collectors, the sampler, and the alerting consumer into `cmd/controlplane`'s
      errgroup (`ctx`-tied stop conditions, prefetch and attempt settings from configuration) and
      verify a test scrapes the metrics handler and finds every metric family added by this change
- [x] 5.4 Write `docs/telemetry.md` — the exchange/queue layout with the routing-key grammar, the
      envelope with its versioning rule, consumer semantics (dedup ledger, retry ladder, dead-letter
      path), the metric catalogue with what each one answers, and the operational notes (dropped
      events, DLQ inspection, retention window) — and verify every name and metric in it matches the
      implementation, with the `docs/telemetry.md` backlog row in `docs/README.md` marked written

## 6. End-to-end verification

- [x] 6.1 Add the integration test spanning the path — in-process gRPC stream: an accepted heartbeat
      is persisted to `telemetry`, published to the broker, consumed by the alerting consumer into
      `device_alerts` with exactly one processed-events document, the queue-depth and lag metrics
      reflect the pipeline, and a broker outage during the run only raises the drop counter while
      heartbeats keep being persisted — and verify it passes with `-tags integration`
- [x] 6.2 Run the definition of done (`goimports -w .`, `go vet ./...`, `golangci-lint run`,
      `go test -race -count=1 ./...`) and verify all four pass; run
      `go test -race -tags integration -count=1 ./...` as the integration bar
- [x] 6.3 Verify the archive prerequisite is recorded: `runtime-config` and `mongo-data-model` are
      created by older, still-unarchived changes, so confirm `openspec validate
      add-telemetry-event-pipeline` passes and note the archive order in the change notes
