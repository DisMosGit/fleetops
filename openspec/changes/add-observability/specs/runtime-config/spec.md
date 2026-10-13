# Spec Delta

## MODIFIED Requirements

### Requirement: Single YAML configuration file

The system SHALL load all service configuration from one YAML file selected by the entrypoint's
`-config` flag. The file SHALL contain these sections and fields:

| Section | Field | Meaning | Default |
|---|---|---|---|
| `simulation` | `fleet_size` | simulated devices the agent emulator runs | `100` |
| `grpc` | `listen_addr` | control plane `AgentService` listen address | `":9090"` |
| `grpc` | `control_plane_addr` | control plane gRPC address the agent connects to | `"localhost:9090"` |
| `mongodb` | `uri` | MongoDB connection URI | `"mongodb://localhost:27017"` |
| `mongodb` | `database` | fleet database name | `"fleetops"` |
| `liveness` | `offline_threshold` | silence after which a device is marked offline | `"30s"` |
| `liveness` | `sweep_interval` | cadence of the offline-staleness sweep | `"10s"` |
| `telemetry` | `batch_size` | heartbeat events per batched telemetry write | `500` |
| `telemetry` | `flush_interval` | longest time a heartbeat waits for its write batch | `"1s"` |
| `snapshots` | `interval` | cadence of periodic device-workflow state snapshots | `"1m"` |
| `rabbitmq` | `url` | RabbitMQ connection URL | `"amqp://guest:guest@localhost:5672/"` |
| `temporal` | `address` | Temporal frontend address | `"localhost:7233"` |
| `temporal` | `namespace` | Temporal namespace | `"default"` |
| `temporal` | `task_queue` | task queue the workers poll | `"fleetops"` |
| `temporal` | `dispatch_task_queue` | task queue the control plane polls for device command dispatch | `"fleetops-controlplane"` |
| `observability` | `otel_endpoint` | OTLP collector endpoint | `"localhost:4317"` |
| `observability` | `otel_sample_ratio` | share of newly rooted traces recorded, parent-based | `0.1` |
| `observability` | `metrics_addr` | Prometheus metrics listen address | `":9091"` |
| `observability` | `health_addr` | liveness/readiness listen address | `":8081"` |

The values above are the defaults applied when a field (or a whole section) is absent, so an
empty file yields a working local-stack configuration. Duration fields are Go duration strings
(as in `"30s"`).

#### Scenario: Absent fields take defaults

- **WHEN** a binary loads a configuration file that omits `simulation`, `grpc`, and `observability`
- **THEN** the loaded configuration carries `fleet_size` 100, `grpc.listen_addr` `":9090"`, and
  `observability.health_addr` `":8081"` from the defaults table

#### Scenario: Present fields override defaults

- **WHEN** a binary loads a configuration file with `simulation.fleet_size: 500` and
  `mongodb.uri: "mongodb://mongo.infra:27017"`
- **THEN** the loaded configuration carries `fleet_size` 500 and the given URI, and every other
  field keeps its default

#### Scenario: Liveness, telemetry, and snapshots sections default

- **WHEN** a binary loads a configuration file that omits `liveness`, `telemetry`, and `snapshots`
- **THEN** the loaded configuration carries `liveness.offline_threshold` `"30s"`,
  `liveness.sweep_interval` `"10s"`, `telemetry.batch_size` `500`,
  `telemetry.flush_interval` `"1s"`, and `snapshots.interval` `"1m"`

#### Scenario: The dispatch task queue defaults to its own queue

- **WHEN** a binary loads a configuration file that omits `temporal.dispatch_task_queue`
- **THEN** the loaded configuration carries `temporal.task_queue` `"fleetops"` and
  `temporal.dispatch_task_queue` `"fleetops-controlplane"`, so the control plane's dispatch work
  is not placed on the queue the workers poll

#### Scenario: No config flag loads defaults only

- **WHEN** a binary starts without `-config`
- **THEN** the entire defaults table applies and no file is read

#### Scenario: Named config file is required to exist

- **WHEN** a binary starts with `-config` pointing at a path that does not exist or cannot be read
- **THEN** startup fails with an error naming the path, and no listener is started

#### Scenario: Broker and alerting sections default

- **WHEN** a binary loads a configuration file that omits `rabbitmq` and `alerting`
- **THEN** the loaded configuration carries `rabbitmq.prefetch` 32, `rabbitmq.publish_buffer`
  1024, `rabbitmq.max_attempts` 3, `rabbitmq.retry_base` `"5s"`, `rabbitmq.retry_max` `"1m"`,
  `rabbitmq.queue_depth_interval` `"15s"`, and `alerting.health_threshold` 0.6

#### Scenario: Rollout gating section defaults

- **WHEN** a binary loads a configuration file that omits `rollout`
- **THEN** the loaded configuration carries `rollout.health_window` `"5m"`,
  `rollout.sample_health_threshold` 0.6, `rollout.min_success_ratio` 0.95,
  `rollout.min_samples` 10, `rollout.decision_timeout` `"30m"`, and
  `rollout.result_timeout` `"5m"`

#### Scenario: Rollout wave sequence defaults

- **WHEN** a binary loads a configuration file that omits `rollout.waves`
- **THEN** the loaded sequence is four waves — 1% and 5% without approval, then 25% and 100% each
  requiring operator approval — in that order

#### Scenario: A configured wave sequence overrides the default

- **WHEN** a configuration file sets `rollout.waves` to a 10% wave and a 100% wave, both without
  approval
- **THEN** loaded rollouts drive exactly those two waves, in that order, and neither waits for
  approval

#### Scenario: Liveness and telemetry sections default

- **WHEN** a binary loads a configuration file that omits `liveness` and `telemetry`
- **THEN** the loaded configuration carries `liveness.offline_threshold` `"30s"`,
  `liveness.sweep_interval` `"10s"`, `telemetry.batch_size` `500`, and
  `telemetry.flush_interval` `"1s"`

#### Scenario: Observability sections default

- **WHEN** a binary loads a configuration file that omits `observability`
- **THEN** the loaded configuration carries `observability.otel_endpoint` `"localhost:4317"`,
  `observability.otel_sample_ratio` `0.1`, `observability.metrics_addr` `":9091"`, and
  `observability.health_addr` `":8081"`

### Requirement: Configuration validation fails fast

Loading SHALL validate the whole file — every section, even those the loading binary does not
consume — and startup SHALL fail with an error that names the offending field when any of the
following holds: the document is not valid YAML; the file contains an unknown key; `fleet_size`
or `telemetry.batch_size` is not a positive integer; a duration (`liveness.offline_threshold`,
`liveness.sweep_interval`, `telemetry.flush_interval`, `snapshots.interval`) is not a positive
Go duration; a listen or dial address (`grpc.listen_addr`, `grpc.control_plane_addr`,
`temporal.address`, `observability.metrics_addr`, `observability.health_addr`,
`observability.otel_endpoint`) is not a `host:port` pair with a numeric port; `mongodb.uri` does
not use the `mongodb://` or `mongodb+srv://` scheme; `mongodb.database`, `temporal.namespace`,
`temporal.task_queue`, or `temporal.dispatch_task_queue` is empty; `rabbitmq.url` does not use the
`amqp://` or `amqps://` scheme; or `observability.otel_sample_ratio` is below `0.0` or above `1.0`.
An empty `observability.otel_endpoint` is valid and means tracing export is disabled.

#### Scenario: Unknown key is rejected

- **WHEN** a configuration file contains a key that is not part of the schema (for example `grpc.port`)
- **THEN** startup fails and the error names the unknown key

#### Scenario: Malformed value is rejected

- **WHEN** `mongodb.uri` is `"localhost:27017"` (no scheme) or `simulation.fleet_size` is `0`
- **THEN** startup fails and the error names the offending field

#### Scenario: Invalid duration or scale value is rejected

- **WHEN** `liveness.offline_threshold` is `"0s"`, `snapshots.interval` is `"soon"`,
  `telemetry.flush_interval` is `"soon"`, or `telemetry.batch_size` is `-1`
- **THEN** startup fails and the error names the offending field

#### Scenario: An empty queue name is rejected

- **WHEN** `temporal.task_queue` or `temporal.dispatch_task_queue` is the empty string
- **THEN** startup fails and the error names the empty field

#### Scenario: Valid partial file passes

- **WHEN** a configuration file sets only `temporal.namespace: "fleetops-dev"` and leaves the rest absent
- **THEN** validation succeeds and all other fields take their defaults

#### Scenario: Invalid broker pipeline value is rejected

- **WHEN** `rabbitmq.prefetch` is `0`, `rabbitmq.max_attempts` is `-1`, `rabbitmq.retry_base` is
  `"soon"`, `rabbitmq.retry_max` is shorter than `rabbitmq.retry_base`, or
  `alerting.health_threshold` is `1.5`
- **THEN** startup fails and the error names the offending field

#### Scenario: Invalid rollout gating value is rejected

- **WHEN** `rollout.health_window` is `"soon"`, `rollout.min_samples` is `0`,
  `rollout.sample_health_threshold` is `1.5`, `rollout.min_success_ratio` is negative, or
  `rollout.decision_timeout` is shorter than `rollout.health_window`
- **THEN** startup fails and the error names the offending field

#### Scenario: Invalid rollout result timeout is rejected

- **WHEN** `rollout.result_timeout` is `"0s"` or `"soon"`, or is longer than
  `rollout.decision_timeout`
- **THEN** startup fails and the error names `rollout.result_timeout`

#### Scenario: Invalid rollout wave sequence is rejected

- **WHEN** `rollout.waves` is empty, holds a percentage of `0` or above `100`, repeats or descends
  from one entry to the next, or ends at a percentage below `100`
- **THEN** startup fails and the error names `rollout.waves`

#### Scenario: Invalid liveness or telemetry value is rejected

- **WHEN** `liveness.offline_threshold` is `"0s"`, `telemetry.flush_interval` is `"soon"`, or
  `telemetry.batch_size` is `-1`
- **THEN** startup fails and the error names the offending field

#### Scenario: Invalid sample ratio is rejected

- **WHEN** `observability.otel_sample_ratio` is below `0.0` (for example `-0.1`) or above `1.0`
  (for example `1.5`)
- **THEN** startup fails and the error names `observability.otel_sample_ratio`

#### Scenario: The sample ratio bounds are valid values

- **WHEN** `observability.otel_sample_ratio` is `0.0` or `1.0`
- **THEN** validation succeeds, and the loaded configuration carries the given ratio
