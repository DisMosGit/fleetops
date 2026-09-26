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
| `observability` | `otel_endpoint` | OTLP collector endpoint | `"localhost:4317"` |
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

#### Scenario: No config flag loads defaults only

- **WHEN** a binary starts without `-config`
- **THEN** the entire defaults table applies and no file is read

#### Scenario: Named config file is required to exist

- **WHEN** a binary starts with `-config` pointing at a path that does not exist or cannot be read
- **THEN** startup fails with an error naming the path, and no listener is started

### Requirement: Configuration validation fails fast

Loading SHALL validate the whole file — every section, even those the loading binary does not
consume — and startup SHALL fail with an error that names the offending field when any of the
following holds: the document is not valid YAML; the file contains an unknown key; `fleet_size`
or `telemetry.batch_size` is not a positive integer; a duration (`liveness.offline_threshold`,
`liveness.sweep_interval`, `telemetry.flush_interval`, `snapshots.interval`) is not a positive
Go duration; a listen or dial address (`grpc.listen_addr`, `grpc.control_plane_addr`,
`temporal.address`, `observability.metrics_addr`, `observability.health_addr`,
`observability.otel_endpoint`) is not a `host:port` pair with a numeric port; `mongodb.uri` does
not use the `mongodb://` or `mongodb+srv://` scheme; `mongodb.database`, `temporal.namespace`, or
`temporal.task_queue` is empty; `rabbitmq.url` does not use the `amqp://` or `amqps://` scheme.
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

#### Scenario: Valid partial file passes

- **WHEN** a configuration file sets only `temporal.namespace: "fleetops-dev"` and leaves the rest absent
- **THEN** validation succeeds and all other fields take their defaults

### Requirement: Entrypoints consume the shared configuration

Each entrypoint SHALL replace its hardcoded endpoint and scale defaults with values loaded from
the shared configuration file, SHALL validate the full file before starting any listener, and
SHALL pass the loaded values into its service startup path in place of any hardcoded constant.
`cmd/controlplane` SHALL consume `grpc`, `mongodb`, `rabbitmq`, `temporal`, `liveness`,
`telemetry`, `snapshots`, and `observability`; `cmd/worker` SHALL consume `mongodb`, `rabbitmq`,
`temporal`, and `observability`; `cmd/agent` SHALL consume `simulation` and `grpc`. The HTTP/SSE
gateway listen address is not part of this configuration surface — it stays stage-5 scope.

#### Scenario: Probe listener comes from configuration

- **WHEN** `cmd/controlplane` starts with a configuration setting `observability.health_addr: ":18081"`
- **THEN** its liveness/readiness listener answers on `:18081` and nothing listens on the
  default `":8081"`

#### Scenario: Agent emulator scale comes from configuration

- **WHEN** `cmd/agent` starts with a configuration setting `simulation.fleet_size: 250` and
  `grpc.control_plane_addr: "controlplane.infra:9090"`
- **THEN** its emulator startup path receives fleet size 250 and the configured control-plane
  address in place of any previously hardcoded default

#### Scenario: Device workflow cadence comes from configuration

- **WHEN** `cmd/controlplane` starts with `snapshots.interval: "30s"` and
  `liveness.offline_threshold: "2m"`
- **THEN** device workflows it starts snapshot their state at a 30-second cadence and flip their
  liveness status offline after two minutes without a heartbeat

#### Scenario: Invalid configuration starts no listener

- **WHEN** any entrypoint loads a configuration that fails validation
- **THEN** the process exits non-zero with the validation error and starts no gRPC or HTTP listener
