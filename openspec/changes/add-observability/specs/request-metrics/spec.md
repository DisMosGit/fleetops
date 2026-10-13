# Spec Delta

## Purpose

Gives every process a request-level view of the traffic it handles — how much, how fast, how often
it fails, and how many connections are open — on the metrics surface each binary is already
configured for.

## ADDED Requirements

### Requirement: The control plane's gRPC traffic is measured

The control plane SHALL expose, for the gRPC streams it serves, a counter of requests by method and
outcome, a histogram of request duration by method, and a gauge of the connections currently open.
Outcome SHALL distinguish success from failure and SHALL NOT use the raw error text as a label, so
cardinality stays bounded.

#### Scenario: Served streams are counted by method
- **WHEN** the control plane serves streams for `AgentService.Connect`
- **THEN** that method's request counter increases once per stream

#### Scenario: Failures are counted separately from successes
- **WHEN** a served stream ends with a status error
- **THEN** the failure outcome is counted and the success outcome is not

#### Scenario: Duration is observable
- **WHEN** streams of varying length are served
- **THEN** the duration histogram records them, so a percentile can be computed over a window

#### Scenario: Open connections are counted
- **WHEN** agents connect and disconnect
- **THEN** the connection gauge rises and falls with the streams actually open, returning to the true count rather than only ever increasing

### Requirement: The agent's outbound traffic is measured

The agent emulator SHALL expose a counter of the calls it makes by method and outcome, a histogram
of their duration, and a gauge of its connections, including how often it has lost and re-established
its stream, so a fleet whose agents are flapping is visible from the agents' side.

#### Scenario: The agent's calls are counted
- **WHEN** the agent emulator sends heartbeats over its stream
- **THEN** its request counter reflects those calls by method

#### Scenario: Reconnects are counted
- **WHEN** the agent's stream is lost and re-established
- **THEN** the reconnect is counted, so repeated stream loss is distinguishable from a steady connection

#### Scenario: The agent's open connections are visible
- **WHEN** the agent emulator holds a stream open
- **THEN** its connection gauge reports one open connection

### Requirement: The worker exposes a metrics surface

The worker SHALL serve Prometheus exposition on the metrics address it is configured with, and SHALL
report its activity execution in it: a counter of executions by activity type and outcome, and a
histogram of execution duration by activity type. The worker's surface SHALL exist for the same
reason the control plane's does — a canary that is stalled is diagnosable from the worker that is
supposed to be advancing it.

#### Scenario: The worker serves metrics
- **WHEN** the worker is running with a configured metrics address
- **THEN** a scrape of that address returns Prometheus exposition

#### Scenario: Activity executions are counted
- **WHEN** the worker executes rollout activities
- **THEN** the execution counter increases by activity type, and a failed execution is counted as a failure rather than a success

#### Scenario: Activity duration is observable
- **WHEN** activities of varying duration run
- **THEN** the duration histogram records them by activity type

#### Scenario: The worker's metrics address is the configured one
- **WHEN** the worker starts with a configuration setting a non-default metrics address
- **THEN** exposition is served there and nothing listens on the default address

### Requirement: Metrics share one namespace and are registered before serving

Every metric added by this capability SHALL be in the `fleetops_` namespace alongside the metrics
that already exist, SHALL be registered before its metrics endpoint begins serving, and SHALL NOT
reuse or redefine a metric name another capability already owns.

#### Scenario: Names follow the project namespace
- **WHEN** the exposition of any binary is inspected
- **THEN** every metric added by this capability is prefixed `fleetops_`

#### Scenario: No scrape misses a metric
- **WHEN** a metrics endpoint is scraped immediately after a process starts serving
- **THEN** the metrics this capability adds are already present

#### Scenario: Existing metrics are not redefined
- **WHEN** the exposition is compared with the pipeline metrics that existed before this capability
- **THEN** the pipeline metric names and their meanings are unchanged

#### Scenario: Cardinality stays bounded
- **WHEN** many distinct requests are served, including requests that fail with distinct error text
- **THEN** the label sets added by this capability do not grow with the number of distinct error messages

### Requirement: Request metrics never gate traffic

Recording request metrics SHALL NOT fail, delay, or reject a request. A metrics registration or
recording failure SHALL leave the request's own outcome unchanged.

#### Scenario: A request succeeds while metrics fail
- **WHEN** a metrics recording fails while a gRPC stream is served
- **THEN** the stream is served with its own result

#### Scenario: Serving does not wait on metrics
- **WHEN** requests are served while the metrics endpoint is being scraped
- **THEN** request latency is not determined by scrape latency

### Requirement: The metrics address is honored by every process that serves one

Each process that exposes metrics SHALL listen on the address configured by
`observability.metrics_addr`, and SHALL fail startup for an invalid address as the configuration
contract already requires, so a deployment can place the surfaces where it wants them.

#### Scenario: A configured address is used
- **WHEN** a binary starts with `observability.metrics_addr` set to a non-default value
- **THEN** its metrics are served on that address

#### Scenario: An invalid address fails startup
- **WHEN** `observability.metrics_addr` is not a valid `host:port` pair
- **THEN** startup fails naming the offending field and no listener is started
