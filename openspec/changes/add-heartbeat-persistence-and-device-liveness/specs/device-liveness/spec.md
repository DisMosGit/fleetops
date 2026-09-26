# Spec Delta

## Purpose

Detects devices that stopped reporting: marking a device offline once no heartbeat has been
received within a configurable threshold, and exposing every such transition as a Prometheus
metric so staleness is visible while it happens.

## ADDED Requirements

### Requirement: Offline marking on heartbeat staleness
The system SHALL mark a registered device `offline` in its `devices` document when no heartbeat
has been accepted for it within the configured offline threshold — measured from the device's
last-seen timestamp — and SHALL otherwise leave the document untouched. The sweep SHALL run
continuously at the configured cadence and SHALL cover every registered device, not only those
whose stream has closed. A device still reporting heartbeats SHALL NOT be marked offline. The
last-seen timestamp SHALL NOT be changed by the offline marking.

#### Scenario: Silent device goes offline
- **WHEN** a registered device sends no heartbeat for longer than the offline threshold
- **THEN** its `devices` document has status `offline` and its last-seen timestamp is unchanged

#### Scenario: Connected but silent is still marked offline
- **WHEN** a device's stream stays open but it sends no heartbeat past the threshold
- **THEN** the device is marked offline exactly the same as one whose stream closed

#### Scenario: Reporting device stays online
- **WHEN** a device keeps sending heartbeats within the threshold
- **THEN** its status stays `online` however long it reports

#### Scenario: Marking is idempotent
- **WHEN** the sweep runs repeatedly over a device already marked offline
- **THEN** its status stays `offline` and the transition is counted once, when it happened

### Requirement: Online marking on new activity
An accepted registration or heartbeat for a device currently marked `offline` SHALL mark it
`online` again, so the recorded status follows the device's actual activity without waiting for
a threshold.

#### Scenario: Returning device goes online
- **WHEN** a device marked offline registers again or sends a heartbeat that is accepted
- **THEN** its `devices` document has status `online`

### Requirement: Configurable offline threshold
The offline threshold and the sweep cadence SHALL each be a single named configuration value
loaded from the shared configuration file, both positive durations, with defaults that make an
unchanged configuration detect a silent device within a minute. Startup SHALL fail on a
threshold or cadence that is absent of a valid duration value or not positive.

#### Scenario: Threshold drives detection
- **WHEN** the offline threshold is configured to `2m`
- **THEN** a device is marked offline once two minutes have passed with no accepted heartbeat,
  and not before

#### Scenario: Invalid threshold fails startup
- **WHEN** the configuration sets the offline threshold to zero or to an unparseable duration
- **THEN** startup fails with an error naming the offending field

### Requirement: Offline transition metric
Every online→offline transition SHALL increment a monotonic Prometheus counter named
`fleetops_device_offline_transitions_total`, exposed on the configured metrics address in the
Prometheus exposition format. The counter SHALL increment exactly once per transition — not
once per sweep over an offline device — and SHALL NOT increment for devices that stay offline
or stay online.

#### Scenario: Transition is counted once
- **WHEN** a device crosses the threshold and is marked offline
- **THEN** `fleetops_device_offline_transitions_total` has increased by exactly one

#### Scenario: Steady offline device adds nothing
- **WHEN** the sweep keeps running over a device already offline
- **THEN** the counter does not increase

#### Scenario: Metric is scrapeable
- **WHEN** a Prometheus scraper reads the configured metrics address
- **THEN** the response includes `fleetops_device_offline_transitions_total` in the exposition
  format with its current value
