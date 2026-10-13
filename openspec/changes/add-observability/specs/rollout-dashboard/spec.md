# Spec Delta

## Purpose

Gives an operator watching a canary one screen that answers what a rollout is doing: how far each
active rollout has progressed, whether the fleet it is updating is healthy, and whether anything is
rolling back.

## ADDED Requirements

### Requirement: The dashboard shows wave progress across active rollouts

The dashboard SHALL show, for the rollouts currently active, how far each has progressed through
its configured wave sequence, so the state of every in-flight canary is readable at a glance rather
than one rollout per query.

#### Scenario: Active rollouts are listed with their progress
- **WHEN** several rollouts are in flight at different waves
- **THEN** the dashboard shows each one's progress rather than a single aggregate that hides which rollout is where

#### Scenario: A rollout that concludes leaves the progress view
- **WHEN** a rollout reaches a terminal status
- **THEN** it stops being shown as an active rollout in progress

#### Scenario: A stalled rollout is visually distinguishable
- **WHEN** a rollout has not advanced for longer than its wave's expected duration
- **THEN** the dashboard makes the lack of progress visible rather than rendering it identically to a healthy one

### Requirement: The dashboard shows health trends

The dashboard SHALL show wave health over time — the measured success ratio and the evidence behind
it — so a trend is visible before a boundary is crossed and a wave that is healthy on thin evidence
is distinguishable from one that is healthy on real evidence.

#### Scenario: Health is shown as a trend, not a single value
- **WHEN** a wave's success ratio changes over its health window
- **THEN** the dashboard shows the change over time

#### Scenario: Evidence size is visible with the ratio
- **WHEN** a wave reports a high ratio on very few samples
- **THEN** the dashboard shows the sample size alongside the ratio, so thin evidence is not read as confidence

#### Scenario: The decision boundary is visible on the chart
- **WHEN** a wave's ratio approaches the configured success boundary
- **THEN** the boundary is marked on the chart, so a crossing reads as a crossing rather than as noise

### Requirement: The dashboard shows rollback events

The dashboard SHALL show rollbacks as they happen — which rollout rolled back, why, and how far
into its compensation plan it got — so a regression is visible on the same screen as the wave that
caused it.

#### Scenario: A rollback is attributable to its rollout and cause
- **WHEN** a rollout rolls back
- **THEN** the dashboard shows that rollout and the outcome that caused the rollback

#### Scenario: Rollback progress is visible
- **WHEN** a rollback is running its compensations
- **THEN** the dashboard shows how many steps have completed against the plan

#### Scenario: Rollback history is visible beyond the current moment
- **WHEN** an operator looks at the dashboard after a rollback concluded
- **THEN** the rollback remains visible as an event rather than disappearing when the rollout left the active set

### Requirement: Every panel reads a metric with a real source

Every panel on the dashboard SHALL read a Prometheus metric that this change or an existing
capability produces. No panel SHALL read a metric that no component publishes, and no metric SHALL
be invented for a panel's convenience.

#### Scenario: Panels resolve against the stack
- **WHEN** the dashboard is opened against the provisioned stack with the application running
- **THEN** every panel returns data rather than a no-data state caused by a metric nothing publishes

#### Scenario: A panel with no data says so
- **WHEN** a panel's query has no data because the work it describes has not happened yet
- **THEN** the panel reports no data rather than rendering zero as if it were a measurement

### Requirement: The dashboard is provisioned from the repository

The dashboard SHALL be a file tracked in `dashboards/`, provisioned into Grafana by the stack, and
SHALL name its datasource by the provisioned reference rather than by an environment-specific
identifier, so it works on a clean start with no import or edit step.

#### Scenario: The dashboard loads on a clean start
- **WHEN** the stack starts from a clean checkout
- **THEN** the dashboard is present and its panels query the provisioned datasource

#### Scenario: The dashboard carries its own identity
- **WHEN** the dashboard is provisioned
- **THEN** it has a stable title and identifier, so a restart updates the existing dashboard instead of creating a duplicate

#### Scenario: Changing the file changes the dashboard
- **WHEN** the tracked dashboard file is edited and the stack restarted
- **THEN** Grafana serves the edited dashboard
