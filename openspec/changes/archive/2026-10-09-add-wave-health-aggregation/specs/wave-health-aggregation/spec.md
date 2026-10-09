# Spec Delta

## Purpose

Turns a wave's heartbeat telemetry into the one number a canary gate decides on: how much of the
wave is healthy over a sliding window, and on how much evidence that judgement rests.

## ADDED Requirements

### Requirement: Wave health evaluation

The system SHALL compute, for one rollout and wave, the health of that wave's target devices over a
sliding window of heartbeat telemetry, reporting the success ratio — the share of the window's
samples whose reported health is at or above the per-sample health threshold — and the sample size
— the number of samples the ratio was computed over. The evaluation MUST count samples only from
the devices the wave records as its targets, only within the effective window, and MUST count each
stored heartbeat sample exactly once, so a redelivered heartbeat cannot inflate the sample size.
Evaluation SHALL observe state and never change it: it records nothing, publishes nothing, and
signals nothing.

#### Scenario: Ratio and sample size over the wave's devices

- **WHEN** a wave targeting ten devices has 95 samples at or above the health threshold and 5 below
  it within the window
- **THEN** the evaluation reports success ratio 0.95 with sample size 100

#### Scenario: Only the wave's target devices count

- **WHEN** devices the wave does not target emit heartbeats within the window
- **THEN** those samples change neither the success ratio nor the sample size

#### Scenario: Redelivered heartbeats are counted once

- **WHEN** a heartbeat accepted into telemetry storage is redelivered and deduplicated by its event
  id
- **THEN** the sample size counts it once

### Requirement: Sliding window semantics

The effective window of an evaluation SHALL be the configured health window ending at the moment
the evaluation runs, and its start MUST NOT reach back before the time the wave started. The
evaluation SHALL report the effective window's start and end so a caller can tell a window that was
clipped by the wave's start from one that ran its full configured width. Samples whose measurement
time falls outside the effective window MUST NOT contribute to the ratio or the sample size, so
later evaluations of the same wave slide forward as older samples age out.

#### Scenario: Samples older than the window age out

- **WHEN** a wave is evaluated twice over the same devices, with a window of the configured width
- **THEN** the second evaluation covers the period since the second evaluation began and excludes
  samples that have aged past the window's start

#### Scenario: The window never reaches back before the wave started

- **WHEN** a wave started two minutes ago is evaluated over a five-minute window
- **THEN** the reported window starts at the wave's start, not five minutes before the evaluation,
  and heartbeats recorded before the wave started are excluded

#### Scenario: The window is reported with the result

- **WHEN** any evaluation returns a result
- **THEN** the result carries the effective window's start and end together with the ratio, the
  sample size, and the verdict

### Requirement: Verdict and decision boundary

An evaluation SHALL report a verdict of healthy, unhealthy, or undecided. The verdict SHALL be
undecided while the sample size is below the configured minimum sample count; at or above it, the
verdict SHALL be healthy when the success ratio is at or above the configured minimum success
ratio and unhealthy when it is below, so the boundary itself — ratio equal to the minimum success
ratio — is healthy. A window that cannot be decided MUST NOT be reported as healthy, and the
verdict MUST NOT be derived from a ratio computed over fewer samples than the minimum.

#### Scenario: Exactly at the boundary is healthy

- **WHEN** a window holds at least the minimum sample count and its success ratio equals the
  configured minimum success ratio exactly
- **THEN** the verdict is healthy

#### Scenario: One failing sample across the boundary flips the verdict

- **WHEN** a window at the boundary with N samples gains a single sample below the health
  threshold, leaving it above the minimum sample count and with a ratio below the minimum success
  ratio
- **THEN** the verdict flips from healthy to unhealthy

#### Scenario: Too few samples cannot be healthy

- **WHEN** a window holds fewer samples than the configured minimum, however high its ratio
- **THEN** the verdict is undecided and the reported ratio is not promotion evidence

### Requirement: Transient failures do not read as a regression

Because the ratio counts individual samples, a wave's health SHALL degrade in proportion to how
many of its samples are degraded: a brief dip below the health threshold in an otherwise healthy
window MUST NOT by itself turn the verdict unhealthy, and a wave whose degraded samples are enough
to cross the minimum success ratio MUST turn unhealthy. Once a transient dip's samples age out of
the window, a later evaluation MUST reflect the recovered ratio.

#### Scenario: A brief dip leaves a healthy wave healthy

- **WHEN** a window whose samples are otherwise healthy contains a short burst of degraded samples
  that leaves the success ratio at or above the minimum success ratio
- **THEN** the verdict stays healthy

#### Scenario: Sustained degradation crosses the boundary

- **WHEN** enough samples fall below the health threshold for the success ratio to drop below the
  minimum success ratio
- **THEN** the verdict is unhealthy

#### Scenario: Recovery is visible once the dip ages out

- **WHEN** a wave whose ratio dropped below the minimum is evaluated again after the degraded
  samples have aged past the window's start
- **THEN** the ratio reflects only the recovered samples and the verdict follows it

### Requirement: Empty windows report no evidence

An evaluation whose effective window holds no samples — because the wave has no target devices, has
just started, or none of its devices reported — SHALL report a sample size of zero, a success ratio
of zero, and an undecided verdict, and MUST NOT be reported as either healthy or unhealthy. The
same MUST hold for a wave that has not started.

#### Scenario: A wave without samples is undecided

- **WHEN** a wave whose devices have emitted no heartbeats in the window is evaluated
- **THEN** the result reports sample size zero, success ratio zero, and verdict undecided

#### Scenario: A wave without target devices is undecided

- **WHEN** a wave that targets no devices is evaluated
- **THEN** the result reports sample size zero and verdict undecided without being read as a
  healthy wave

#### Scenario: An empty window is not a failure

- **WHEN** a wave is evaluated before any of its devices has reported
- **THEN** the evaluation succeeds with an undecided result rather than an error
