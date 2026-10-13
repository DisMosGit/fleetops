# Spec Delta

## Purpose

Gives the agent emulator deliberate, seeded firmware-download faults and gives the update path
bounded retry, so the retry, backoff, and dead-letter behavior the pipeline exists to demonstrate is
actually exercised end to end instead of only in unit fakes.

## ADDED Requirements

### Requirement: The emulator can be configured to fault firmware downloads

The emulator SHALL support configuring a share of the fleet, or a share of download attempts, to
fault its firmware download, and SHALL support the two fault kinds the retry path must survive: a
download that stalls for a period before failing or completing, and a download that fails
immediately with a transfer error. The configuration SHALL be optional and its zero value SHALL mean
no faults are injected, so an emulator without fault settings behaves exactly as it does today.

#### Scenario: A configured fault share produces faults

- **WHEN** the emulator runs with a non-zero fault share for a fleet of devices
- **THEN** the configured share of those devices' download attempts fault, and the rest succeed

#### Scenario: No configuration means no faults

- **WHEN** the emulator runs with no fault configuration
- **THEN** every download behaves as it does today, and the update path reports the same phases and
  outcomes it reports now

#### Scenario: A stalled download is bounded by the context

- **WHEN** a device's download is injected with a stall and the command's context ends during it
- **THEN** the stall ends with the context, no partial file is left behind, and the handler returns
  rather than continuing to wait

### Requirement: Injected faults are seeded and reproducible

Fault decisions SHALL be drawn from the emulator's seeded source rather than from an unseeded one, so
the same seed replays the same faults for the same device and attempt sequence. Which devices fault,
and which kind each fault is, SHALL therefore be reproducible across runs, and a test SHALL be able
to pin a device's fault by seed rather than by timing.

#### Scenario: The same seed replays the same faults

- **WHEN** the same fault configuration and seed are run twice
- **THEN** the same devices fault in the same order with the same fault kinds

#### Scenario: Fault decisions are per device

- **WHEN** one device's downloads fault under a given seed
- **THEN** another device's faults are determined by its own draws, so injecting a fault for one
  device does not imply a fault for another

### Requirement: A firmware download is retried with bounded backoff

The update path SHALL retry a failed or stalled firmware download a bounded number of times with a
backoff that grows between attempts and is capped, and SHALL stop retrying once the attempt budget
is exhausted or the command's context ends. Retrying SHALL NOT repeat a phase report the control
plane has already accepted beyond what the retry itself needs — the download phase is reported
again as an attempt begins, and no terminal result is reported until the attempt budget is spent. A
download failure that cannot succeed on retry — a checksum mismatch between the transfer and the
command, or a malformed transfer — SHALL NOT be retried: the attempt budget is for faults that can
clear, not for a binary the device must reject.

#### Scenario: A transient fault succeeds on a retry

- **WHEN** a download faults once and succeeds on a later attempt within the budget
- **THEN** the update proceeds to apply and reports the completed outcome, and the number of
  attempts stays within the configured budget

#### Scenario: Backoff grows and is capped

- **WHEN** a download faults repeatedly until the attempt budget is spent
- **THEN** the delays between attempts grow and never exceed the configured cap, and the attempt
  count equals the configured budget

#### Scenario: Exhausting the budget reports the failure

- **WHEN** every attempt within the budget faults
- **THEN** the update reports the failed phase and exactly one terminal failed result, carrying an
  operator-safe detail, exactly as a single-attempt failure does today

#### Scenario: A checksum mismatch is not retried

- **WHEN** the transfer's checksum disagrees with the checksum the command carried
- **THEN** the update fails on that attempt without spending the retry budget

#### Scenario: Shutdown interrupts the retry loop

- **WHEN** the command's context ends while the update path is waiting to retry
- **THEN** it stops waiting and returns promptly, leaving no partial download file behind

### Requirement: A failure is reported and what a consumer cannot process is dead-lettered

A download fault that outlasts its retry budget SHALL be reported as the command's terminal failed
outcome, so the failure travels the same path as any other failed update report: the control plane
accepts it and the workflow records it as the device's result. An event a work-queue consumer cannot
process SHALL be retried up that queue's configured ladder within its attempt budget and reach the
queue's dead-letter queue carrying its failure reason and the attempts made, rather than being
dropped or requeued forever. This delivery consumes reports over the agent stream rather than from a
queue, and the rollout work queue is declared without a consumer, so what this requirement's second
half covers is the terminal behavior of the queues the fleet's broker does carry.

#### Scenario: A fault that outlasts its retries is reported as a failure

- **WHEN** a device's download faults on every attempt in its budget
- **THEN** the control plane receives one terminal failed result for that command and the rollout
  records that device's outcome as failed rather than waiting for its result timeout

#### Scenario: A poison event reaches the dead-letter queue

- **WHEN** an event a consumer cannot process fails on every attempt of its retry ladder
- **THEN** the message reaches the dead-letter queue carrying its failure reason and the attempts
  made, rather than being silently dropped or retried forever
