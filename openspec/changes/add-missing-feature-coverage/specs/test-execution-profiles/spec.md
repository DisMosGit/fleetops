# Spec Delta

## Purpose

Defines how a test run is configured for the machine it runs on: the environment variables that set
a run's profile, probe budgets, workload sizes, and container use, the defaults that preserve
today's behavior, and the rule that a run's behavior is chosen by its environment and never by
inspecting the host.

## ADDED Requirements

### Requirement: A test run's configuration comes from named environment variables

A test run's profile, probe budgets, workload sizes, and container use SHALL be configurable through
named environment variables, read through one helper rather than by each test package reading the
environment for itself. The resolved configuration SHALL be the same for a given environment
regardless of the host it runs on. The variables a run honours SHALL be exactly the documented set:
an undocumented variable SHALL have no effect, and a variable that is documented SHALL have the
effect its documentation describes.

#### Scenario: The profile comes from the environment

- **WHEN** a test run is started with the weak-host profile selected through its environment
  variable
- **THEN** the run applies that profile's values for workload size and probe budgets

#### Scenario: An undocumented variable changes nothing

- **WHEN** a run is started with an environment variable outside the documented set
- **THEN** the run's workload sizes, budgets, and container use are exactly what they would have
  been without it

### Requirement: Defaults preserve today's behavior on a capable host

With no environment variables set, a test run SHALL behave as it does today: the same workload
sizes, the same probe budgets, the same container stacks, and the same set of tests executed.
Configuring a run for a weak host SHALL be an explicit choice, not something a capable host
experiences by default.

#### Scenario: An unconfigured run is today's run

- **WHEN** the suite runs with no configuration variables set
- **THEN** the workload sizes and probe budgets are the values the suite used before this
  configuration existed, and no test is skipped that was not skipped before

#### Scenario: A configured run narrows, not widens

- **WHEN** a run is configured for a weak host
- **THEN** its workload sizes and budgets are at most the defaults, so no configuration makes a
  run heavier than an unconfigured one

### Requirement: A weak-host profile fits a 4 GB, 4 CPU machine

The configuration SHALL offer a profile whose documented target is a machine with at most 4 GB of
RAM and 4 CPUs, and which keeps the suite's peak memory and concurrent container count within that
budget. The profile SHALL reduce cost by narrowing the work a run does — simulated fleet sizes,
sample counts, probe budgets, and the number and sharing of container stacks — and MUST NOT reach
its budget by deleting assertions: every test the profile runs SHALL make the same assertions the
default profile makes, and a test the profile cannot afford SHALL be reported as skipped with the
reason, never silently weakened.

#### Scenario: The weak-host profile runs the suite within its budget

- **WHEN** the integration suite runs with the weak-host profile on a machine with 4 GB of RAM and
  4 CPUs
- **THEN** it completes without exhausting memory and without a test failing for a resource reason

#### Scenario: A narrowed run asserts the same behavior

- **WHEN** a test runs under the weak-host profile
- **THEN** it asserts the same observable behavior it asserts under the default profile, with only
  workload sizes and budgets differing

#### Scenario: An unaffordable test says so

- **WHEN** a test cannot run within the configured budget
- **THEN** it is skipped with a message naming the budget and the configuration variable that would
  allow it, rather than being run with weakened assertions or failing for a resource reason

### Requirement: A run's behavior never depends on probing the host

The configuration SHALL NOT detect the host's memory, CPU count, or load and choose behavior from
it. The same environment SHALL produce the same run on every machine, so a result is reproducible
and a configuration mistake is visible rather than absorbed by an automatic downgrade.

#### Scenario: The same environment behaves the same on any host

- **WHEN** two machines with different memory and CPU counts run the suite with the same
  environment
- **THEN** the workload sizes, probe budgets, and container use are identical on both, and neither
  machine's run is narrowed on its own initiative

#### Scenario: A resource-shaped run is an explicit choice

- **WHEN** a run must be narrowed for a small machine
- **THEN** the narrowing appears in the environment the run was started with, not in behavior the
  suite chose for itself

### Requirement: A misconfigured run fails loudly

An environment value that cannot be used — a profile name that is not defined, a size or budget
that is not a positive number, a budget multiplier that is not greater than zero — SHALL fail the
run with a message naming the variable and the value it rejected, when the configuration is
resolved. A misconfigured run MUST NOT fall back to a default silently, and it MUST NOT fail later
as an unrelated test timeout that hides the configuration error.

#### Scenario: An unknown profile name fails the run

- **WHEN** the profile variable names a profile that is not defined
- **THEN** resolving the configuration fails with a message naming the variable and the unknown
  value, before any test body runs

#### Scenario: A non-positive size or budget fails the run

- **WHEN** a size or budget variable is set to zero, a negative number, or a value that is not a
  number
- **THEN** resolving the configuration fails with a message naming the variable and the rejected
  value

### Requirement: Container-backed tests share a stack per test package

Integration tests that need MongoDB, RabbitMQ, or a Temporal server SHALL obtain them from a stack
that is started once per test package and shared by the tests in it, with each test isolated by its
own database name rather than by its own containers. A test SHALL still get its own isolated data:
no test SHALL observe another test's documents, queues, or workflow executions. The number of
container stacks a package starts SHALL NOT grow with the number of tests in the package.

#### Scenario: One stack serves a package's tests

- **WHEN** a package's integration tests run
- **THEN** the container stack is started once for the package and every test in the package uses
  it

#### Scenario: Tests stay isolated from each other

- **WHEN** two tests in a package run against the shared stack
- **THEN** neither observes the other's database documents, queue messages, or workflow executions

#### Scenario: A stack failure fails the package, not one test

- **WHEN** the shared stack cannot be started
- **THEN** the package's tests fail reporting the stack that could not start, rather than each test
  failing separately with an unrelated error
