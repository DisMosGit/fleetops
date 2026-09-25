# Spec Delta

## Purpose

Defines the repository layout contract and the build tool surface: which directories exist, what
belongs in each, how the frontend submodule placeholder behaves, and which Makefile targets every
contributor and automation step can rely on.

## ADDED Requirements

### Requirement: Standard directory layout
The repository SHALL organize all content into the following top-level directories with exactly
one owner domain each: `cmd/` (thin entrypoints for the control plane, the Temporal workers, and
the agent emulator), `internal/` (all library code, one package per capability: agent emulation,
Temporal orchestration, telemetry ingestion), `api/proto/` (Protobuf contracts and their generated
Go), `deploy/` (local-stack manifests for compose/k3d), `dashboards/` (Grafana dashboards), and
`web/` (the frontend, as a git submodule placeholder). No library code SHALL live outside
`internal/`, and there SHALL be no exported library surface.

#### Scenario: Fresh clone exposes the layout
- **WHEN** a contributor clones the repository at this change's completion
- **THEN** every directory named above exists at the top level, and each contains at least a
  placeholder note naming the delivery stage that will fill it with working content

#### Scenario: No code outside the sanctioned directories
- **WHEN** new Go code is added to the repository
- **THEN** it lives either in a thin `cmd/` entrypoint (flags, dependency wiring, run) or in an
  `internal/` package, and no `utils`, `helpers`, or `common` package exists

#### Scenario: Planned paths are not presented as working
- **WHEN** a directory exists but its delivery stage has not landed
- **THEN** its placeholder content states that the code is not implemented yet and names the
  stage that will deliver it

### Requirement: Thin command entrypoints
The `cmd/` directory SHALL contain one entrypoint package per binary — control plane, worker, and
agent emulator — and each entrypoint SHALL limit itself to flag parsing, dependency construction
and wiring, and starting the run loop. All actual behavior SHALL live in `internal/` packages so
that it is testable without a running binary.

#### Scenario: Behavior is testable below the entrypoint
- **WHEN** a test needs to exercise behavior owned by a binary
- **THEN** the behavior is reachable from an `internal/` package test and the entrypoint package
  itself contains no logic beyond flags, wiring, and run

### Requirement: Build tool targets
The repository SHALL provide a `Makefile` at the root whose targets wrap — never replace — the
commands named in the engineering rules, and SHALL define at least these targets: `build` (compile
all binaries), `vet` (`go vet ./...`), `lint` (the repository's linter over the tree), `test`
(`go test -race -count=1 ./...`), `check` (the full Definition of Done in order: import/format
cleanup, `go vet`, lint, `go test`), `proto` (regenerate Go from the Protobuf contracts), `up`
(bring the local stack up from `deploy/` manifests), and `down` (tear the local stack down). Each
target SHALL run exactly the command its help text documents and SHALL fail with a non-zero exit
when that command fails.

#### Scenario: Definition of Done via the build tool
- **WHEN** a contributor runs `make check`
- **THEN** the four Definition-of-Done commands run in order (import/format cleanup, `go vet`,
  linter, `go test -race -count=1 ./...`) and the target exits non-zero if any of them fails

#### Scenario: Single targets stay single-purpose
- **WHEN** a contributor runs `make test`, `make vet`, or `make lint`
- **THEN** exactly the documented command runs — `make test` runs the race-enabled test suite and
  nothing else, and likewise for `vet` and `lint`

#### Scenario: Stack lifecycle targets
- **WHEN** a contributor runs `make up` or `make down`
- **THEN** the local stack defined by the `deploy/` manifests is brought up or torn down, and a
  failure in the underlying manifest tooling fails the target

### Requirement: Frontend submodule placeholder
`web/` SHALL be a git submodule registered in `.gitmodules`, pointing at the frontend repository
that will hold the React + Vite UI. The rest of the repository SHALL be fully usable when the
submodule is not initialized: an uninitialized or placeholder `web/` SHALL NOT break any Go build,
test, or lint target.

#### Scenario: Usable clone without submodule content
- **WHEN** a contributor clones the repository without initializing submodules
- **THEN** `web/` is registered but empty or placeholder-only, and `make check` still passes

#### Scenario: Submodule registration is explicit
- **WHEN** a contributor inspects `.gitmodules`
- **THEN** the `web/` submodule entry exists with its URL and path, and the placeholder content in
  `web/` names the frontend stack (React + Vite) and the stage that will deliver it