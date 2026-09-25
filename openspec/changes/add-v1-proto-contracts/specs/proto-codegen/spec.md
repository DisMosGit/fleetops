# Spec Delta

## Purpose

Defines how the v1 protobuf contracts are compiled into Go: the contract-to-package mapping, the
pinned generator toolchain that makes output reproducible, and the repeatable command that
regenerates client and server stubs from the contract files.

## ADDED Requirements

### Requirement: Contract and Go package layout
Protobuf contracts SHALL live under `api/proto/<context>/v1/`, one proto package per bounded
context, and each contract file SHALL map its generated Go to the import path
`github.com/DisMosGit/fleetops/api/proto/<context>/v1` with generated files written beside the
contract that produced them. No generated Go package SHALL live outside `api/proto/`.

#### Scenario: Predictable import path
- **WHEN** the agent contract at `api/proto/agent/v1/` is compiled
- **THEN** client and server code imports it at `github.com/DisMosGit/fleetops/api/proto/agent/v1`
  and the generated files sit beside the contract

#### Scenario: One package per bounded context
- **WHEN** a new bounded context needs contracts
- **THEN** it gets its own `api/proto/<context>/v1/` package and does not import messages from
  another context's package

### Requirement: Pinned generator toolchain
The Go stub generators (`protoc-gen-go` and `protoc-gen-go-grpc`) SHALL be pinned to exact
versions in a single generator configuration that the regeneration command reads, so the same
contract files always compile with the same generator versions. Generator plugins are build-time
tools and SHALL NOT become runtime dependencies of the binaries.

#### Scenario: Reproducible across machines
- **WHEN** two contributors regenerate from identical contract files using the pinned toolchain
- **THEN** their generated output is identical byte for byte

#### Scenario: Tools are not runtime dependencies
- **WHEN** the module's dependencies are inspected
- **THEN** the generator plugins appear only as pinned build-time tooling, not as imports of
  shipped binaries

### Requirement: Repeatable regeneration command
The repository SHALL provide one documented, repeatable command — the `make proto` target — that
regenerates both client and server stubs from every `.proto` contract under `api/proto/`. The
command MUST fail with a non-zero exit and a clear message naming the missing tool when `protoc`
or a pinned plugin is unavailable, and MUST NOT leave partially generated output behind.

#### Scenario: Regenerating the full stub surface
- **WHEN** a contributor runs the regeneration command after changing a contract
- **THEN** fresh client and server stubs for every service in every contract file are produced
  under `api/proto/` and the command exits zero

#### Scenario: Missing toolchain fails loudly
- **WHEN** the regeneration command runs on a machine without `protoc` or the Go plugins
- **THEN** it exits non-zero with a message naming what is missing and how the toolchain is
  expected to be provided

#### Scenario: Regeneration is idempotent
- **WHEN** the regeneration command runs twice with no contract change in between
- **THEN** the second run leaves the working tree unchanged

### Requirement: Generated files are build artifacts
Generated Go files SHALL carry the generators' do-not-edit header and SHALL never be edited by
hand; the regeneration command is the only way they change. Contract files — not generated output —
are the source of truth.

#### Scenario: Hand edits do not survive
- **WHEN** a generated file is edited by hand and the regeneration command runs
- **THEN** the file is restored to the canonical generator output and the hand edit is gone

#### Scenario: Review sees contracts first
- **WHEN** a change to the wire contract is reviewed
- **THEN** the reviewed artifact is the `.proto` file and the generated diff is understood as its
  mechanical consequence
