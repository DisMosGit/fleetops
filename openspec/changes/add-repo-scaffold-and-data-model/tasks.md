# Tasks

## 1. Repository skeleton

- [x] 1.1 Create the top-level directories `cmd/`, `internal/`, `api/proto/`, `deploy/`, `dashboards/`, and `web/` and verify with `ls` that each exists at the repository root (verify: `ls -d cmd internal api/proto deploy dashboards web` lists all six)
- [x] 1.2 Add a placeholder `README.md` to each skeleton directory stating its one owner domain, what belongs there (per the spec's layout requirement), and the delivery stage that will fill it with working content — explicitly labelled "not implemented yet" (verify: every directory from 1.1 contains a README whose text names its owning stage and no placeholder claims working functionality)
- [x] 1.3 Create the `internal/` capability packages `internal/agent`, `internal/temporal`, `internal/telemetry` with package doc comments naming their future responsibility (agent emulation, Temporal orchestration, telemetry ingestion) and verify `go build ./...` passes (verify: `go build ./...` exits 0 and `go doc ./internal/...` shows a doc comment per package)
- [x] 1.4 Create the thin entrypoint packages `cmd/controlplane`, `cmd/worker`, `cmd/agent`, each a minimal `main` containing only flag parsing, dependency wiring placeholders, and a run call — no behavior (verify: `go build ./...` passes and each `main` file stays under ~50 lines with no business logic)
- [x] 1.5 Verify the layout contract end-to-end: no Go code exists outside `cmd/` and `internal/`, and no `utils`/`helpers`/`common` package exists anywhere (verify: `go list ./...` returns only packages under `cmd/` and `internal/`, and `find . -name utils -o -name helpers -o -name common` finds nothing)

## 2. Frontend submodule placeholder

- [x] 2.1 Register `web/` as a git submodule in `.gitmodules` pointing at the intended frontend repository URL (confirm the URL with the user if none is known yet; a clearly-marked placeholder URL is acceptable and updated later) and verify the registration (verify: `.gitmodules` contains a `web/` entry with URL and path, and `git submodule status` recognizes it)
- [x] 2.2 Add the `web/` placeholder content naming the frontend stack (React + Vite) and stage 5 as its delivery milestone (verify: the placeholder states the stack and stage and does not present the UI as existing)
- [x] 2.3 Verify a submodule-less clone stays fully usable by the Go toolchain (verify: with `web/` uninitialized, `make check` from task 4 passes)

## 3. Makefile build tool

- [x] 3.1 Write the root `Makefile` with targets `build` (compile all binaries), `vet` (`go vet ./...`), `lint` (the repo linter over the tree), `test` (`go test -race -count=1 ./...`), and `check` (the AGENTS.md Definition of Done in order: `goimports -w .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...`), each failing non-zero when its command fails (verify: `make check` runs all four commands in order on this tree and exits 0; `make test` runs only the test command)
- [x] 3.2 Add targets `proto` (regenerate Go from `api/proto/` contracts), `up` (bring the local stack up from `deploy/` manifests), and `down` (tear it down), each wrapping its underlying tool and failing non-zero on tool failure (verify: each target runs exactly its documented command; `make up`/`make down` fail loudly rather than silently when the stack tooling is missing)
- [x] 3.3 Add `make help` listing every target with its documented command so the Makefile is self-describing (verify: `make help` prints every target from 3.1–3.2 with one line each)
- [x] 3.4 Add a doc-comment header to the Makefile pointing at the AGENTS.md Definition of Done as the source of truth it wraps (verify: the header names AGENTS.md and states the wrap-not-replace relationship)

## 4. MongoDB data model

- [x] 4.1 Write the MongoDB bootstrap definition (under `deploy/`, applied by the local stack's MongoDB) creating `devices` with required fields `model`, `region`, `current_fw`, `status`, `last_heartbeat` enforced by a schema validator (verify: inserting a device doc missing a required field is rejected; a complete doc succeeds)
- [x] 4.2 Extend the bootstrap with the firmware metadata collection (required `version`, `checksum`, `gridfs_id`; no inline payload), `rollouts` (required `firmware_id`, `status`, `temporal_wf_id`, `region`, `model`), and `waves` (required `rollout_id`, `percent`, `status`, `success_rate`) — each with a required-field validator (verify: one valid and one invalid insert per collection shows the validator accepts/rejects correctly)
- [x] 4.3 Create the `telemetry` heartbeat store as a time-series collection keyed by `ts` with `meta` carrying `device_id`, `region`, `model` and top-level `cpu`, `mem`, `health`; if the pinned MongoDB rejects the idempotency contract on a time-series collection, fall back to a plain collection with a TTL index on `ts` per design decision 5 and record the deviation (verify: a heartbeat doc inserts and is retrievable by device + time range; the chosen shape is recorded in the change notes)
- [x] 4.4 Make telemetry ingestion idempotent at the storage layer: `_id` = event id, and a duplicate insert of the same event id is a no-op, not an error or a second row (verify: inserting the same event twice leaves exactly one document and the second insert reports success-as-no-op at the ingest contract level)
- [x] 4.5 Create the index set from design decision 7: `devices` unique `_id` + `region`/`model` + `status`; firmware unique `version`; `rollouts` `status` + `firmware_id`; `waves` `rollout_id` (+ wave order); `telemetry` `meta.device_id`+`ts` and `meta.region`+`meta.model`+`ts` (verify: `getIndexes`/`listIndexes` shows every named index on every collection)
- [x] 4.6 Add the telemetry retention policy: TTL on `ts` with a single named configuration value (default 7 days) shared by the TTL index definition and the deploy stack; no TTL on `devices`, firmware metadata, `rollouts`, or `waves`; GridFS binaries persist while referenced (verify: a telemetry doc with `ts` beyond the retention window is removed by the TTL monitor without any cleanup job, and a domain doc past the same age survives)
- [x] 4.7 Write a verification script (e.g. `deploy/mongo/verify.sh`) asserting collections, validators, indexes, and the TTL configuration against a running MongoDB, and wire it into a make target or document its invocation in the deploy placeholder README (verify: running the script against the bootstrapped MongoDB exits 0 and exits non-zero when any index or validator is dropped)
- [x] 4.8 Document the data model contract where contributors will look for it: field lists, index rationale, and retention per collection in the `deploy/` placeholder README (verify: every field named in `specs/mongo-data-model/spec.md` appears in the documented contract)

## 5. Cross-cutting consistency

- [x] 5.1 Update the README repository map: `web/` shown as a frontend submodule placeholder and `dashboards/` added, with no planned path presented as already working (verify: README's repo map matches the tree created in groups 1–2 and `grep -n "dashboards" README.md` hits the map)
- [x] 5.2 Run the Definition of Done across the finished tree and fix findings: `goimports -w .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...` (verify: all four commands exit 0)
- [x] 5.3 Final scope check: only scaffold, Makefile, submodule registration, deploy bootstrap/verify files, placeholder READMEs, and this openspec change differ from the previous state — no application logic snuck in (verify: `git status` shows no changed file outside those paths)