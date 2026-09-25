# Tasks

## 1. Rewrite AGENTS.md (project-specific, from scratch)

- [x] 1.1 Diff the current generic AGENTS.md rule set against the planned FleetOps stack and record which rules carry over and which are dropped (verify: a short carry-over/drop checklist exists in the change notes and every dropped rule is listed)
- [x] 1.2 Write the new AGENTS.md from scratch: FleetOps-specific engineering rules — Go idioms and error handling, Temporal workflow determinism and activity idempotency, gRPC bidi stream conventions, MongoDB/RabbitMQ conventions, testing rules and coverage bars, Definition-of-Done commands (verify: the file contains no generic sections unrelated to FleetOps and its DoD block lists the four goimports/go vet/golangci-lint/go test commands)
- [x] 1.3 Add the planned repository map to AGENTS.md matching the idea source layout (`cmd/<binary>`, `internal/{agent,telemetry,temporal,...}`, `api/proto`, `deploy`, `web`) (verify: every path named in the map appears in the layout of .nda/idea_description.md §7/§8 and no invented path is presented as existing)
- [x] 1.4 Verify the new AGENTS.md survives a link and consistency check — every referenced file/command/flag name is spelled consistently with the repo (verify: `grep` shows zero references to `.nda/` and zero broken internal file references)

## 2. Add README.md (project front door)

- [x] 2.1 Write the "what and why" section: FleetOps purpose (IoT-fleet OTA updates with canary waves, health checks, auto-rollback), who it is for, and portfolio/demo framing — in our own words, no verbatim NDA content (verify: no sentence copied from .nda/idea_description.md and no reference to `.nda/` paths)
- [x] 2.2 Write the architecture summary and tech-stack table (Go, Temporal, MongoDB, RabbitMQ, gRPC/Protobuf, React+Vite, k3d, OTel/Prometheus/Grafana) with one-line justification per technology (verify: every technology in the table appears in the idea source stack table and none of the explicitly-excluded technologies — Kafka, Redis, Helm — is presented as used)
- [x] 2.3 Write the repository map and delivery-status table (5 stages from the idea source, current status "not started") (verify: status table lists exactly the 5 stages and no stage is marked done)
- [x] 2.4 Write a stage-gated Getting Started: prerequisites only, plus explicitly-labelled "planned" pointers to future run instructions (verify: no run/build/deploy command is documented as working, and each "planned" item names the stage that will deliver it)
- [x] 2.5 Add navigation links to CONTRIBUTING.md, AGENTS.md, LICENSE.md, and docs/README.md (verify: every link target exists in the repository after tasks 3–4 land)

## 3. Add CONTRIBUTING.md (human contributor workflow)

- [x] 3.1 Write prerequisites and development setup sections (Go toolchain, Docker/k3d, Temporal CLI/UI as they become available) gated on current project state (verify: setup steps reference only tools the project actually requires per the tech-stack decision and mark not-yet-needed ones as "planned")
- [x] 3.2 Write the branch/PR workflow, review expectations, and a Definition-of-Done checklist that links to AGENTS.md rules instead of restating them (verify: the DoD section contains at most one full checklist and every engineering rule mentioned is a link or one-line reference to AGENTS.md)
- [x] 3.3 Write the "what belongs where" pointers: bug reports / feature requests via issues, spec-driven changes via OpenSpec (verify: CONTRIBUTING.md mentions `openspec/` as the home of change proposals and contains no duplicated engineering rules)

## 4. Add docs/README.md (docs index + future-docs plan)

- [x] 4.1 Write the docs index with the content-ownership table from design.md Decision 1 (README / CONTRIBUTING / AGENTS.md / docs/) so future doc content has an obvious home (verify: the table lists exactly the four ownership surfaces and one sentence each on what does and does not belong there)
- [x] 4.2 Write the future-docs backlog table: `docs/architecture.md`, `docs/api.md`, `docs/workflows.md` (Temporal), `docs/telemetry.md`, `docs/observability.md`, `docs/demo.md` — each with purpose, audience, and trigger milestone from the 5-stage plan (verify: every backlog row has a non-empty purpose, audience, and milestone cell, and each milestone maps to one of the 5 stages)
- [x] 4.3 State the docs-writing rule: a planned doc is written when its trigger milestone lands, as part of that stage's work (verify: the rule appears once in docs/README.md and README.md links to the backlog instead of repeating it)

## 5. Cross-document consistency review

- [x] 5.1 Run a final consistency pass over README.md, CONTRIBUTING.md, AGENTS.md, and docs/README.md: no contradictions on ownership, no duplicated DoD, no dead links, no `.nda/` leakage (verify: `grep -rn "\.nda" README.md CONTRIBUTING.md AGENTS.md docs/` returns nothing and every intra-repo link resolves to an existing file)
- [x] 5.2 Confirm nothing outside the four doc files changed (verify: `git status` shows only README.md, CONTRIBUTING.md, AGENTS.md, docs/README.md (and this openspec change) as modified/added)