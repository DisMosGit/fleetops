# api/proto/ — Protobuf contracts

v1 `.proto` contracts for FleetOps' bounded contexts (one proto package per service, messages
never shared across services) and the Go code generated from them. v1 freezes at the end of
stage 1; afterwards contracts evolve by adding fields only — never renumber, never rewrite.

**Not implemented yet** — no `.proto` contract exists; stage 1 delivers the frozen v1 set
(including `AgentService.Connect` and `AgentService.Report`). Regenerate Go with `make proto`
(requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`).
