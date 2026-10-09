# Design

## Context

All three entrypoints (`cmd/controlplane`, `cmd/worker`, `cmd/agent`) are stage-gated stubs whose
only configuration is inline `flag` defaults, and none of them serves HTTP. There is no config
file, no validation, and no probe surface. Constraints that shape the approach:

- `go.mod` currently carries only `google.golang.org/grpc`, `google.golang.org/protobuf`, and
  `go-cmp`; per AGENTS.md every new dependency needs a one-line justification, and the MongoDB,
  RabbitMQ, and Temporal client libraries arrive with the stages that actually use them — not
  before.
- The deployment scripts already own their own env knobs (`MONGO_URI`,
  `FLEETOPS_TELEMETRY_RETENTION_DAYS` in `deploy/mongo/verify.sh`, `deploy/mongo/init.js`); the
  `mongo-data-model` contract keeps that meaning and is untouched here.
- The HTTP/SSE gateway (`-http-addr`, `:8080`) is stage-5 scope; the probes needed now must not
  block on or collide with it.

See `proposal.md` for motivation and `specs/runtime-config/spec.md`,
`specs/health-readiness/spec.md` for the behavior contracts; this document only covers the how.

## Goals / Non-Goals

**Goals:**

- One typed configuration surface shared by all three binaries, with defaults applied at decode
  time and validation that fails startup loudly with the offending field named.
- A readiness probe that answers "can this process reach MongoDB, RabbitMQ, and Temporal right
  now" within bounded time, on both the control plane and the worker.
- Testable seams: config loading is a pure function of file bytes → struct; dependency checks
  are small interfaces with hand-written fakes in tests.

**Non-Goals:**

- Environment-variable overrides or config hot-reload (restart-to-reconfigure is fine locally).
- Secrets management; the file may hold local-stack credentials and lives outside version
  control expectations (the committed `deploy/config.yaml` is a sample with dev defaults).
- Implementing the OTel exporter or Prometheus registry — only their endpoints land in the
  config now; the exporters come with the observability stage.
- k3d/compose manifest wiring of the probes (a later roadmap item); TLS/auth on the probe
  listener; a health surface on the agent emulator.

## Decisions

### 1. `internal/config`: decode-then-validate, defaults as a fully-populated struct

`config.Load(path)` (and `config.Defaults()` for the no-flag case) returns a `config.Config`
struct whose fields mirror the YAML sections one-to-one. Decoding starts from a
fully-defaulted `Config`, so absent fields simply keep their defaults — no per-field
`if zero` logic scattered around. Validation is a separate pass over the decoded struct and
returns all violations joined (`errors.Join`), each wrapped with the offending field name
(`simulation.fleet_size: must be positive`), so one broken file is fixed in one pass and the
error still names fields precisely.

- *Why not Viper/koanf?* They bring env/flag merging, remote config, and watch semantics none of
  which are in scope — a struct plus one decoder is less code and fewer dependencies.
- *Why strict decoding?* `yaml.Decoder.KnownFields(true)` turns a typo like `grpc.port` into a
  startup error instead of a silently ignored key; the spec requires this.
- *Why not validation via struct tags?* Tag-based validators are another dependency and hide the
  rules in strings; explicit `validate()` code is greppable and table-testable.

YAML decoding is the one new runtime dependency: `go.yaml.in/yaml/v3` (maintained successor of
`gopkg.in/yaml.v3`; supports strict unknown-key rejection). Justification in the PR: decode the
single YAML configuration file. Probe-server lifecycle additionally brings
`golang.org/x/sync/errgroup` into `go.mod` as a direct dependency (justification: the
errgroup-with-context goroutine ownership the concurrency rules mandate); it was already in the
module graph as an indirect dependency.

### 2. `internal/health`: named checks behind a tiny interface

The package exposes a handler constructor that takes the configured checks and serves
`/healthz` and `/readyz` on one `http.ServeMux`:

```go
type Check interface {
    Name() string          // "mongodb" | "rabbitmq" | "temporal"
    Check(ctx context.Context) error
}
```

`/healthz` never runs checks. `/readyz` runs all checks concurrently, each with its own short
timeout (a package constant, ~2s — deliberately not config: nobody tunes it), collects every
result (no fail-fast: the body reports all three), and answers 200/503 per the spec. Error
summaries are the check error's message with URLs stripped — checks wrap errors themselves so
the handler can render them verbatim without leaking credentials.

- *Why an interface, not functions?* AGENTS.md: interfaces where consumed. Tests hand-write a
  two-method fake; swapping in driver-backed checks later is a constructor change only.
- *Why concurrent checks?* Worst-case latency is one timeout, not the sum of three — the spec
  demands a promptly answering probe when everything is down.

### 3. Dependency checks v1: bounded TCP dial; protocol depth upgrades later

Without the driver libraries, a full protocol ping is hand-rolled wire-format work (Mongo
OP_MSG/`hello`, AMQP frame handshake, gRPC health service) — over-engineered for a probe and a
maintenance liability. V1 checks are `net.DialTimeout` reachability probes with per-check
timeout, implemented in one small `dialCheck` type. The `Check` interface isolates this: when
the MongoDB/AMQP/Temporal clients land in stages 2–3, their constructors gain a `Ping`-based
check and the probe contract (`/readyz` shape, status codes) is unchanged.

- *Why not add the real drivers now?* Three heavy dependencies purely for probes violates the
  dependency rule and stages 2–3 would add them anyway.
- *Trade-off (see Risks):* a TCP dial cannot distinguish "process up but service broken".

### 4. Probe listener is separate from the gateway; one address for both binaries

Both services serve probes on `observability.health_addr` (default `:8081`) via their own
`http.Server`, lifecycle owned by the same `errgroup` as the main listeners (start in
`Go`, shut down on ctx cancel, `Shutdown` with a short grace period). The stage-5 HTTP gateway
keeps its `-http-addr` flag untouched — probes are ready today, the gateway is not, and merging
them would make the probe contract depend on stage-5 code. Two HTTP ports on one binary is the
cost; `deploy/config.yaml` and the README document which is which.

### 5. Entrypoint wiring: full-file validation in every binary

Every entrypoint validates the *whole* file even though each consumes only some sections. A
file broken for the worker is therefore caught at control-plane startup too — one config, one
schema, one truth. `cmd/agent` keeps nothing but `-config` (its old `-control-plane` /
`-fleet-size` flags become config fields), and each `main` stays thin: parse `-config`, load,
validate, construct, serve.

## Risks / Trade-offs

- [TCP-only readiness can pass while a dependency is protocol-broken] → Acceptable at this
  stage; the `Check` interface makes the upgrade to driver `Ping` checks a local change, and
  readiness semantics (status codes, body shape) are already spec'd and tested.
- [The YAML schema is a compatibility surface the moment a file exists] → Evolve additively:
  new fields need defaults; renaming/removing is a spec change. Strict decoding means adding a
  field is safe, changing one is not.
- [`:8080` (gateway flag) vs `:8081` (probe config) confusion] → Documented in
  `deploy/config.yaml` comments and the README run section; the gateway flag disappears when
  stage 5 replaces it.
- [Joined validation errors are noisier than fail-fast] → Each error is field-named and wrapped;
  the noise buys one-pass fixes. Tests assert on `errors.Is`/`errors.As` and field names, not on
  the joined string.

## Migration Plan

None — no running behavior exists yet. Rollback is a revert; no data or wire contract changes.

## Open Questions

- When the stage-5 HTTP gateway lands, should probes move onto the gateway listener and drop
  `health_addr`? Deferrable: keeping the dedicated probe port is also a valid end state, and
  either choice is additive or a documented config change — it does not affect this change's
  specs or tasks.
- Should `observability.metrics_addr` also serve probes in the observability stage? Same
  deferral; the probe contract does not depend on the answer.
