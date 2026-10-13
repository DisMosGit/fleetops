# Design

## Context

See `proposal.md` — Why. The constraints that shape the approach are the ones the repository and the
development machine impose on a deployment:

- **The cluster already exists.** The development machine runs a single-node k3s (`k3s v1.36.4+k3s1`,
  control-plane role, `local-path` as the default StorageClass). Its kubeconfig lives at
  `/etc/rancher/k3s/k3s.yaml`, and the `kubectl` binary shipped with k3s reads it directly. There is
  no `k3d`, no Helm chart repository, and no image registry.
- **Configuration is a file, never the environment.** Every binary takes `-config <path>` and
  `config.Load` rejects unknown keys; no binary reads an environment variable. `internal/config`
  owns the whole schema, so nothing in this change may introduce a configuration field.
- **Three binaries, one image recipe.** `cmd/controlplane`, `cmd/worker`, and `cmd/agent` differ only
  in which `main` package is built; `go.mod` declares Go 1.27.1 and the module has no cgo
  requirement, so one static build serves all three.
- **Two hard dependency rules already specified.** `deploy/README.md` and the `temporal-worker`
  capability record that MongoDB must be a replica set because the worker establishes a change
  stream over `devices` before it polls; the `health-readiness` capability fixes the probe endpoints
  and their semantics.
- **What exists to observe.** Only `controlplane` serves a metrics endpoint; `worker` and `agent` do
  not. `observability.otel_endpoint` is declared and validated but read by no binary, so nothing
  exports traces today.
- **Device identity is process-local.** `agent.NewFleet` numbers devices `device-0 … device-n-1` and
  no configuration field parameterizes the numbering, so two emulator processes produce the same
  identities and the control plane's hub routes a device id to the most recent registration.

## Goals / Non-Goals

**Goals:**

- The repository contains everything needed to run the whole stack on a cluster that already exists,
  with one command up and one command down.
- The demo overlay is the only place a demo value lives; the base is what a non-demo environment
  would extend, and it declares no replica count, fleet size, or resource size that is a demo choice.
- Every application workload is observable in the way the code already supports: probes against the
  documented endpoints, metrics scraped from the binary that serves them.
- Tear-down is safe by default: it removes workloads, not data; and the command that removes data
  says which claims it removed.

**Non-Goals:**

- Creating a cluster. The cluster is an input, verified rather than provisioned.
- A registry, image promotion, or multi-architecture images. Images are built on the node's own
  architecture and imported into its container runtime.
- Producer/consumer-grade availability: single replicas for the data and platform services, no
  replication, no backups, no PodDisruptionBudgets, no horizontal pod autoscaling.
- Authentication, TLS, and network policy between components. The project rules put auth and
  multi-tenancy out of scope, and the stack is loopback-reachable only.
- Composer or any non-Kubernetes bring-up. The request asked for the cluster; `deploy/README.md`'s
  "compose first, then k3d" ordering is superseded and the divergence is recorded there.
- Grafana dashboards with panels. `dashboards/` stays a placeholder: the metrics the panels would
  read do not all have sources yet, and the repository's own rule is that a panel must read a metric
  that exists.

## Decisions

### D1 — Target the operator's kube context; verify, never provision

`deploy/cluster.sh` reads the current context, requires the API server to answer
(`kubectl version`/`kubectl get --raw=/readyz`), requires a default StorageClass, and creates the
three namespaces idempotently. It refuses to continue — naming the context and the missing fact —
when any of those fails.

*Alternatives considered.* (a) Provision a k3d cluster: not installed here, and the request's
"single-node cluster suitable for the full stack" is already satisfied by the machine's k3s.
(b) Detect a cluster and create one only when absent: two code paths and a heavier dependency, for a
machine that already has the cluster. (c) Use Helm: adds templating to a repo with no chart and
requires chart repositories, which are unset.

*Why this shape.* It keeps the script honest about what it did — it reports what it verified and
what it created — and it works unchanged against k3d, kind, or a remote cluster, because nothing in
it is k3s-specific except a documented note that the k3s kubeconfig needs root to read.

### D2 — One image, built locally, imported into the node's image store

`deploy/Dockerfile` is a multi-stage build parameterized by the binary name;
`deploy/images.sh` (invoked by `up.sh`) builds `controlplane`, `worker`, and `agent`, then imports
them into the cluster's container runtime. Manifests reference the images by tag with
`imagePullPolicy: IfNotPresent`.

The image-store import has two supported mechanisms, detected rather than assumed: `k3s ctr images
import` when a k3s container runtime is present, and `docker save` + `ctr -n k8s.io images import`
through a helper pod otherwise. The k3s path is what the development machine uses.

*Alternatives considered.* (a) A local registry container: one more long-lived component to run and
document, and its image also has to be imported to be reachable. (b) `docker build` per binary with
three Dockerfiles: three copies of the same recipe. (c) A `ko`-style build: another tool to install.

*Why this shape.* One recipe, one build, and a bring-up path that works on a machine with no
registry. `IfNotPresent` is what makes it work: the node never tries to pull a tag it already has.

### D3 — Kustomize base plus a demo overlay, one configuration object per workload

```
deploy/k8s/
  base/
    kustomization.yaml
    namespace-*.yaml            # the three namespaces
    config-controlplane.yaml    # ConfigMap: the mounted configuration file
    config-worker.yaml
    config-agent.yaml
    secret-data.yaml            # database and broker credentials
    mongodb-statefulset.yaml    mongodb-service.yaml
    mongodb-init-job.yaml       # replica-set initiation + data-model bootstrap
    rabbitmq-deployment.yaml    rabbitmq-service.yaml
    temporal-deployment.yaml    temporal-service.yaml
    temporal-ui-deployment.yaml temporal-ui-service.yaml
    prometheus-*.yaml  grafana-*.yaml  otel-collector-*.yaml
    controlplane-deployment.yaml controlplane-service.yaml
    worker-deployment.yaml
    agent-deployment.yaml
  overlays/demo/
    kustomization.yaml          # references ../../base, patches the demo values
    fleet-size.yaml             # the agent ConfigMap patch
    replicas.yaml               # the worker replica count
    resources.yaml              # the demo's resource sizing
```

`kubectl apply -k deploy/k8s/overlays/demo` is the apply step; `kubectl kustomize` is built into the
`kubectl` the repository already requires, so no new binary is needed to render or inspect the
overlay.

*Configuration per workload, not one shared object.* Each workload mounts its own ConfigMap holding
a complete configuration file. The file format is one document — `deploy/config.yaml` remains the
commented sample and the single place the format is explained — but a shared ConfigMap would force
every workload to carry values it does not consume, and an overlay patch would have to merge into a
nested mapping. Per-workload objects make a patch a whole-field replacement (`simulation.fleet_size`)
and keep the mounted file readable next to the workload that mounts it.

*Why a JSON patch rather than a strategic merge for the fleet size.* The configuration is an opaque
string in a ConfigMap, not a structured object kustomize can merge into, so the overlay replaces the
one field with a JSON patch and the base keeps the default. The alternative — a patch per workload
of the whole file — would duplicate the file in the overlay and let the two copies drift.

### D4 — MongoDB as a single-member replica set, initiated by a Job, addressed with `directConnection`

A `StatefulSet` (one replica, `serviceName` a headless Service) serves mongod with a persistent
volume for its data directory. A `Job` waits for mongod to answer, runs `rs.initiate()` for the
member `mongodb-0.mongodb.<data-namespace>.svc.cluster.local:27017`, waits for a primary, then
applies `deploy/mongo/init.js` — the repository's bootstrap, unchanged — through `mongosh`.
`up.sh` deletes a completed init Job and re-applies it, so the sequence runs on every bring-up and
is a no-op when the set is already initiated and the bootstrap already applied.

The application configuration's MongoDB URI carries `replicaSet=<name>&directConnection=true`. The
`directConnection` parameter is what makes the advertised member hostname irrelevant: a client
dials the service, asks the member directly, and never resolves the in-cluster name from outside.

*Alternatives considered.* (a) An `initContainer` on the worker: it would make one consumer
responsible for the topology every service depends on, and a worker restart would re-run it.
(b) `rs.initiate()` from the StatefulSet's own entrypoint: a second start of a pod whose volume was
wiped cannot re-initiate safely, and the initiation would be retried inside a process that is also
serving traffic. (c) A `timeseries` collection or a standalone mongod: the first contradicts the
recorded data model, the second fails the worker's startup by design.

*Why the Job.* Initiation is a one-time cluster-level fact with a clear success condition, and a Job
is the primitive that expresses it, retried by the operator's own bring-up rather than by the
service that happens to start first.

### D5 — Temporal as the development server plus the UI as its own deployment

`temporal server start-dev --headless` runs as the engine, with its SQLite file on a persistent
volume so workflow history survives a pod restart. The UI runs the `temporalio/ui` image configured
with `TEMPORAL_ADDRESS` pointing at the engine's Service and `TEMPORAL_DEFAULT_NAMESPACE` set to the
namespace the applications use. Because the operator reaches the UI through a forwarded port, the
UI's CORS allowance is widened for local development only (`TEMPORAL_CORS_ORIGINS` set to the
forwarded origin, or the unsafe-allow-all flag the image documents for local use), and the choice is
recorded in `deploy/README.md` as a development-only setting.

*Alternatives considered.* (a) The full Temporal server with Cassandra or PostgreSQL: four more
workloads and a datastore for a demo that needs durable history, not durability under failure.
(b) `temporal server start-dev` with the UI bundled on the dev server's own port: fewer objects, but
the UI then shares a process with the engine, so the "UI not required for the stack" requirement
could not hold. (c) A second namespace: the applications' configuration names one namespace, and a
namespace per environment is not a local-stack concern.

### D6 — Metrics from the endpoints that exist, discovery avoided

Prometheus runs with a static scrape configuration over Service addresses: the control plane's
metrics port, plus Prometheus itself. The control plane's Service exposes a named metrics port
alongside the gRPC and HTTP ports, so the scrape target is a stable in-cluster name rather than a pod
IP. No Kubernetes service-discovery API is used, so the metrics store needs no RBAC beyond its
default service account.

*Alternatives considered.* (a) `kubernetes_sd_configs` with endpoint discovery: the idiomatic
choice, but it needs a ClusterRole to list endpoints and a relabeling config to maintain, to scrape
one target. (b) A per-pod scrape by annotation: no admission-side annotation exists on the workload
we control unless we add one, and the ports are static anyway. (c) `ServiceMonitor` CRDs: they
require the Prometheus Operator, a third-party dependency this change is avoiding.

*Why this shape.* The faithful rule in the goals is "collect from the endpoints that exist". A static
target listing is small, reviewable, and makes the absence of a worker metrics target a visible fact
rather than a silent one.

### D7 — The trace collector is deployed ready for stage 4 and documented as not yet fed

`otel/opentelemetry-collector` runs with an OTLP receiver and a debug or logging exporter, and the
application configuration's `observability.otel_endpoint` names its in-cluster address.

The honest part is that this value is read by no binary today, so the collector receives nothing.
Two alternatives were considered: leave the collector out entirely (then `make up` ships no tracing
component at all, and the stage-4 change has to add the workload as well as the instrumentation), or
point the applications at it and imply traces flow (the repository's observability rule forbids
implying a source that does not exist). Deploying it with the limitation stated in
`deploy/README.md` and in the `platform-services-deployment` capability is the middle path: the
component and its wiring are reviewable now, and the empty trace view is explained where an operator
will look for it.

### D8 — Bring-up and tear-down are ordered `kubectl` calls, and teardown asks before deleting data

`up.sh`, in order: verify the cluster (`cluster.sh`) → build and import images → apply the data
services and wait for the replica set to be initiated and the broker to accept connections → apply
the platform services → apply the application workloads → wait for readiness → print the operator
endpoints and the forwarding commands for them.

`down.sh` deletes in the reverse direction with `--wait`, applications first, then platform, then
data. It deletes persistent volume claims only when `--volumes` is passed, and it prints each claim
it removes. Ordering is expressed with `kubectl`'s own wait flags rather than sleeps, so a slow
cluster is waited for rather than raced.

*Alternatives considered.* (a) One `kubectl delete -k` over the whole overlay: the deletion order
would follow the rendered manifest order, which is the apply order, so the applications would go last
— the opposite of what a clean shutdown wants. (b) Deleting the namespaces: it takes everything with
it, including data, which is exactly the implicit volume deletion the lifecycle requirement forbids.
(c) `helm uninstall`: no chart.

### D9 — One emulator process, with the identity constraint written into the spec

The base runs the emulator as a single-replica `Deployment` whose fleet size comes from its mounted
configuration, and the demo overlay sizes that fleet. The `agent-emulator-deployment` capability
states the constraint rather than hiding it: two emulator processes would simulate the same device
identities, and the hub routes a device id to whichever stream registered last, so a command meant
for one process reaches the other. Parameterizing device identities is a configuration-contract
change — a new `internal/config` field, a new requirement in `runtime-config`, and a change to
`NewFleet` — and belongs in its own change with its own tests, not in a manifest change.

*Alternative considered:* scale the emulator to several replicas anyway and accept duplicate
identities. Rejected: the duplicate registrations are not merely cosmetic — a rollout dispatches
commands through those identities, and the demo's central claim is that commands reach the devices
they target.

### D10 — Namespace layout and labels

Three namespaces, created by `cluster.sh`: `fleetops` (application services), `fleetops-data`
(MongoDB, RabbitMQ), `fleetops-platform` (Temporal, its UI, Prometheus, Grafana, the collector).
Every object carries `app.kubernetes.io/part-of: fleetops` and `app.kubernetes.io/name: <component>`,
which is what `down.sh` and any later selective teardown select on. The namespace names are
constants in `cluster.sh` and appear in the manifests; the README records them as the stack's public
layout.

### D11 — Probes, resources, and the agent's absence of one

Control plane and worker pods take their readiness from `/readyz` and their liveness from
`/healthz` on the configured health port — the endpoints the `health-readiness` capability already
fixes — with thresholds wide enough that a dependency outage marks a pod unready rather than
restarting it. Every container declares CPU and memory requests and limits, sized so the whole stack
fits a single-node cluster; the demo overlay is where those sizes are tuned. The emulator declares
resources but no probe, because it serves no health surface by specification; its pod status is its
observability, which the `agent-emulator-deployment` scenarios assert.

## Risks / Trade-offs

- **[The k3s kubeconfig needs root]** → `cluster.sh` detects the unreadable kubeconfig and reports
  the exact remedy (copy it with the right mode, or use a kubeconfig the operator's user can read)
  instead of failing with a permission error mid-apply.
- **[The image import path is cluster-specific]** → Two mechanisms are detected rather than assumed,
  and the failure mode is a clear message naming which runtime was found and how to import manually.
- **[A single-node cluster with a single replica of everything]** → The stack is sized to fit; a
  resource request the node cannot satisfy shows up as a pending pod with a scheduling message, which
  is one of the specified scenarios rather than a silent degradation.
- **[`directConnection=true` hides replica-set topology]** → Correct for one member and documented;
  a multi-member set later must drop it and make member hostnames resolvable, which the design notes
  where the URI is declared.
- **[Temporal's development server is not a production configuration]** → Recorded in
  `deploy/README.md` as the demo's deliberate choice, together with what would replace it.
- **[Widening the UI's CORS allowance for local development]** → Scoped to the UI container's own
  environment in the demo overlay, with the flag named in the README as development-only.
- **[An init Job that does not re-run after a volume wipe]** → `up.sh` deletes the completed Job and
  re-applies it on every bring-up, and the Job's own steps are idempotent, so the sequence runs
  whenever bring-up runs.
- **[The worker serves no metrics]** → Prometheus does not claim to scrape it; the gap is stated in
  the README and in the `platform-services-deployment` capability so the next change that adds the
  endpoint also adds the target.
- **[Duplicate device identities if the emulator is scaled]** → Specified as a requirement rather
  than left to discovery; the follow-up change that parameterizes identities is named in the design
  and in the README.

## Migration Plan

There is no existing deployment to migrate from: `make up` and `make down` currently do nothing, so
the change is additive for every environment. The steps are the implementation order itself — images,
then namespaces and the cluster script, then data, platform, and application manifests, then the
overlay and the make targets, then the documentation. Rollback is `make down` followed by reverting
the change: because the base and overlay are additive files and the only edited targets are `up` and
`down` (which return to being no-ops when the scripts are removed), reverting leaves no partially
applied state behind — `kubectl delete -k deploy/k8s/overlays/demo --ignore-not-found` is the manual
escape hatch if the scripts themselves are gone.

## Open Questions

None that change the specs, the approach, or the task breakdown. The two values that are genuinely
tunable — the demo's fleet size, worker replica count, and resource sizes — are overlay data, and
their exact numbers are set during implementation against what the node can schedule.
