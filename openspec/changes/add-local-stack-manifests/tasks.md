# Tasks

Two verification commands recur, and both are confirmed to work in this repository:

- Render and validate the manifests locally: `kubectl kustomize deploy/k8s/overlays/demo` (renders
  with no cluster contact) and
  `sudo k3s kubectl apply --dry-run=server -k deploy/k8s/overlays/demo` (validates against the API
  server, including admission).
- The engineering gates: `make check`, which wraps the four Definition of Done commands.

## 1. Container images

- [ ] 1.1 Add `deploy/Dockerfile`: a multi-stage build with a pinned Go builder stage, a module and
  build cache layer that survives source changes, and a final stage selected by an `ARG BINARY`
  build argument so one recipe builds `controlplane`, `worker`, and `agent`; verify all three build
  with `docker build --build-arg BINARY=controlplane -t fleetops/controlplane:dev deploy/` (and the
  same for `worker` and `agent`)
- [ ] 1.2 Add `deploy/images.sh` building the three images and importing them into the cluster's
  container runtime, detecting the k3s runtime (import through `k3s ctr`) and falling back to
  `docker save` piped into `ctr -n k8s.io images import`, failing with a message naming what it
  detected; verify it runs end to end and that `sudo k3s ctr images ls | grep fleetops` lists all
  three tags, and that `bash -n deploy/images.sh` reports no syntax error
- [ ] 1.3 Document the image build and import path in `deploy/README.md`, including why
  `imagePullPolicy: IfNotPresent` is what makes a registry-less cluster work; verify every command
  the section shows runs as written

## 2. Cluster and namespace script

- [ ] 2.1 Add the three namespace manifests under `deploy/k8s/base/` and a `base/kustomization.yaml`
  listing them, with the stack's standard labels
  (`app.kubernetes.io/part-of: fleetops`, `app.kubernetes.io/name: <component>`) on every object;
  verify `kubectl kustomize deploy/k8s/base` renders exactly three Namespace objects and nothing else
- [ ] 2.2 Add `deploy/cluster.sh`: verify a kube context is reachable, verify a default StorageClass
  exists, create the namespaces idempotently, and report the context, server, StorageClass, and each
  namespace with whether it was created or already existed; detect the unreadable k3s kubeconfig and
  report the remedy rather than failing mid-apply; verify it is re-runnable — running it twice
  reports `created` once and `exists` on the second run — and that `bash -n deploy/cluster.sh` reports
  no syntax error
- [ ] 2.3 Verify the script's failure paths against the real cluster: with a kubeconfig pointing at a
  port nothing listens on it exits non-zero naming the context, and the missing-StorageClass path is
  exercised by running it against a context whose cluster has none; verify the first with a temporary
  kubeconfig and record both observed messages in `deploy/README.md`'s troubleshooting section

## 3. Data services and the configuration objects

- [ ] 3.1 Add `deploy/k8s/base/mongodb-statefulset.yaml` (one replica, headless `serviceName`, a
  persistent volume claim template) and `mongodb-service.yaml`; verify
  `sudo k3s kubectl apply --dry-run=server -k deploy/k8s/base` accepts it once the namespaces exist
  and `kubectl kustomize deploy/k8s/base` shows the claim and the headless Service
- [ ] 3.2 Add `deploy/k8s/base/mongodb-init-job.yaml`: wait for mongod to answer, run `rs.initiate()`
  for the StatefulSet member's in-cluster name, wait for a primary, then apply the repository's
  `deploy/mongo/init.js` through `mongosh`; make every step a no-op when it is already done; verify
  the Job succeeds against the cluster (`sudo k3s kubectl wait --for=condition=complete job/...`) and
  that `sudo k3s kubectl exec statefulset/mongodb -- mongosh --quiet --eval 'rs.status().ok'` reports
  `1`
- [ ] 3.3 Add `deploy/k8s/base/rabbitmq-deployment.yaml`, `rabbitmq-service.yaml`, and
  `secret-data.yaml` carrying the broker and database credentials; verify the broker's pod reports
  ready, `kubectl kustomize deploy/k8s/base` renders the Secret rather than literals in either
  workload, and no manifest under `deploy/k8s/` contains a plaintext password
  (`grep -rn "guest:guest" deploy/k8s/` reports nothing)
- [ ] 3.4 Add the three application configuration objects —
  `config-controlplane.yaml`, `config-worker.yaml`, `config-agent.yaml` — each holding a complete
  configuration file shaped like `deploy/config.yaml` with in-cluster addresses for MongoDB (with
  `replicaSet` and `directConnection=true`), RabbitMQ, and Temporal, so groups 4 and 5 only add
  workloads that mount them; verify `python3 -c "import yaml,sys; [yaml.safe_load(open(p)) for p in sys.argv[1:]]" deploy/k8s/base/config-*.yaml` parses every file and each file's keys are a subset
  of the field names in `deploy/config.yaml`
- [ ] 3.5 Document the data namespace, the credentials object, and the configuration-object convention
  in `deploy/README.md`, replacing the "Not implemented yet" paragraph's claim about manifests;
  verify the file no longer contains that claim (`grep -c "Not implemented yet" deploy/README.md` is
  `0`) and that the documented namespace names match the manifests

## 4. Platform services

- [ ] 4.1 Add `deploy/k8s/base/temporal-deployment.yaml` and `temporal-service.yaml`: the Temporal
  development server, headless, with its persistence file on a persistent volume; verify the pod
  reports ready, a client in the cluster connects on the frontend port
  (`sudo k3s kubectl exec deploy/worker -- ...` once group 5 lands, or a throwaway pod before then),
  and deleting the pod leaves the workflow history it had recorded
- [ ] 4.2 Add `deploy/k8s/base/temporal-ui-deployment.yaml` and `temporal-ui-service.yaml`, wired
  with `TEMPORAL_ADDRESS` to the engine's service and `TEMPORAL_DEFAULT_NAMESPACE` to the
  applications' namespace, with the local-development CORS setting named as development-only in the
  manifest's own comment; verify the UI answers over a forwarded port and lists the namespace's
  workflow executions (`sudo k3s kubectl port-forward svc/temporal-ui 8080:8080` then `curl -sf
  localhost:8080/api/v1/namespaces/default/workflows | head`)
- [ ] 4.3 Add the metrics store: a deployment with a persistent volume and a static scrape
  configuration naming the control plane's metrics port and the store itself, plus the store's
  service; verify the store's own health endpoint reports ready, its configuration lists no target
  for a component that serves no metrics endpoint, and a query for a control plane series returns
  data after the control plane is running
- [ ] 4.4 Add the dashboard with a provisioned data source pointing at the metrics store and an admin
  credential read from a configuration object; verify the provisioned data source answers a query
  without being added by hand
- [ ] 4.5 Add the trace collector with an OTLP receiver and a verbose local exporter, and set
  `observability.otel_endpoint` in the three configuration objects to its in-cluster address; verify
  the collector accepts a connection on its OTLP port and that `deploy/README.md` states the
  applications do not yet export traces
- [ ] 4.6 Document the platform namespace, the engine's development-server posture, and the
  observability gap in `deploy/README.md`; verify every command the new section shows runs as written
  against the running stack

## 5. Application workloads

- [ ] 5.1 Add `deploy/k8s/base/controlplane-deployment.yaml` and `controlplane-service.yaml`: the
  gRPC port with its Service, the HTTP gateway, the metrics port, the health port, `/healthz` and
  `/readyz` probes, resource requests and limits, and the group 3 configuration object mounted
  read-only; verify both probes answer through the pod
  (`sudo k3s kubectl exec deploy/controlplane -- wget -qO- localhost:8081/readyz`) and the gRPC
  Service resolves in the cluster
- [ ] 5.2 Add `deploy/k8s/base/worker-deployment.yaml`: replicas declared so the overlay can change
  them, the health port, `/healthz` and `/readyz` probes, resource requests and limits, and the
  worker's configuration object mounted; verify the pod reports ready only once MongoDB is a replica
  set and the broker is reachable — with the broker stopped the readiness endpoint reports it failing
  while the pod stays running
- [ ] 5.3 Add `deploy/k8s/base/agent-deployment.yaml`: one replica, resource requests and limits, no
  probe (the emulator serves no health surface), a termination grace period long enough for its
  fleet to stop, and its configuration object mounted with the control plane's in-cluster service
  address; verify devices register
  (`sudo k3s kubectl exec deploy/controlplane -- wget -qO- localhost:9091/metrics | grep -c heartbeat`)
  and that deleting the pod leaves no restart loop
- [ ] 5.4 Verify the worker's replica knob and the change-stream dependency against the cluster:
  scale the worker to two replicas and confirm both report ready and both execute tasks, scale back
  to one and confirm the count stays after `kubectl apply -k` runs again, and confirm the worker
  fails its startup against a standalone database — the last by pointing a throwaway worker at the
  same database over `directConnection=true` without the replica set parameters
- [ ] 5.5 Document the application namespace, the replica knob, and the single-emulator identity
  constraint in `deploy/README.md` and `cmd/README.md`; verify the documented scaling command runs as
  written (`sudo k3s kubectl -n fleetops scale deploy/worker --replicas=3`) and that the identity
  constraint names the unparameterized device numbering in `internal/agent`

## 6. Demo overlay, one command up, one command down

- [ ] 6.1 Add `deploy/k8s/overlays/demo/kustomization.yaml` with the fleet-size patch, the worker
  replica count, and the resource sizing; verify `kubectl kustomize deploy/k8s/overlays/demo` renders
  the demo's values and no demo value appears under `deploy/k8s/base/`
  (`grep -rn "fleet_size" deploy/k8s/base/` shows only the base default)
- [ ] 6.2 Add `deploy/up.sh`: verify the cluster, build and import the images, apply the data
  services and wait for the replica set and the broker, apply the platform services, apply the
  application workloads, wait for readiness, and print each operator endpoint with the command that
  forwards it; verify a cold bring-up exits successfully with the stack running and that a second
  bring-up restarts no pod (`sudo k3s kubectl get pods -A -o jsonpath=...` before and after, comparing
  creation timestamps)
- [ ] 6.3 Add `deploy/down.sh`: delete applications, then platform, then data, all with `--wait` and
  `--ignore-not-found`, and delete persistent volume claims only when `--volumes` is passed, printing
  each claim it removes; verify a default run leaves the claims in place, a run with `--volumes`
  removes them and names them, and a second run of either exits successfully
- [ ] 6.4 Replace the `up` and `down` targets in the `Makefile` with the scripts and add the sibling
  targets the scripts need; verify `make help` lists the new targets and `make up` on an already-up
  stack succeeds without restarting a pod
- [ ] 6.5 Update `deploy/README.md` with the two commands and the follower ordering a demo obeys (the
  emulator registers devices before a rollout has a fleet to target), and update `README.md`'s
  delivery-status table so it no longer claims stages 1–5 are "Not started"; verify
  `grep -n "Not started" README.md` reports nothing and the commands the README shows run as written

## 7. Integration verification

- [ ] 7.1 Run the acceptance sequence on the real cluster and record what was observed in
  `## Recorded verification` below: `deploy/down.sh --volumes`, then `make up` on a stack that is not
  running, then `make up` again, then verifying the running stack — every workload ready, the
  replica set initiated, the bootstrap applied, the dashboard's data source answering, the UI listing
  the namespace, and the emulator's devices registered and heartbeating
- [ ] 7.2 Run the engineering gates over the change: `make check` passes, so the four Definition of
  Done commands are green with no Go file touched by this change
- [ ] 7.3 Verify the stack's own documentation commands are executable as written by running each one
  in `deploy/README.md`'s bring-up, scaling, and troubleshooting sections against the live stack, and
  record any command that needed correction
- [ ] 7.4 Verify the spec scenarios that need a live cluster and cannot be asserted offline: the
  gRPC service reachable from an emulator pod, the worker removed from service when a dependency is
  down, the restored data after a default teardown followed by bring-up, and the two spec scenarios
  named in `add-device-search-attributes-snapshot-and-worker`'s deferred verification (`temporal-worker`
  "Two replicas share the load" and "Replica loss changes nothing observable"); record the outcome of
  each in `## Recorded verification`

## Recorded verification

To be written during group 7. Record the observed result of each verification step against the
running stack on the day it is run — not the expected result — including the final resource sizing
the demo overlay carries, and any divergence from the values this plan predicted.
