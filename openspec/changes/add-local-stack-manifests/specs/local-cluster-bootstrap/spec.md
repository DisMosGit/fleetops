# Spec Delta

## Purpose

Defines how the FleetOps local stack attaches itself to the Kubernetes cluster an operator already
has: what bring-up verifies before it applies anything, which namespaces exist and what belongs in
each, and how the stack behaves when the cluster cannot run it.

## ADDED Requirements

### Requirement: The stack runs against a cluster the operator supplies

The stack SHALL deploy to the cluster selected by the operator's current kube context and SHALL NOT
create, replace, or delete a cluster. Bring-up SHALL first verify that the selected context is
reachable — the API server answers — and SHALL stop with an error naming the context when it is not,
rather than applying manifests to a cluster it cannot confirm.

#### Scenario: A reachable context is accepted

- **WHEN** the operator runs bring-up with a kube context whose API server answers
- **THEN** the verification passes, reports the context and server it verified, and bring-up
  continues to apply manifests

#### Scenario: An unreachable context stops bring-up

- **WHEN** the operator runs bring-up with no kube context configured, or with one whose API server
  does not answer
- **THEN** bring-up stops before applying anything, naming the context and the failure, and exits
  non-zero

#### Scenario: No cluster is created as a side effect

- **WHEN** bring-up verifies its cluster and applies the stack
- **THEN** no cluster, node, or cluster-scoped resource is created or deleted, so the set of
  clusters on the machine is exactly what it was before

### Requirement: Namespaces separate application services from the services they depend on

The stack SHALL declare its workloads across three namespaces: one for FleetOps application
services, one for the data services a FleetOps service stores in or publishes through, and one for
the platform services the applications are configured to talk to. Application workloads SHALL NOT be
declared in the data or platform namespace.

#### Scenario: Each workload lands in its own namespace

- **WHEN** the stack is applied and every workload is running
- **THEN** the application services are in the application namespace, the document database and the
  message broker are in the data namespace, and the workflow engine and the observability
  components are in the platform namespace

#### Scenario: Namespace creation is idempotent

- **WHEN** bring-up runs against a cluster where the namespaces already exist and hold a running
  stack
- **THEN** it reports the namespaces as existing, creates nothing, and leaves every running workload
  untouched

### Requirement: Bring-up requires storage the cluster can actually provision

Persistent volume claims SHALL bind through the cluster's default StorageClass, and bring-up SHALL
verify that a default StorageClass exists and can provision a volume before applying the workloads
that claim one. A cluster without one SHALL stop bring-up with an error naming the missing
StorageClass, rather than leaving the data services pending on unbound claims.

#### Scenario: A default StorageClass is accepted

- **WHEN** the cluster has a default StorageClass that dynamically provisions volumes
- **THEN** verification passes and bring-up applies the workloads, whose claims bind

#### Scenario: A cluster with no default StorageClass stops bring-up

- **WHEN** the cluster has no StorageClass marked default
- **THEN** bring-up stops before applying the data workloads, names the missing default
  StorageClass, and exits non-zero

### Requirement: Verification reports what it found

Bring-up's verification step SHALL report, before any manifest is applied: the kube context and
server it verified, each namespace with whether it already existed or was created, and the
StorageClass the volumes will bind through. It SHALL be re-runnable: a second run reports the same
namespaces as existing rather than as created.

#### Scenario: The report precedes the apply

- **WHEN** the operator runs bring-up
- **THEN** the cluster, namespace, and storage facts are reported before the first manifest is
  applied, so a failure to apply never hides what the verification found

#### Scenario: A second run reports no creation

- **WHEN** bring-up is run twice against the same cluster
- **THEN** the first run reports the namespaces as created and the second reports them as existing,
  and both exit successfully
