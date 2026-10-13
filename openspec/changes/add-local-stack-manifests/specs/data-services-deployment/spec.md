# Spec Delta

## Purpose

Defines the stateful services the stack stores in and publishes through: the namespace they are
separated into, the volumes that outlive their pods, the replica-set topology the worker requires,
and the readiness that gates the application services depending on them.

## ADDED Requirements

### Requirement: Data services are separated from application services

The document database and the message broker SHALL be declared in a namespace of their own, separate
from the namespace holding FleetOps application services and from the one holding platform services,
and application workloads SHALL reach them by in-cluster service addresses.

#### Scenario: The services live in the data namespace

- **WHEN** the stack is running
- **THEN** the document database's workload and the broker's workload are both in the data
  namespace, and no FleetOps application workload is

#### Scenario: Application configuration addresses them in the cluster

- **WHEN** an application pod loads its mounted configuration
- **THEN** the document database URI and the broker URL name in-cluster service addresses rather than
  a loopback address, so the connection resolves from any pod

### Requirement: Data survives the pods that serve it

The document database and the message broker SHALL each store their data on a persistent volume
claim, and the data SHALL survive the pod being deleted, restarted, or rescheduled to another node.
Neither service's data SHALL live only in the container's writable layer.

#### Scenario: A deleted pod keeps the data

- **WHEN** the document database's pod is deleted while the stack is running and the workload
  recreates it
- **THEN** the collections, indexes, and stored firmware binaries are still present after the pod
  returns, because they were written to the persistent volume

#### Scenario: Broker state survives a restart

- **WHEN** the message broker's pod restarts
- **THEN** its declared exchanges, queues, and bindings are present after it returns, and messages
  that had been published persist according to the broker's own durability rules

### Requirement: The document database runs as a replica set before the worker starts

The document database's workload SHALL be a single-member replica set, and the stack SHALL initiate
that set and apply the repository's data-model bootstrap before the worker starts, because the
worker establishes a change stream over the device collection before it polls and fails startup
without one. The service's readiness SHALL NOT report ready before the set is initiated and a
primary is elected.

#### Scenario: A fresh cluster yields an initiated set

- **WHEN** the stack is applied to a cluster that has never run it
- **THEN** the document database's replica set is initiated with the member the workload serves,
  a primary is elected, and the readiness check reports ready

#### Scenario: The bootstrap is applied

- **WHEN** the document database first becomes a primary
- **THEN** the repository's bootstrap has been applied, so the collections, validators, and indexes
  a change stream and a device registration depend on exist

#### Scenario: Readiness waits for the set

- **WHEN** the database process is listening but the replica set has not been initiated
- **THEN** the workload does not report ready, so dependent services are not started against a
  topology that would fail their startup

### Requirement: Broker readiness gates the services that publish and consume

The message broker SHALL accept AMQP connections only after it is ready to serve them, the stack
SHALL gate the application services on that readiness, and the broker's credentials SHALL come from
a configuration object rather than being written into the workload's manifests as literals.

#### Scenario: Broker readiness gates the worker

- **WHEN** the broker is not yet accepting connections
- **THEN** the worker's readiness endpoint reports the broker check failing, so bring-up does not
  report the stack up

#### Scenario: Credentials are not literals in the manifests

- **WHEN** an operator inspects the stack's manifests and the objects they create
- **THEN** the broker's and the database's credentials are read from a configuration object that the
  manifests reference, rather than written as literals in the workload's specification
