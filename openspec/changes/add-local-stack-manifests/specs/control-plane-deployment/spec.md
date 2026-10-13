# Spec Delta

## Purpose

Defines the control plane's presence in the local stack: the ports it serves and how each is
reached, the probes that decide whether a replica is alive and ready, and the resources it declares
before it is scheduled.

## ADDED Requirements

### Requirement: The control plane serves its ports and its gRPC surface is discoverable in the cluster

The control plane's workload SHALL serve the gRPC agent surface, the operator HTTP gateway, the
metrics endpoint, and the liveness and readiness endpoints, all on the ports the mounted
configuration names. A `Service` SHALL expose the gRPC port inside the cluster under a stable DNS
name, so an emulator replica reaches the control plane without knowing which pod serves it.

#### Scenario: A simulated fleet reaches the control plane by service name

- **WHEN** the agent emulator runs with its configured control-plane address set to the control
  plane's in-cluster service address
- **THEN** devices register and heartbeat successfully, so the control plane's gRPC surface is
  reachable through the service rather than through a specific pod

#### Scenario: An operator reaches the HTTP gateway

- **WHEN** an operator forwards the control plane's HTTP port to their machine and calls the
  rollout list endpoint
- **THEN** the gateway answers with the API's response rather than a connection error

#### Scenario: Every served port is reachable through the pod

- **WHEN** each of the control plane's ports is queried from inside the cluster
- **THEN** the gRPC port accepts a connection, the HTTP port answers, the metrics port serves the
  metrics exposition, and the health port answers both probe endpoints

### Requirement: Probes gate the control plane on its documented endpoints

The control plane's liveness probe SHALL query the liveness endpoint and its readiness probe SHALL
query the readiness endpoint, both on the port the configuration names, and the workload SHALL
declare resource requests and limits on every container. A replica whose readiness endpoint reports
a dependency failure SHALL NOT be sent traffic.

#### Scenario: A healthy replica reports ready and takes traffic

- **WHEN** the control plane's dependencies are reachable and its readiness endpoint answers
  successfully
- **THEN** the pod reports ready and the gRPC service routes connections to it

#### Scenario: A replica with an unreachable dependency is removed from service

- **WHEN** a control plane replica cannot reach a dependency its readiness endpoint checks
- **THEN** the pod reports not ready, no service traffic is routed to it, and the pod is not
  restarted for it, because the liveness probe still passes

#### Scenario: A pod that cannot be scheduled is reported, not silently pending

- **WHEN** the cluster cannot satisfy the workload's declared resource requests
- **THEN** the pod stays pending with a scheduling message naming the resource it lacks, rather than
  running under-specified

### Requirement: One configuration object configures the control plane

The control plane SHALL read its configuration from the single YAML file the entrypoint loads,
mounted read-only from a configuration object the stack declares, and the stack SHALL provide the
same configuration object to every component that consumes the file. The mounted file SHALL use
in-cluster addresses for the services the control plane reaches, not the loopback addresses the
development defaults carry.

#### Scenario: The mounted file reaches the binary

- **WHEN** the control plane's container starts
- **THEN** it loads the mounted configuration file, and its startup log reports the addresses the
  file carries rather than the built-in defaults

#### Scenario: An invalid mounted file fails loudly

- **WHEN** the mounted configuration object carries a value the configuration loader rejects
- **THEN** the container exits non-zero naming the offending field, and the pod's status shows the
  failure rather than a running container with default endpoints
