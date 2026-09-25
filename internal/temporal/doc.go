// Package temporal contains the FleetOps Temporal workflows and their activities: the
// long-lived per-device DeviceWorkflow entity, the RolloutWorkflow canary saga (waves with
// durable health windows and compensating rollback), and the FirmwareWorkflow. Not implemented
// yet — delivered at stage 2.
package temporal
