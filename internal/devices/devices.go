package devices

import "time"

// Device presence statuses recorded in the devices collection.
const (
	// StatusOnline marks a device the control plane has heard from recently.
	StatusOnline = "online"
	// StatusOffline marks a device that stopped reporting.
	StatusOffline = "offline"
)

// Record is one devices document: the identity fields an agent registers with, the presence
// status, and the time the control plane last heard from the device.
type Record struct {
	// ID is the stable device identity; the _id of the devices document.
	ID string
	// Model is the device model, part of the rollout target group.
	Model string
	// Region is the device region, part of the rollout target group.
	Region string
	// CurrentFw is the firmware version the device reports running.
	CurrentFw string
	// Status is the presence status (StatusOnline or StatusOffline).
	Status string
	// LastSeen is the time the control plane last heard from the device.
	LastSeen time.Time
}

// Update is one device-state refresh produced by heartbeat ingestion: the newer last-seen
// time and the state the device reported with it.
type Update struct {
	// ID is the device identity the refresh belongs to.
	ID string
	// CurrentFw is the firmware version the device reported.
	CurrentFw string
	// Status is the presence status to record.
	Status string
	// LastSeen is the time the control plane accepted the heartbeat.
	LastSeen time.Time
}
