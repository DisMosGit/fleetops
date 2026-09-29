package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/DisMosGit/fleetops/internal/devices"
)

// DeviceInventoryOutcome names how one device's recorded firmware version ended up, as the rollback
// step that dispatched the reconciliation needs to know.
type DeviceInventoryOutcome string

// The outcomes one device's reconciliation reports. They are total: every device the rollback
// touched is reported as exactly one of them, so nothing is recorded as verified that was not.
const (
	// InventoryAgreed means the recorded version already matched the version the device holds.
	InventoryAgreed DeviceInventoryOutcome = "agreed"
	// InventoryCorrected means the record held another version and now holds the reported one.
	InventoryCorrected DeviceInventoryOutcome = "corrected"
	// InventoryUnverified means the comparison could not be made: the device's workflow holds no
	// firmware version, or the fleet holds no record for it.
	InventoryUnverified DeviceInventoryOutcome = "unverified"
)

// DeviceInventory is one device's reconciliation as its activity reported it back to the rollback
// step.
type DeviceInventory struct {
	// DeviceID is the device that was reconciled.
	DeviceID string `json:"device_id"`
	// Outcome is how the reconciliation ended.
	Outcome DeviceInventoryOutcome `json:"outcome"`
	// Version is the firmware version the device was found to run: the version the record holds
	// once the activity returned. It is empty for a device that could not be verified, which is
	// never counted as running any version.
	Version string `json:"version,omitempty"`
	// Detail is the reason a device could not be verified; empty otherwise.
	Detail string `json:"detail,omitempty"`
}

// ReconcileInventoryRequest is the reconcile-device-inventory activity's request: one device of one
// compensated wave to reconcile the fleet's record of.
type ReconcileInventoryRequest struct {
	// RolloutID is the rollout that is rolling back.
	RolloutID string `json:"rollout_id"`
	// WaveID is the wave whose devices this device belongs to; it names the step in the record.
	WaveID string `json:"wave_id"`
	// DeviceID is the device to reconcile.
	DeviceID string `json:"device_id"`
}

// NewReconcileInventoryActivity returns the reconcile-device-inventory activity bound to the
// device-state reader and the fleet's firmware inventory: it reads the version the device's
// workflow — the authority on its own firmware — holds, reconciles the fleet's record against it,
// and reports whether they agreed, the record was corrected, or the comparison could not be made.
//
// A device whose workflow holds no version is unverified rather than corrected to the empty one: a
// record is only ever corrected to a version a device reported. A read or write that fails for
// another reason is an error, which the caller's retry policy owns; a failure that outlives those
// retries is recorded by the step as an unverified device with its reason.
func NewReconcileInventoryActivity(
	states DeviceStateReader,
	inventory FirmwareInventory,
) func(ctx context.Context, req ReconcileInventoryRequest) (DeviceInventory, error) {
	return func(ctx context.Context, req ReconcileInventoryRequest) (DeviceInventory, error) {
		if req.DeviceID == "" {
			return DeviceInventory{}, errors.New("reconcile device inventory: device id required")
		}
		state, err := states.State(ctx, req.DeviceID)
		if err != nil {
			return DeviceInventory{}, fmt.Errorf("read device %s state: %w", req.DeviceID, err)
		}
		if state.CurrentFw == "" {
			return DeviceInventory{
				DeviceID: req.DeviceID,
				Outcome:  InventoryUnverified,
				Detail:   fmt.Sprintf("device %s holds no firmware version", req.DeviceID),
			}, nil
		}

		result, err := inventory.ReconcileFirmware(ctx, req.DeviceID, state.CurrentFw)
		if err != nil {
			return DeviceInventory{}, fmt.Errorf("reconcile device %s firmware: %w", req.DeviceID, err)
		}
		switch result {
		case devices.FirmwareMissing:
			return DeviceInventory{
				DeviceID: req.DeviceID,
				Outcome:  InventoryUnverified,
				Detail:   fmt.Sprintf("the fleet holds no record for device %s", req.DeviceID),
			}, nil
		case devices.FirmwareCorrected:
			return DeviceInventory{
				DeviceID: req.DeviceID,
				Outcome:  InventoryCorrected,
				Version:  state.CurrentFw,
			}, nil
		default:
			return DeviceInventory{
				DeviceID: req.DeviceID,
				Outcome:  InventoryAgreed,
				Version:  state.CurrentFw,
			}, nil
		}
	}
}
