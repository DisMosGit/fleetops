package temporal

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrDeviceUnreachable reports a device whose workflow refuses every delivery, which is what a
// device the rollout cannot command at all looks like to its update activity.
var ErrDeviceUnreachable = errors.New("device workflow is unreachable")

// device is one scripted device in the world: the conclusions it schedules and the commands it has
// accepted.
type device struct {
	// concludes schedules a conclusion for the delivery at that position: entry i is the
	// outcome the device reports after its (i+1)-th delivery of the wave's command. An empty
	// entry means that delivery never concludes anything.
	concludes []ConcludedCommand
	// commands are the commands the device accepted, in delivery order.
	commands []CommandIssuedSignal
	// unreachable reports that the device's workflow refuses every delivery.
	unreachable bool
}

// deviceWorld is a scriptable stand-in for the fleet's device entity workflows. It is the device
// half of the rollout test world: the update activity's delivery seam signals it, and its state
// reader answers from it, so a workflow test exercises the real activity over devices whose
// behaviour the test scripts — a device that concludes successfully, one that concludes with a
// failure, one that never reports, and one whose delivery is refused.
//
// The world mirrors the entity's semantics the update activity depends on: a command becomes the
// device's last concluded command only once the delivery that concludes it has happened, a device
// that has not concluded reports its command as pending, and a device nothing has signalled has no
// workflow execution to read.
type deviceWorld struct {
	mu      sync.Mutex
	devices map[string]*device
}

// newDeviceWorld returns a device world in which every device accepts its command and never
// concludes it: scripts are what make a device report.
func newDeviceWorld() *deviceWorld {
	return &deviceWorld{devices: map[string]*device{}}
}

// machine returns a device's entry, creating it on first use. The caller holds the lock.
func (w *deviceWorld) machine(deviceID string) *device {
	if w.devices[deviceID] == nil {
		w.devices[deviceID] = &device{}
	}
	return w.devices[deviceID]
}

// concludeOnDelivery scripts a device to conclude the command it is delivered with the given
// outcome, on the given delivery (1 for the first). Deliveries before it leave the command
// unconcluded, so a device can be made to report later than its first delivery — which is how a
// test proves a device's result was found by the activity's wait rather than by its delivery.
func (w *deviceWorld) concludeOnDelivery(deviceID string, delivery int, outcome CommandOutcome, detail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	machine := w.machine(deviceID)
	// Everything but the addressed delivery is left unconcluded, so re-scripting a device that
	// was scripted to conclude immediately takes that conclusion back.
	concludes := make([]ConcludedCommand, delivery)
	concludes[delivery-1] = ConcludedCommand{Outcome: outcome, Detail: detail}
	machine.concludes = concludes
}

// succeed scripts a device to conclude the update successfully as soon as it is delivered.
func (w *deviceWorld) succeed(deviceIDs ...string) {
	for _, id := range deviceIDs {
		w.concludeOnDelivery(id, 1, OutcomeSucceeded, "")
	}
}

// failWith scripts a device to conclude the update with a failure carrying detail as soon as it is
// delivered.
func (w *deviceWorld) failWith(detail string, deviceIDs ...string) {
	for _, id := range deviceIDs {
		w.concludeOnDelivery(id, 1, OutcomeFailed, detail)
	}
}

// deleteConclusion un-scripts a device: it accepts its command and never concludes it, which is
// the device a wave stops waiting on and records as unreported.
func (w *deviceWorld) deleteConclusion(deviceIDs ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range deviceIDs {
		w.machine(id).concludes = nil
	}
}

// refuseDelivery makes a device's workflow unreachable: every delivery attempt fails, which is the
// case that fails a wave rather than reporting an outcome.
func (w *deviceWorld) refuseDelivery(deviceIDs ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range deviceIDs {
		w.machine(id).unreachable = true
	}
}

// SignalCommandIssued implements DeviceCommander: it accepts one command for one device, which is
// what makes the device's later deliveries possible.
func (w *deviceWorld) SignalCommandIssued(_ context.Context, cmd CommandIssuedSignal) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	machine := w.machine(cmd.DeviceID)
	if machine.unreachable {
		return fmt.Errorf("deliver %s to device %s: %w", cmd.CommandID, cmd.DeviceID, ErrDeviceUnreachable)
	}
	machine.commands = append(machine.commands, cmd)
	return nil
}

// State implements DeviceStateReader: it answers the device's state, which reports the command's
// conclusion once the delivery that concludes it has happened. A device nothing has signalled has
// no workflow execution to read.
func (w *deviceWorld) State(_ context.Context, deviceID string) (State, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	machine := w.devices[deviceID]
	if machine == nil || len(machine.commands) == 0 {
		return State{}, fmt.Errorf("read device %s state: %w", deviceID, ErrDeviceNotFound)
	}
	state := State{DeviceID: deviceID, CurrentFw: "1.0.0"}
	delivered := len(machine.commands)
	// The device reports the conclusion its delivery count has reached: a script that concludes
	// on a later delivery leaves the command pending until then, exactly as a real device that
	// takes a while does.
	for i := min(delivered, len(machine.concludes)) - 1; i >= 0; i-- {
		if machine.concludes[i].Outcome == "" {
			continue
		}
		concluded := machine.concludes[i]
		concluded.Command = machine.commands[i]
		state.LastCommand = &concluded
		return state, nil
	}
	state.Pending = &PendingCommand{Command: machine.commands[delivered-1], Dispatched: true}
	return state, nil
}
