package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	agentv1 "github.com/DisMosGit/fleetops/api/proto/agent/v1"
)

// StatusReporter delivers firmware-update progress reports to the control plane. *Client
// satisfies it — the interface lives here because the update handler consumes it.
type StatusReporter interface {
	// ReportUpdateStatus delivers one progress report and returns its acknowledgment.
	ReportUpdateStatus(ctx context.Context, req *agentv1.UpdateStatusRequest) (*agentv1.UpdateStatusResponse, error)
}

// UpdateReporter reports one firmware update back to the control plane. Progress and outcomes
// travel as update status over the stream — fine-grained and frequent, acknowledged per
// report — and the terminal command result travels as the unary report under an idempotency
// key derived from the command id, so a redelivered result reuses its key and can never
// produce a second effect.
type UpdateReporter struct {
	status  StatusReporter
	results agentv1.AgentServiceClient
}

// NewUpdateReporter returns a reporter sending update status through status and terminal
// command results through results.
func NewUpdateReporter(status StatusReporter, results agentv1.AgentServiceClient) *UpdateReporter {
	return &UpdateReporter{status: status, results: results}
}

// Status reports one update phase for a device's firmware. An unaccepted or undelivered
// report is an error: nothing was recorded, and the caller decides whether to report again.
func (r *UpdateReporter) Status(
	ctx context.Context,
	deviceID, firmwareID string,
	phase agentv1.UpdatePhase,
	percent int32,
	detail string,
) error {
	ack, err := r.status.ReportUpdateStatus(ctx, &agentv1.UpdateStatusRequest{
		DeviceId:        deviceID,
		FirmwareId:      firmwareID,
		Phase:           phase,
		ProgressPercent: percent,
		Detail:          detail,
	})
	if err != nil {
		return fmt.Errorf("report update status of %s: %w", deviceID, err)
	}
	if !ack.GetAccepted() {
		return fmt.Errorf("report update status of %s: not accepted", deviceID)
	}
	return nil
}

// Conclude reports the terminal result of one command. Its idempotency key is derived from
// the command id alone, so every redelivery of the same result carries the same key.
func (r *UpdateReporter) Conclude(
	ctx context.Context,
	cmd *agentv1.Command,
	outcome agentv1.CommandOutcome,
	detail string,
) error {
	res, err := r.results.Report(ctx, &agentv1.ReportRequest{
		IdempotencyKey: reportKey(cmd.GetCommandId()),
		CommandId:      cmd.GetCommandId(),
		DeviceId:       cmd.GetDeviceId(),
		Outcome:        outcome,
		Detail:         detail,
	})
	if err != nil {
		return fmt.Errorf("report command %s: %w", cmd.GetCommandId(), err)
	}
	if !res.GetAccepted() {
		return fmt.Errorf("report command %s: not accepted", cmd.GetCommandId())
	}
	return nil
}

// reportKey derives the idempotency key of one command's terminal result from its command id.
func reportKey(commandID string) string { return commandID }

// StatusRelay forwards update status reports to a stream client bound after construction.
// The client takes the command handler at construction and the handler reports through the
// client, so the relay breaks that construction cycle: cmd/agent binds the client once it
// exists, and a report arriving before that fails loudly instead of vanishing.
type StatusRelay struct {
	mu     sync.Mutex
	client StatusReporter
}

// Bind attaches the status reporter the relay forwards to.
func (r *StatusRelay) Bind(client StatusReporter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = client
}

// ReportUpdateStatus forwards one progress report to the bound client.
func (r *StatusRelay) ReportUpdateStatus(
	ctx context.Context,
	req *agentv1.UpdateStatusRequest,
) (*agentv1.UpdateStatusResponse, error) {
	r.mu.Lock()
	client := r.client
	r.mu.Unlock()
	if client == nil {
		return nil, errors.New("update status: no stream client bound")
	}
	return client.ReportUpdateStatus(ctx, req)
}

// ReportUpdateStatus delivers one firmware-update progress report to the control plane and
// returns its acknowledgment. It rides the Connect stream's correlated request path: a report
// sent on a stream that dies unanswered is resent on the next stream under its original
// correlation id, like every other agent request.
func (c *Client) ReportUpdateStatus(
	ctx context.Context,
	req *agentv1.UpdateStatusRequest,
) (*agentv1.UpdateStatusResponse, error) {
	resp, err := c.request(ctx, &agentv1.AgentEnvelope{
		CorrelationId: c.ids.Next("corr"),
		Payload:       &agentv1.AgentEnvelope_UpdateStatus{UpdateStatus: req},
	})
	if err != nil {
		return nil, err
	}
	ack := resp.GetUpdateStatus()
	if ack == nil {
		return nil, fmt.Errorf("update status of %s: response carries no acknowledgment", req.GetDeviceId())
	}
	return ack, nil
}
