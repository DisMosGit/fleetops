package temporal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
)

// fakeQueryClient is a hand-written queryClient double recording query calls and answering with
// a scripted value or failure.
type fakeQueryClient struct {
	mu    sync.Mutex
	calls []queryCall
	value converter.EncodedValue
	err   error
}

// queryCall is one recorded query.
type queryCall struct {
	workflowID string
	queryType  string
	runID      string
}

func (c *fakeQueryClient) QueryWorkflow(
	_ context.Context,
	workflowID, runID, queryType string,
	_ ...any,
) (converter.EncodedValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, queryCall{workflowID: workflowID, queryType: queryType, runID: runID})
	if c.err != nil {
		return nil, c.err
	}
	return c.value, nil
}

func (c *fakeQueryClient) recorded() []queryCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]queryCall(nil), c.calls...)
}

// encodedValue is a query answer holding one already-encoded payload, the way the client hands
// a decoded query result back. Get always decodes into a fresh pointer, as the contract requires.
type encodedValue struct {
	payloads *commonpb.Payloads
}

// HasValue reports that the answer carries a value.
func (v encodedValue) HasValue() bool { return v.payloads != nil }

// Get decodes the answer into valuePtr.
func (v encodedValue) Get(valuePtr any) error {
	return converter.GetDefaultDataConverter().FromPayloads(v.payloads, valuePtr)
}

// encoded is a query answer carrying value, as the client would decode it.
func encoded(t *testing.T, value any) converter.EncodedValue {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(value)
	if err != nil {
		t.Fatalf("encode query answer: %v", err)
	}
	return encodedValue{payloads: payloads}
}

func TestDeviceStatesState(t *testing.T) {
	t.Parallel()

	t.Run("reads the device's own state query", func(t *testing.T) {
		t.Parallel()

		want := State{
			DeviceID:  "dev-1",
			Region:    "eu-west",
			CurrentFw: "fw-2",
			LastCommand: &ConcludedCommand{
				Command: CommandIssuedSignal{
					CommandID: "w1-dev-1", DeviceID: "dev-1", Kind: CommandKindUpdate,
					FirmwareID: "fw-2", Version: "fw-2",
				},
				Outcome: OutcomeFailed,
				Detail:  "flash error",
			},
		}
		fc := &fakeQueryClient{value: encoded(t, want)}
		got, err := NewDeviceStates(fc).State(context.Background(), "dev-1")
		if err != nil {
			t.Fatalf("State() error = %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("state mismatch (-want +got):\n%s", diff)
		}

		calls := fc.recorded()
		if len(calls) != 1 {
			t.Fatalf("query calls = %d, want 1", len(calls))
		}
		if calls[0].workflowID != "device-dev-1" {
			t.Errorf("queried workflow = %q, want %q", calls[0].workflowID, "device-dev-1")
		}
		if calls[0].queryType != GetStateQueryType {
			t.Errorf("queried type = %q, want %q", calls[0].queryType, GetStateQueryType)
		}
	})

	t.Run("a device with no workflow is not found", func(t *testing.T) {
		t.Parallel()

		fc := &fakeQueryClient{err: serviceerror.NewNotFound("workflow not found")}
		_, err := NewDeviceStates(fc).State(context.Background(), "dev-1")
		if !errors.Is(err, ErrDeviceNotFound) {
			t.Errorf("State() error = %v, want ErrDeviceNotFound", err)
		}
		if !strings.Contains(err.Error(), "dev-1") {
			t.Errorf("State() error = %v, want it to name the device", err)
		}
	})

	t.Run("a query failure is wrapped with the device and workflow", func(t *testing.T) {
		t.Parallel()

		fc := &fakeQueryClient{err: errors.New("frontend is unavailable")}
		_, err := NewDeviceStates(fc).State(context.Background(), "dev-1")
		if err == nil {
			t.Fatal("State() error = nil, want a failure")
		}
		if errors.Is(err, ErrDeviceNotFound) {
			t.Errorf("State() error = %v, want a read failure rather than not-found", err)
		}
		for _, want := range []string{"dev-1", "device-dev-1", "frontend is unavailable"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("State() error = %v, want it to name %q", err, want)
			}
		}
	})

	t.Run("an undecodable answer is refused", func(t *testing.T) {
		t.Parallel()

		fc := &fakeQueryClient{value: encoded(t, "not a state")}
		_, err := NewDeviceStates(fc).State(context.Background(), "dev-1")
		if err == nil {
			t.Fatal("State() error = nil, want a decode failure")
		}
		if !strings.Contains(err.Error(), "decode") {
			t.Errorf("State() error = %v, want it to name the decode", err)
		}
	})

	t.Run("an empty device id is refused without a query", func(t *testing.T) {
		t.Parallel()

		fc := &fakeQueryClient{}
		_, err := NewDeviceStates(fc).State(context.Background(), "")
		if err == nil {
			t.Fatal("State() error = nil, want a refusal")
		}
		if calls := fc.recorded(); len(calls) != 0 {
			t.Errorf("query calls = %d, want none for an empty device id", len(calls))
		}
	})
}
