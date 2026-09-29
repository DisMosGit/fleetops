package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	enumspb "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"google.golang.org/grpc"
)

// fakeSearchAttributeRegistry is a hand-written searchAttributeRegistry double: a namespace
// whose registered attributes a test controls, recording what gets added.
type fakeSearchAttributeRegistry struct {
	registered map[string]enumspb.IndexedValueType
	added      []map[string]enumspb.IndexedValueType
	listErr    error
	addErr     error
}

func (r *fakeSearchAttributeRegistry) ListSearchAttributes(
	_ context.Context, _ *operatorservice.ListSearchAttributesRequest, _ ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: r.registered}, nil
}

func (r *fakeSearchAttributeRegistry) AddSearchAttributes(
	_ context.Context, in *operatorservice.AddSearchAttributesRequest, _ ...grpc.CallOption,
) (*operatorservice.AddSearchAttributesResponse, error) {
	if r.addErr != nil {
		return nil, r.addErr
	}
	if r.registered == nil {
		r.registered = map[string]enumspb.IndexedValueType{}
	}
	for name, typ := range in.SearchAttributes {
		r.registered[name] = typ
	}
	r.added = append(r.added, in.SearchAttributes)
	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func TestEnsureSearchAttributes(t *testing.T) {
	t.Parallel()

	all := searchAttributeTypes()

	t.Run("fresh namespace gets the attributes", func(t *testing.T) {
		t.Parallel()
		reg := &fakeSearchAttributeRegistry{}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); err != nil {
			t.Fatalf("EnsureSearchAttributes: %v", err)
		}
		if diff := cmp.Diff(all, reg.registered); diff != "" {
			t.Errorf("registered attributes mismatch (-want +got):\n%s", diff)
		}
		if len(reg.added) != 1 {
			t.Errorf("registration calls = %d, want 1", len(reg.added))
		}
	})

	t.Run("re-registration is a no-op", func(t *testing.T) {
		t.Parallel()
		reg := &fakeSearchAttributeRegistry{registered: searchAttributeTypes()}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); err != nil {
			t.Fatalf("EnsureSearchAttributes: %v", err)
		}
		if len(reg.added) != 0 {
			t.Errorf("registration calls = %d, want none", len(reg.added))
		}
	})

	t.Run("only missing attributes are registered", func(t *testing.T) {
		t.Parallel()
		reg := &fakeSearchAttributeRegistry{registered: map[string]enumspb.IndexedValueType{
			SearchAttrDeviceRegion: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrDeviceModel:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); err != nil {
			t.Fatalf("EnsureSearchAttributes: %v", err)
		}
		want := map[string]enumspb.IndexedValueType{
			SearchAttrDeviceFirmware:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrDeviceOnline:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			SearchAttrRolloutFirmware: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrRolloutRegion:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrRolloutStatus:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}
		if diff := cmp.Diff(want, reg.added[0]); diff != "" {
			t.Errorf("registered attributes mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a namespace with the device attributes gets the rollout ones", func(t *testing.T) {
		t.Parallel()
		// The upgrade path this change introduces: a namespace already carrying the four device
		// attributes is extended with the three rollout attributes, and the existing ones are
		// left alone.
		deviceAttrs := map[string]enumspb.IndexedValueType{
			SearchAttrDeviceRegion:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrDeviceModel:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrDeviceFirmware: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrDeviceOnline:   enumspb.INDEXED_VALUE_TYPE_BOOL,
		}
		reg := &fakeSearchAttributeRegistry{registered: deviceAttrs}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); err != nil {
			t.Fatalf("EnsureSearchAttributes: %v", err)
		}
		want := map[string]enumspb.IndexedValueType{
			SearchAttrRolloutFirmware: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrRolloutRegion:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrRolloutStatus:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}
		if len(reg.added) != 1 {
			t.Fatalf("registration calls = %d, want 1", len(reg.added))
		}
		if diff := cmp.Diff(want, reg.added[0]); diff != "" {
			t.Errorf("registered attributes mismatch (-want +got):\n%s", diff)
		}
		for name, typ := range deviceAttrs {
			if reg.registered[name] != typ {
				t.Errorf("device attribute %s = %s, want it left at %s", name, reg.registered[name], typ)
			}
		}
	})

	t.Run("the registered set is the seven attributes both families upsert", func(t *testing.T) {
		t.Parallel()
		want := map[string]enumspb.IndexedValueType{
			"DeviceRegion":    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceModel":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceFirmware":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"DeviceOnline":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"RolloutFirmware": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutRegion":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"RolloutStatus":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}
		if diff := cmp.Diff(want, searchAttributeTypes()); diff != "" {
			t.Errorf("registered attribute set mismatch (-want +got):\n%s", diff)
		}
		// Every attribute a workflow can derive has a registered type: attrUpdates silently
		// drops a name the registry does not know, so this is what keeps a derived value from
		// being dropped on the floor.
		for name := range searchAttributes(deviceState{}) {
			if _, ok := want[name]; !ok {
				t.Errorf("device attribute %s has no registered type", name)
			}
		}
		for name := range rolloutSearchAttributes(rolloutState{}) {
			if _, ok := want[name]; !ok {
				t.Errorf("rollout attribute %s has no registered type", name)
			}
		}
	})

	t.Run("a registration race is confirmed, not failed", func(t *testing.T) {
		t.Parallel()
		// Another replica registered the attributes first: the add call reports it as a
		// failure, and the registry's content proves the work is done.
		reg := &fakeSearchAttributeRegistry{
			registered: searchAttributeTypes(),
			addErr:     errors.New("already exists"),
		}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); err != nil {
			t.Errorf("EnsureSearchAttributes = %v, want nil on a lost race", err)
		}
	})

	t.Run("a registered attribute at the wrong type fails loudly", func(t *testing.T) {
		t.Parallel()
		reg := &fakeSearchAttributeRegistry{registered: map[string]enumspb.IndexedValueType{
			SearchAttrDeviceOnline: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}}
		err := EnsureSearchAttributes(context.Background(), reg, "default")
		if err == nil {
			t.Fatal("EnsureSearchAttributes with a mistyped attribute returned nil")
		}
	})

	t.Run("list and add failures are wrapped", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("temporal is down")
		if err := EnsureSearchAttributes(context.Background(),
			&fakeSearchAttributeRegistry{listErr: boom}, "default"); !errors.Is(err, boom) {
			t.Errorf("EnsureSearchAttributes(list) error = %v, want it wrapped", err)
		}
		reg := &fakeSearchAttributeRegistry{addErr: boom}
		if err := EnsureSearchAttributes(context.Background(), reg, "default"); !errors.Is(err, boom) {
			t.Errorf("EnsureSearchAttributes(add) error = %v, want it wrapped", err)
		}
	})
}

func TestSearchAttributesDerivation(t *testing.T) {
	t.Parallel()

	s := newDeviceState("dev-1", testSettings())
	s.Region, s.Model, s.CurrentFw, s.Online = "eu-west", "oak-s3", "fw-2", true

	want := map[string]any{
		SearchAttrDeviceRegion:   "eu-west",
		SearchAttrDeviceModel:    "oak-s3",
		SearchAttrDeviceFirmware: "fw-2",
		SearchAttrDeviceOnline:   true,
	}
	if diff := cmp.Diff(want, searchAttributes(s)); diff != "" {
		t.Errorf("derived attributes mismatch (-want +got):\n%s", diff)
	}

	// A device nothing is known about still derives the complete map — empty values, not
	// missing ones — so the run is filterable by what is known and honest about what is not.
	empty := searchAttributes(newDeviceState("dev-2", testSettings()))
	for _, name := range []string{
		SearchAttrDeviceRegion, SearchAttrDeviceModel, SearchAttrDeviceFirmware, SearchAttrDeviceOnline,
	} {
		if _, ok := empty[name]; !ok {
			t.Errorf("derived attributes of an unknown device miss %q", name)
		}
	}
}

// TestSearchAttributesChangeDetection pins which state changes alter the derived map: the
// mirrored values are region, model, firmware, and liveness — everything else derives an
// unchanged map and must not cause an upsert.
func TestSearchAttributesChangeDetection(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	fresh := func() deviceState {
		s := newDeviceState("dev-1", testSettings())
		s.Region, s.Model, s.CurrentFw = "eu-west", "oak-s3", "fw-1"
		s.LastHeartbeatAt = base
		s.Online = true
		return s
	}

	cases := []struct {
		name      string
		change    func(*deviceState)
		wantEqual bool
	}{
		{
			name: "heartbeat-only timestamp refresh keeps the map",
			change: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", Timestamp: base.Add(time.Second),
				})
			},
			wantEqual: true,
		},
		{
			name: "identity adoption changes the map",
			change: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", Region: "us-east", Model: "oak-s9",
					Timestamp: base.Add(time.Second),
				})
			},
			wantEqual: false,
		},
		{
			name: "firmware adoption changes the map",
			change: func(s *deviceState) {
				s.applyHeartbeat(HeartbeatSignal{
					EventID: "evt-1", DeviceID: "dev-1", CurrentFw: "fw-2",
					Timestamp: base.Add(time.Second),
				})
			},
			wantEqual: false,
		},
		{
			name: "liveness flip changes the map",
			change: func(s *deviceState) {
				s.refreshLiveness(base.Add(2 * testSettings().OfflineThreshold))
			},
			wantEqual: false,
		},
		{
			name: "pending-command lifecycle keeps the map",
			change: func(s *deviceState) {
				s.applyCommandIssued(CommandIssuedSignal{
					CommandID: "cmd-1", DeviceID: "dev-1", Kind: CommandKindAbort, Reason: "stop",
				})
				s.applyCommandResult(CommandResultSignal{
					DeviceID: "dev-1", CommandID: "cmd-1", Outcome: OutcomeSucceeded,
				})
			},
			wantEqual: true,
		},
		{
			name: "configuration apply keeps the map",
			change: func(s *deviceState) {
				s.applyConfigChanged(ConfigChangedSignal{DeviceID: "dev-1", Version: 1})
			},
			wantEqual: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := fresh()
			before := searchAttributes(s)
			tc.change(&s)
			after := searchAttributes(s)
			if got := attrsEqual(before, after); got != tc.wantEqual {
				t.Errorf("attrsEqual(before, after) = %v, want %v (before %v, after %v)",
					got, tc.wantEqual, before, after)
			}
		})
	}
}

func TestAttrsEqual(t *testing.T) {
	t.Parallel()

	same := map[string]any{"a": "x", "b": true}
	cases := []struct {
		name string
		a, b map[string]any
		want bool
	}{
		{name: "identical maps", a: same, b: map[string]any{"a": "x", "b": true}, want: true},
		{name: "both empty", a: map[string]any{}, b: map[string]any{}, want: true},
		{name: "differing value", a: same, b: map[string]any{"a": "x", "b": false}, want: false},
		{name: "missing name", a: same, b: map[string]any{"a": "x"}, want: false},
		{name: "extra name", a: map[string]any{"a": "x"}, b: same, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := attrsEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("attrsEqual() = %v, want %v", got, tc.want)
			}
		})
	}
}
