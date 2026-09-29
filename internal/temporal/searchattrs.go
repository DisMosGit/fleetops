package temporal

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
)

// The custom Temporal search attributes every device workflow run carries. Their values mirror
// device state (see searchAttributes), which is what makes the fleet filterable in the Temporal
// UI by region, model, firmware version, and online status. The names are the operators'
// contract: they show up verbatim in UI queries.
const (
	// SearchAttrDeviceRegion is the Keyword attribute holding the device's region.
	SearchAttrDeviceRegion = "DeviceRegion"
	// SearchAttrDeviceModel is the Keyword attribute holding the device's model.
	SearchAttrDeviceModel = "DeviceModel"
	// SearchAttrDeviceFirmware is the Keyword attribute holding the firmware version.
	SearchAttrDeviceFirmware = "DeviceFirmware"
	// SearchAttrDeviceOnline is the Bool attribute holding the liveness status.
	SearchAttrDeviceOnline = "DeviceOnline"
)

// The custom Temporal search attributes every rollout workflow run carries. Their values mirror
// rollout state (see rolloutSearchAttributes), which is what makes releases filterable in the
// Temporal UI by the firmware version being deployed, the target region, and the rollout's current
// status — alone or combined.
const (
	// SearchAttrRolloutFirmware is the Keyword attribute holding the deployed firmware's
	// version. It is empty until the rollout's firmware metadata has loaded: the version is not
	// knowable from the start input.
	SearchAttrRolloutFirmware = "RolloutFirmware"
	// SearchAttrRolloutRegion is the Keyword attribute holding the target selector's region.
	SearchAttrRolloutRegion = "RolloutRegion"
	// SearchAttrRolloutStatus is the Keyword attribute holding the rollout's lifecycle status.
	SearchAttrRolloutStatus = "RolloutStatus"
)

// searchAttributes derives the complete search-attribute map of one device state — one pure
// function of state, so the attributes cannot drift from what they mirror. The workflow
// upserts the whole map: four values, all of them derived here.
func searchAttributes(s deviceState) map[string]any {
	return map[string]any{
		SearchAttrDeviceRegion:   s.Region,
		SearchAttrDeviceModel:    s.Model,
		SearchAttrDeviceFirmware: s.CurrentFw,
		SearchAttrDeviceOnline:   s.Online,
	}
}

// rolloutSearchAttributes derives the complete search-attribute map of one rollout state — one
// pure function of state, so the attributes cannot drift from the rollout they mirror, and the
// firmware version and the status cannot disagree with the rollout document or the state query.
// The status is the projected one (see rolloutState.reported), so a paused rollout is filterable
// as paused while a wave is still in flight.
func rolloutSearchAttributes(s rolloutState) map[string]any {
	return map[string]any{
		SearchAttrRolloutFirmware: s.FirmwareVersion,
		SearchAttrRolloutRegion:   s.Region,
		SearchAttrRolloutStatus:   string(s.reported()),
	}
}

// attrsEqual reports whether two derived attribute maps hold the same values under the same
// names — the change detection that keeps a state change which mirrors nothing from upserting.
func attrsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for name, av := range a {
		bv, ok := b[name]
		if !ok || av != bv {
			return false
		}
	}
	return true
}

// attrUpdates encodes a derived attribute map as the typed updates the workflow upserts. The map
// stays the one derivation of attribute values; this is only its transport form, and the type of
// each attribute comes from the same registry the namespace bootstrap uses, so a value can never
// be upserted at a type the namespace does not have.
func attrUpdates(attrs map[string]any) []temporal.SearchAttributeUpdate {
	types := searchAttributeTypes()
	updates := make([]temporal.SearchAttributeUpdate, 0, len(attrs))
	for name, value := range attrs {
		switch types[name] {
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			text, _ := value.(string)
			updates = append(updates, temporal.NewSearchAttributeKeyKeyword(name).ValueSet(text))
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			flag, _ := value.(bool)
			updates = append(updates, temporal.NewSearchAttributeKeyBool(name).ValueSet(flag))
		}
	}
	return updates
}

// searchAttributeRegistry is the slice of the Temporal operator service the attribute
// bootstrap needs — see what is registered, register what is missing.
// operatorservice.OperatorServiceClient satisfies it, and tests hand-write a fake.
type searchAttributeRegistry interface {
	// ListSearchAttributes reports the namespace's registered search attributes.
	ListSearchAttributes(
		ctx context.Context, in *operatorservice.ListSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.ListSearchAttributesResponse, error)
	// AddSearchAttributes registers custom search attributes on a namespace.
	AddSearchAttributes(
		ctx context.Context, in *operatorservice.AddSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.AddSearchAttributesResponse, error)
}

// searchAttributeTypes returns the registered type of every custom search attribute — the
// typing the UI's filters depend on, and the typing attrUpdates encodes values at. A fresh map
// every call keeps this free of shared mutable state.
func searchAttributeTypes() map[string]enumspb.IndexedValueType {
	return map[string]enumspb.IndexedValueType{
		SearchAttrDeviceRegion:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrDeviceModel:     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrDeviceFirmware:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrDeviceOnline:    enumspb.INDEXED_VALUE_TYPE_BOOL,
		SearchAttrRolloutFirmware: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrRolloutRegion:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrRolloutStatus:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
}

// EnsureSearchAttributes registers the custom search attributes on the namespace when they
// are missing, so workflows can upsert them and the Temporal UI can filter on them. It is
// idempotent and safe to run concurrently from several workers: an attribute that already
// exists is left unchanged, and a registration that lost a race is confirmed against the
// registry instead of failed. An attribute registered at the wrong type is a misconfiguration
// and fails loudly.
func EnsureSearchAttributes(
	ctx context.Context,
	reg searchAttributeRegistry,
	namespace string,
) error {
	want := searchAttributeTypes()
	have, err := registeredAttributes(ctx, reg, namespace)
	if err != nil {
		return err
	}
	missing := make(map[string]enumspb.IndexedValueType)
	for name, typ := range want {
		switch got, ok := have[name]; {
		case !ok:
			missing[name] = typ
		case got != typ:
			return fmt.Errorf("search attribute %s registered as %s (want %s)", name, got, typ)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	req := &operatorservice.AddSearchAttributesRequest{
		Namespace:        namespace,
		SearchAttributes: missing,
	}
	if _, err := reg.AddSearchAttributes(ctx, req); err != nil {
		// A replica registering the same attributes first looks exactly like a failure
		// here; only the registry's actual content settles it.
		have, listErr := registeredAttributes(ctx, reg, namespace)
		if listErr == nil && covers(have, want) {
			return nil
		}
		return fmt.Errorf("register search attributes on %s: %w", namespace, err)
	}
	return nil
}

// registeredAttributes returns the namespace's custom search attributes.
func registeredAttributes(
	ctx context.Context,
	reg searchAttributeRegistry,
	namespace string,
) (map[string]enumspb.IndexedValueType, error) {
	resp, err := reg.ListSearchAttributes(ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("list search attributes on %s: %w", namespace, err)
	}
	return resp.GetCustomAttributes(), nil
}

// covers reports whether have carries every wanted attribute at its wanted type.
func covers(have, want map[string]enumspb.IndexedValueType) bool {
	for name, typ := range want {
		if got, ok := have[name]; !ok || got != typ {
			return false
		}
	}
	return true
}
