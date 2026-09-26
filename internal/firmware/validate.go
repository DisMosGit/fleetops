package firmware

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors of the firmware registry: the expected outcomes an upload can end in, which
// the HTTP endpoint maps onto its status codes.
var (
	// ErrInvalidMetadata reports upload metadata that is malformed: no binary part, or a
	// missing version or models field.
	ErrInvalidMetadata = errors.New("invalid firmware metadata")
	// ErrIncompatible reports an upload whose target device models fail compatibility
	// validation: an empty list, a duplicate, or a model the device registry does not know.
	ErrIncompatible = errors.New("firmware incompatible with target device models")
	// ErrVersionConflict reports an upload whose version already identifies a firmware
	// record.
	ErrVersionConflict = errors.New("firmware version already registered")
	// ErrNotFound reports a firmware id with no firmware record.
	ErrNotFound = errors.New("firmware not found")
)

// ModelCatalog reports the device models the device registry knows — the models at least one
// registered device runs. *devices.Store satisfies it.
type ModelCatalog interface {
	// DeviceModels lists the distinct device models of the registered devices.
	DeviceModels(ctx context.Context) ([]string, error)
}

// ParseModels splits the upload's target-model metadata — one or more comma-separated form
// values — into its model list, dropping empty entries.
func ParseModels(values []string) []string {
	var models []string
	for _, value := range values {
		for _, model := range strings.Split(value, ",") {
			if model = strings.TrimSpace(model); model != "" {
				models = append(models, model)
			}
		}
	}
	return models
}

// ValidateUpload checks one upload's metadata before anything is stored: the version must be
// present, and the target models must be a non-empty, duplicate-free list of models the
// device registry knows — the check that the firmware is compatible with the device models it
// targets. Failures wrap ErrInvalidMetadata or ErrIncompatible and name the offending model.
func ValidateUpload(ctx context.Context, catalog ModelCatalog, version string, models []string) error {
	if strings.TrimSpace(version) == "" {
		return fmt.Errorf("%w: version required", ErrInvalidMetadata)
	}
	if len(models) == 0 {
		return fmt.Errorf("%w: at least one target model required", ErrIncompatible)
	}
	seen := make(map[string]bool, len(models))
	for _, model := range models {
		if seen[model] {
			return fmt.Errorf("%w: duplicate model %q", ErrIncompatible, model)
		}
		seen[model] = true
	}
	known, err := catalog.DeviceModels(ctx)
	if err != nil {
		return fmt.Errorf("known device models: %w", err)
	}
	knownSet := make(map[string]bool, len(known))
	for _, model := range known {
		knownSet[model] = true
	}
	for _, model := range models {
		if !knownSet[model] {
			return fmt.Errorf("%w: unknown model %q", ErrIncompatible, model)
		}
	}
	return nil
}
