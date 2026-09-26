package config

import (
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a Go duration value that YAML loads from a duration string such as "30s". A
// value that is not a duration string is kept as an invalid marker instead of failing the
// decode, so Validate can name the offending configuration field — the decoder would only
// name a line of the file.
type Duration struct {
	// Duration is the loaded value; zero when the input was not a duration string.
	Duration time.Duration
	// invalid marks input that did not parse; only Validate consults it.
	invalid bool
}

// UnmarshalYAML decodes one YAML scalar into d. Anything that is not a duration string marks
// d invalid without failing the decode; Validate turns that into a field-naming error.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		d.invalid = true
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		d.invalid = true
		return nil
	}
	d.Duration = parsed
	return nil
}

// Equal reports whether d and other hold the same value and validity, so configuration
// comparisons see a malformed field as different from a zero one.
func (d Duration) Equal(other Duration) bool {
	return d.Duration == other.Duration && d.invalid == other.invalid
}
