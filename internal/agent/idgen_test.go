package agent

import (
	"strings"
	"testing"
)

func TestIDGen(t *testing.T) {
	t.Parallel()

	gen, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}

	first := gen.Next("device-1")
	second := gen.Next("device-1")
	if first == second {
		t.Errorf("ids in one run must differ, both %q", first)
	}
	if !strings.HasPrefix(first, "device-1-") {
		t.Errorf("id %q must carry its scope as prefix", first)
	}
	if other := gen.Next("device-2"); other == first || other == second {
		t.Errorf("ids of different scopes must differ, got %q", other)
	}
}

func TestIDGenRunNonceIsUnique(t *testing.T) {
	t.Parallel()

	a, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	b, err := NewIDGen()
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	if a.Next("device-1") == b.Next("device-1") {
		t.Error("ids minted by different runs must differ")
	}
}
