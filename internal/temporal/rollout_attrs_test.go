package temporal

import (
	"slices"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/DisMosGit/fleetops/internal/rollout"
)

// wantRolloutAttrs builds the exact search-attribute collection a rollout run must upsert for the
// given mirrored values.
func wantRolloutAttrs(firmware, region, status string) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(
		temporal.NewSearchAttributeKeyKeyword(SearchAttrRolloutFirmware).ValueSet(firmware),
		temporal.NewSearchAttributeKeyKeyword(SearchAttrRolloutRegion).ValueSet(region),
		temporal.NewSearchAttributeKeyKeyword(SearchAttrRolloutStatus).ValueSet(status),
	)
}

// expectAttrs installs one exact upsert expectation per entry, in order. The test environment
// matches them positionally, so the sequence is the contract: an extra, missing, or reordered
// upsert fails the test, and AssertExpectations proves every expected one happened.
func expectAttrs(env *testsuite.TestWorkflowEnvironment, attrs ...temporal.SearchAttributes) {
	for _, want := range attrs {
		env.OnUpsertTypedSearchAttributes(want).Once()
	}
}

func TestRolloutWorkflowSearchAttributes(t *testing.T) {
	t.Parallel()

	const (
		deployed = "2.0.0"
		region   = "eu-west"
	)

	t.Run("an un-gated rollout mirrors start, firmware, and its terminal status", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(healthyAt(0.99))

		env := newRolloutEnv(fakes)
		expectAttrs(env,
			// Run start: the firmware's version is not knowable from the start input yet.
			wantRolloutAttrs("", region, string(rollout.RolloutRunning)),
			// The metadata loaded: the version is known from here on.
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			// The sequence completed.
			wantRolloutAttrs(deployed, region, string(rollout.RolloutCompleted)),
		)
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.FirmwareVersion != deployed {
			t.Errorf("view firmware version = %q, want %q", view.FirmwareVersion, deployed)
		}
		env.AssertExpectations(t)
	})

	t.Run("waiting for approval and the terminal outcome are mirrored", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100, RequireApproval: true})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(healthyAt(0.99))

		env := newRolloutEnv(fakes)
		queueSignalsAt(env, signalApproval(0))
		expectAttrs(env,
			wantRolloutAttrs("", region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutAwaitingApproval)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutCompleted)),
		)
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.Status != rollout.RolloutCompleted {
			t.Fatalf("rollout concluded as %s, want completed", view.Status)
		}
		env.AssertExpectations(t)
	})

	t.Run("a pause and a resume are mirrored", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(healthyAt(0.99))

		env := newRolloutEnv(fakes)
		queueSignalsAt(env, signalPause(0), signalResume(1*time.Second))
		expectAttrs(env,
			wantRolloutAttrs("", region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutPaused)),
			// A repeated pause is a no-op for the attributes too: the map did not change.
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutCompleted)),
		)
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.Status != rollout.RolloutCompleted {
			t.Fatalf("rollout concluded as %s, want completed", view.Status)
		}
		env.AssertExpectations(t)
	})

	t.Run("a rollout that cannot start mirrors its refusal with no firmware version", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		// The firmware does not target the selector's model: the rollout fails before any
		// command, and its metadata never loads.
		in := rolloutInputFor(settings)
		in.Model = "oak-s3-mini"

		env := newRolloutEnv(fakes)
		expectAttrs(env,
			wantRolloutAttrs("", region, string(rollout.RolloutRunning)),
			wantRolloutAttrs("", region, string(rollout.RolloutFailed)),
		)
		view := runRollout(t, env, in)

		if view.Status != rollout.RolloutFailed {
			t.Fatalf("rollout concluded as %s, want failed", view.Status)
		}
		// The version is not knowable from the start input, so the attribute stays empty: such
		// a rollout is findable by region and status, not by a version it never had.
		if view.FirmwareVersion != "" {
			t.Errorf("view firmware version = %q, want it empty", view.FirmwareVersion)
		}
		env.AssertExpectations(t)
	})

	t.Run("an unhealthy wave mirrors the rolled-back status", func(t *testing.T) {
		t.Parallel()

		settings := pauseResumeSettings(RolloutWave{Percent: 100})
		fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
		fakes.scriptHealth(unhealthyAt(0.4))

		env := newRolloutEnv(fakes)
		expectAttrs(env,
			wantRolloutAttrs("", region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRunning)),
			wantRolloutAttrs(deployed, region, string(rollout.RolloutRolledBack)),
		)
		view := runRollout(t, env, rolloutInputFor(settings))

		if view.Status != rollout.RolloutRolledBack {
			t.Fatalf("rollout concluded as %s, want rolled_back", view.Status)
		}
		env.AssertExpectations(t)
	})
}

// TestRolloutSearchAttributesDerivation pins the pure function the attributes come from: one map
// derived from state, holding exactly the three rollout attributes at the values state carries.
func TestRolloutSearchAttributesDerivation(t *testing.T) {
	t.Parallel()

	state := newRolloutState(rolloutTestInput())
	state.FirmwareVersion = "2.0.0"

	got := rolloutSearchAttributes(state)
	want := map[string]any{
		SearchAttrRolloutFirmware: "2.0.0",
		SearchAttrRolloutRegion:   "eu-west",
		SearchAttrRolloutStatus:   string(rollout.RolloutRunning),
	}
	if len(got) != len(want) {
		t.Fatalf("derived %d attributes, want %d: %v", len(got), len(want), got)
	}
	for name, wantValue := range want {
		if got[name] != wantValue {
			t.Errorf("attribute %s = %v, want %v", name, got[name], wantValue)
		}
	}

	// Every derived attribute has a registered type, so attrUpdates can encode it: a name the
	// registry does not know would be dropped silently.
	types := searchAttributeTypes()
	for name := range got {
		typ, ok := types[name]
		if !ok {
			t.Errorf("attribute %s has no registered type", name)
			continue
		}
		if typ != enumspb.INDEXED_VALUE_TYPE_KEYWORD {
			t.Errorf("attribute %s is registered as %s, want Keyword", name, typ)
		}
	}
	updates := attrUpdates(got)
	if len(updates) != len(want) {
		t.Errorf("encoded %d updates, want %d", len(updates), len(want))
	}
}

// TestUpsertRolloutAttrsSkipsUnchangedState pins the change detection: a state change that
// mirrors nothing does not upsert, and one that moves a mirrored value does.
func TestUpsertRolloutAttrsSkipsUnchangedState(t *testing.T) {
	t.Parallel()

	settings := pauseResumeSettings(RolloutWave{Percent: 100})
	fakes := newRolloutFakes(settings, rolloutTestFirmware(), deviceIDs(2)...)
	fakes.scriptHealth(healthyAt(0.99))

	env := newRolloutEnv(fakes)
	// Three maps for a run that recorded its status twice and wrote four wave documents: the
	// wave's own progress — membership, dispatch, a decided wave, device outcomes — mirrors
	// nothing, so only the initial status, the loaded version, and the terminal status upsert.
	expectAttrs(env,
		wantRolloutAttrs("", "eu-west", string(rollout.RolloutRunning)),
		wantRolloutAttrs("2.0.0", "eu-west", string(rollout.RolloutRunning)),
		wantRolloutAttrs("2.0.0", "eu-west", string(rollout.RolloutCompleted)),
	)
	view := runRollout(t, env, rolloutInputFor(settings))

	if view.Status != rollout.RolloutCompleted {
		t.Fatalf("rollout concluded as %s, want completed", view.Status)
	}
	env.AssertExpectations(t)

	// The status the rollout recorded was written twice — once at the start, once at the end —
	// and the second of those is the one the firmware-load upsert interleaved with, not a
	// duplicate of it.
	statuses := fakes.rolloutStatuses()
	want := []rollout.RolloutStatus{rollout.RolloutRunning, rollout.RolloutCompleted}
	if !slices.Equal(statuses, want) {
		t.Errorf("recorded statuses = %v, want %v", statuses, want)
	}
	if len(fakes.recordedWaveEvents()) != 2 {
		t.Errorf("wave writes = %d, want the wave measured and then decided",
			len(fakes.recordedWaveEvents()))
	}
}
