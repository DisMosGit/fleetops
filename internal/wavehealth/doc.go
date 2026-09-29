// Package wavehealth evaluates how healthy a canary wave is over a sliding window of heartbeat
// telemetry, and serves that evaluation over gRPC.
//
// One evaluation answers a single question for one rollout and wave: of the heartbeat samples the
// wave's target devices emitted inside the effective window, what share reported a health score at
// or above the per-sample health threshold — and is that share, on that much evidence, enough to
// call the wave healthy. The effective window is the configured health window ending at the moment
// of evaluation, clipped so it never reaches back before the wave started; samples from before the
// wave started come from devices still running the previous firmware and would make a regressing
// wave look healthiest exactly when the gate must be strictest.
//
// The verdict is three-state rather than a boolean. Below the configured minimum sample count the
// wave is undecided — not healthy — so thin evidence can never promote a wave, and a gate that
// should wait is distinguishable from one that should roll back. At or above that count the
// boundary is inclusive: a success ratio equal to the minimum success ratio is healthy.
//
// Both storage dependencies are consumed interfaces, so the formula, the window, and the decision
// boundary are unit-testable without a database; the Mongo-backed implementations sit beside them,
// and the gRPC service is a thin adapter that maps this package's sentinel not-found error onto
// NOT_FOUND. Evaluation is read-only: it observes rollouts, waves, and telemetry and never writes
// the wave's success_rate — that stays the rollout workflow's write.
package wavehealth
