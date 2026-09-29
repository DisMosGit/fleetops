// Package rollout owns the records a canary rollout is driven by: the rollout and wave
// documents the rollout workflow projects its state through, and the resolution that turns a
// wave's configured share into the devices it targets.
//
// A wave's share is cumulative — 1%, then 5%, then 25%, then 100% of the eligible pool — so the
// devices a wave targets are the ones its share adds beyond the shares the rollout's earlier
// waves already targeted. Resolving that difference is the only place membership is decided, and
// it is decided against a deterministically ordered pool so the same pool and shares always
// produce the same target sets. The resolution is recorded before anything is dispatched:
// downstream, a wave's health is measured over exactly the devices its document names.
//
// Membership resolution and the record writes are the workflow's side effects, so they live
// behind the interfaces internal/temporal consumes; this package holds the Mongo-backed
// implementations and the pure arithmetic both the workflow's decisions and its tests rest on.
package rollout
