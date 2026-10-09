# Change notes

## Telemetry storage: time-series collection fell back to a plain collection (task 4.3)

Design decision 5 anticipated a possible incompatibility between MongoDB time-series collections
and the `_id = event_id` idempotency contract and pre-authorized a fallback. Probed empirically
with `mongosh` on 2026-09-25 against the version candidates for the local stack:

| Probe | MongoDB 7 (mongo:7) | MongoDB 8.3.11 (mongo:8) |
|---|---|---|
| custom `_id` accepted on a `timeseries` collection | yes | yes |
| second `insertOne` with the same `_id` | **accepted → duplicate row** | **accepted → duplicate row** |
| `createIndex({ts: 1}, {expireAfterSeconds})` on the timeField | rejected: TTL on time-series requires a `partialFilterExpression` on the metaField | not re-probed |

Time-series collections carry no unique `_id` index (`getIndexes()` shows only the meta/time
bucket index), so "exactly one document per event id" cannot be enforced at the storage layer,
and TTL on the time field only works behind a partial filter. That contradicts the
redelivery-must-be-a-no-op requirement, which takes precedence per design decision 5: "the
spec's observable contract — one row per event id, automatic expiry — holds either way."

**Deviation taken:** `telemetry` is a plain collection with a TTL index on `ts` (the fallback
pre-authorized by task 4.3 and design decision 5). Documents keep the designed time-series
shape (`ts`, `meta: {device_id, region, model}`, top-level `cpu`/`mem`/`health`), the index set
from design decision 7 is unchanged, and `_id = event_id` idempotency is enforced by the standard
unique `_id` index — a duplicate insert raises `E11000`, which the ingest contract maps to a
no-op. If bucketed time-series storage is preferred later, the ingest write path would have to
own dedup (upsert semantics), which weakens the storage-level guarantee; revisit deliberately.

The delta spec's phrase "MongoDB time-series collection" names the intended storage; every spec
scenario (one row per event id, redelivery no-op, automatic expiry, device/time retrieval) holds
with this fallback.
