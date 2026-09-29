#!/usr/bin/env bash
# Verifies the FleetOps MongoDB data model (deploy/mongo/init.js) against a running MongoDB:
# collections, required-field validators, the index set, telemetry and processed-events TTL
# retention, and the redelivery no-op contract. Exits non-zero on the first violation.
#
# Usage:
#   deploy/mongo/verify.sh
#
# Environment:
#   MONGO_URI                        connection string (default mongodb://localhost:27017)
#   FLEETOPS_TELEMETRY_RETENTION_DAYS expected retention in days (default 7; must match init.js)
#   FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS
#                                    expected dedup-ledger retention in days (default 7)
#   MONGO_CONTAINER                  container providing mongosh when none is on PATH
#                                    (default fleetops-mongo)
set -euo pipefail

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
RETENTION_DAYS="${FLEETOPS_TELEMETRY_RETENTION_DAYS:-7}"
LEDGER_RETENTION_DAYS="${FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS:-7}"
MONGO_CONTAINER="${MONGO_CONTAINER:-fleetops-mongo}"

if command -v mongosh >/dev/null 2>&1; then
  MONGOSH=(mongosh --quiet "$MONGO_URI" --eval)
elif command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -qx "$MONGO_CONTAINER"; then
  MONGOSH=(docker exec -i "$MONGO_CONTAINER" mongosh --quiet "$MONGO_URI" --eval)
else
  echo "verify.sh: no mongosh on PATH and no running container named '$MONGO_CONTAINER'" >&2
  exit 1
fi

SCRIPT="$(mktemp)"
trap 'rm -f "$SCRIPT"' EXIT
sed "s/__RETENTION_DAYS__/${RETENTION_DAYS}/g; s/__LEDGER_RETENTION_DAYS__/${LEDGER_RETENTION_DAYS}/g" >"$SCRIPT" <<'EOF'
// Assertions for the FleetOps data model; the retention placeholders are substituted by
// verify.sh.
const EXPECTED_RETENTION_SECONDS = __RETENTION_DAYS__ * 24 * 60 * 60;
const EXPECTED_LEDGER_RETENTION_SECONDS = __LEDGER_RETENTION_DAYS__ * 24 * 60 * 60;
const fleet = db.getSiblingDB("fleetops");
let failures = 0;

function check(ok, label) {
  print((ok ? "ok   " : "FAIL ") + label);
  if (!ok) {
    failures++;
  }
}

// --- collections ---
const present = fleet.getCollectionNames();
for (const name of ["devices", "device_state_snapshots", "firmware", "rollouts", "waves", "telemetry", "processed_events", "device_alerts"]) {
  check(present.includes(name), "collection " + name + " exists");
}

// --- required-field validators ---
function requiredOf(name) {
  const info = fleet.getCollectionInfos({ name: name })[0];
  const schema = info && info.options && info.options.validator && info.options.validator.$jsonSchema;
  return (schema && schema.required) || [];
}
const expectedRequired = {
  devices: ["model", "region", "current_fw", "status", "last_heartbeat"],
  device_state_snapshots: ["region", "model", "current_fw", "online", "last_heartbeat", "config", "snapshot_at"],
  firmware: ["version", "models", "checksum", "size", "created_at", "gridfs_id"],
  rollouts: ["firmware_id", "status", "temporal_wf_id", "region", "model"],
  waves: ["rollout_id", "percent", "status", "success_rate", "device_ids", "started_at"],
  processed_events: ["consumer", "event_id", "device_id", "claimed_at"],
  device_alerts: ["device_id", "region", "model", "threshold", "min_health", "first_seen_at", "last_seen_at"],
};
for (const [coll, fields] of Object.entries(expectedRequired)) {
  const req = requiredOf(coll);
  check(fields.every((f) => req.includes(f)), coll + " validator requires " + fields.join(", "));
}

// --- the wave outcome sets: optional string arrays, written by every wave write ---
function propertiesOf(name) {
  const info = fleet.getCollectionInfos({ name: name })[0];
  const schema = info && info.options && info.options.validator && info.options.validator.$jsonSchema;
  return (schema && schema.properties) || {};
}
const waveProps = propertiesOf("waves");
for (const field of ["failed_device_ids", "unreported_device_ids"]) {
  const prop = waveProps[field];
  check(prop !== undefined, "waves validator describes " + field);
  check(
    prop && prop.bsonType === "array" && prop.items && prop.items.bsonType === "string",
    "waves." + field + " is an array of device ids",
  );
  // Optional on purpose: a document written before these fields existed must stay updatable,
  // and a document written with them stays valid for the previous validator.
  check(!expectedRequired.waves.includes(field), "waves." + field + " is not a required field");
}

// --- the wave outcome sets round-trip through the validator ---
const waveID = "verify-wave-" + Date.now();
fleet.waves.insertOne({
  _id: waveID,
  rollout_id: "verify-rollout",
  percent: 25,
  status: "evaluating",
  success_rate: 0,
  device_ids: ["verify-a", "verify-b", "verify-c"],
  started_at: new Date(),
  failed_device_ids: ["verify-b"],
  unreported_device_ids: ["verify-c"],
});
const storedWave = fleet.waves.findOne({ _id: waveID });
check(storedWave.failed_device_ids.length === 1 && storedWave.failed_device_ids[0] === "verify-b",
  "waves stores the failed device set");
check(
  storedWave.unreported_device_ids.length === 1 && storedWave.unreported_device_ids[0] === "verify-c",
  "waves stores the unreported device set",
);
// An empty set is stored as an empty array rather than null, so a reader never has to tell
// "no failures" apart from "not collected yet".
fleet.waves.updateOne({ _id: waveID },
  { $set: { failed_device_ids: [], unreported_device_ids: [] } });
const clearedWave = fleet.waves.findOne({ _id: waveID });
check(
  Array.isArray(clearedWave.failed_device_ids) && clearedWave.failed_device_ids.length === 0 &&
    Array.isArray(clearedWave.unreported_device_ids) && clearedWave.unreported_device_ids.length === 0,
  "waves stores empty outcome sets as empty arrays",
);
// A document written before the fields existed stays updatable by the new build.
fleet.waves.updateOne({ _id: waveID },
  { $unset: { failed_device_ids: "", unreported_device_ids: "" } });
let legacyWaveUpdated = true;
try {
  fleet.waves.updateOne({ _id: waveID }, { $set: { status: "healthy", success_rate: 0.99 } });
} catch (e) {
  legacyWaveUpdated = false;
}
check(legacyWaveUpdated, "a wave document without the outcome sets stays updatable");
fleet.waves.deleteMany({ _id: waveID });

// --- the rollback record: optional, with the plan, the inventory, and the unrestored ids ---
const rolloutProps = propertiesOf("rollouts");
check(rolloutProps.rollback !== undefined, "rollouts validator describes rollback");
check(!expectedRequired.rollouts.includes("rollback"), "rollouts.rollback is not a required field");
const rollbackRequired = (rolloutProps.rollback && rolloutProps.rollback.required) || [];
check(
  ["outcome", "steps", "inventory", "unrestored_device_ids"].every((f) => rollbackRequired.includes(f)),
  "rollouts.rollback requires outcome, steps, inventory, unrestored_device_ids",
);
const stepProps = ((rolloutProps.rollback || {}).properties || {}).steps;
const stepFields = ((stepProps || {}).items || {}).properties || {};
for (const field of ["kind", "wave_id", "status", "devices", "restored", "failed", "unreported",
  "skipped", "unavailable", "agreed", "corrected", "unverified", "detail"]) {
  check(stepFields[field] !== undefined, "rollouts.rollback step describes " + field);
}
check(stepProps.items.required.includes("kind") && stepProps.items.required.includes("status"),
  "rollouts.rollback step requires its kind and status");
check(
  ((rolloutProps.rollback || {}).properties || {}).inventory !== undefined,
  "rollouts.rollback describes the reconciled inventory",
);

// --- the rollback record round-trips through the validator ---
const rolloutID = "verify-rollout-" + Date.now();
fleet.rollouts.insertOne({
  _id: rolloutID,
  firmware_id: "verify-fw",
  status: "rolling_back",
  temporal_wf_id: "rollout-" + rolloutID,
  region: "verify",
  model: "verify",
  rollback: {
    outcome: "unhealthy_wave",
    steps: [
      { kind: "notify_started", status: "completed", devices: 0 },
      { kind: "downgrade", wave_id: "verify-wave-" + Date.now(), status: "completed",
        devices: 2, restored: 1, unreported: 1 },
      { kind: "reconcile_inventory", status: "completed", devices: 2, agreed: 1, corrected: 1 },
      { kind: "notify_completed", status: "failed", devices: 0, detail: "broker is unreachable" },
    ],
    inventory: [{ version: "1.0.0", devices: 1 }, { version: "2.0.0", devices: 1 }],
    unrestored_device_ids: ["verify-b"],
  },
});
const storedRollout = fleet.rollouts.findOne({ _id: rolloutID });
check(storedRollout.status === "rolling_back", "rollouts stores the compensating status");
check(storedRollout.rollback.steps.length === 4, "rollouts stores a rollback record with its plan");
check(storedRollout.rollback.steps[3].detail === "broker is unreachable",
  "rollouts stores why a step failed");
check(storedRollout.rollback.inventory.length === 2,
  "rollouts stores the reconciled inventory");
check(storedRollout.rollback.unrestored_device_ids[0] === "verify-b",
  "rollouts stores the devices the rollback could not restore");

// A record missing a required piece is refused, the terminal status is storable with a record,
// and a document written without one stays updatable by the new build.
let incompleteRecordRejected = false;
try {
  fleet.rollouts.updateOne({ _id: rolloutID }, { $set: { rollback: { outcome: "unhealthy_wave" } } });
} catch (e) {
  incompleteRecordRejected = e.code === 121;
}
check(incompleteRecordRejected, "a rollback record without its plan is refused");
let concludedWithRecord = true;
try {
  fleet.rollouts.updateOne({ _id: rolloutID }, { $set: { status: "rolled_back" } });
} catch (e) {
  concludedWithRecord = false;
}
check(concludedWithRecord, "a rollback record is storable with the terminal status");
let legacyRolloutUpdated = true;
try {
  fleet.rollouts.updateOne({ _id: rolloutID }, { $unset: { rollback: "" } });
} catch (e) {
  legacyRolloutUpdated = false;
}
check(legacyRolloutUpdated, "a rollout document without a rollback record stays updatable");
let missingFieldRejected = false;
try {
  fleet.rollouts.insertOne({ _id: rolloutID + "-bad", firmware_id: "verify-fw", status: "running" });
} catch (e) {
  missingFieldRejected = e.code === 121;
}
check(missingFieldRejected, "a rollout document missing a required field is refused");
fleet.rollouts.deleteMany({ _id: { $in: [rolloutID, rolloutID + "-bad"] } });

// --- index set ---
function indexMap(name) {
  const map = {};
  for (const idx of fleet.getCollection(name).getIndexes()) {
    map[idx.name] = idx;
  }
  return map;
}
function sameKey(idx, expected) {
  if (!idx) {
    return false;
  }
  const entries = Object.entries(idx.key);
  return (
    entries.length === expected.length &&
    expected.every(([field, dir], i) => entries[i][0] === field && entries[i][1] === dir)
  );
}

const dev = indexMap("devices");
check(sameKey(dev["idx_region_model"], [["region", 1], ["model", 1]]), "devices.idx_region_model {region, model}");
check(sameKey(dev["idx_status"], [["status", 1]]), "devices.idx_status {status}");

const snaps = indexMap("device_state_snapshots");
check(sameKey(snaps["idx_region_model"], [["region", 1], ["model", 1]]),
  "device_state_snapshots.idx_region_model {region, model}");
check(sameKey(snaps["idx_online"], [["online", 1]]), "device_state_snapshots.idx_online {online}");

const fw = indexMap("firmware");
check(sameKey(fw["idx_version"], [["version", 1]]) && fw["idx_version"].unique === true,
  "firmware.idx_version {version} unique");

const roll = indexMap("rollouts");
check(sameKey(roll["idx_status"], [["status", 1]]), "rollouts.idx_status {status}");
check(sameKey(roll["idx_firmware_id"], [["firmware_id", 1]]), "rollouts.idx_firmware_id {firmware_id}");

const wav = indexMap("waves");
check(sameKey(wav["idx_rollout_percent"], [["rollout_id", 1], ["percent", 1]]),
  "waves.idx_rollout_percent {rollout_id, percent}");

const tel = indexMap("telemetry");
check(sameKey(tel["idx_device_ts"], [["meta.device_id", 1], ["ts", -1]]),
  "telemetry.idx_device_ts {meta.device_id, ts}");
check(sameKey(tel["idx_region_model_ts"], [["meta.region", 1], ["meta.model", 1], ["ts", -1]]),
  "telemetry.idx_region_model_ts {meta.region, meta.model, ts}");
check(
  sameKey(tel["idx_ts_ttl"], [["ts", 1]]) &&
    tel["idx_ts_ttl"].expireAfterSeconds === EXPECTED_RETENTION_SECONDS,
  "telemetry.idx_ts_ttl {ts} expireAfterSeconds=" + EXPECTED_RETENTION_SECONDS,
);

const ledger = indexMap("processed_events");
check(
  sameKey(ledger["idx_consumer_event"], [["consumer", 1], ["event_id", 1]]) &&
    ledger["idx_consumer_event"].unique === true,
  "processed_events.idx_consumer_event {consumer, event_id} unique",
);
check(sameKey(ledger["idx_device_id"], [["device_id", 1]]), "processed_events.idx_device_id {device_id}");
check(
  sameKey(ledger["idx_claimed_at_ttl"], [["claimed_at", 1]]) &&
    ledger["idx_claimed_at_ttl"].expireAfterSeconds === EXPECTED_LEDGER_RETENTION_SECONDS,
  "processed_events.idx_claimed_at_ttl {claimed_at} expireAfterSeconds=" + EXPECTED_LEDGER_RETENTION_SECONDS,
);

const alerts = indexMap("device_alerts");
check(sameKey(alerts["idx_region_model"], [["region", 1], ["model", 1]]),
  "device_alerts.idx_region_model {region, model}");

// --- retention: no TTL anywhere else (domain records and GridFS never expire) ---
for (const coll of ["devices", "device_state_snapshots", "firmware", "rollouts", "waves", "device_alerts"]) {
  const anyTtl = Object.values(indexMap(coll)).some((i) => i.expireAfterSeconds !== undefined);
  check(!anyTtl, coll + " has no TTL index");
}
if (present.includes("fs.files")) {
  const anyTtl = Object.values(indexMap("fs.files")).some((i) => i.expireAfterSeconds !== undefined);
  check(!anyTtl, "fs.files (GridFS) has no TTL index");
}

// --- redelivery contract: one document per event id ---
const evt = "verify-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8);
const doc = {
  _id: evt,
  ts: new Date(),
  meta: { device_id: "verify-dev", region: "verify", model: "verify" },
  cpu: 1,
  mem: 2,
  health: "ok",
};
// Ingest contract: insert-or-no-op by _id.
fleet.telemetry.updateOne({ _id: evt }, { $setOnInsert: doc }, { upsert: true });
fleet.telemetry.updateOne({ _id: evt }, { $setOnInsert: doc }, { upsert: true });
check(fleet.telemetry.countDocuments({ _id: evt }) === 1,
  "redelivery via the ingest contract leaves exactly one document");

// A raw duplicate insert must signal no-op via duplicate-key _id and never add a row.
let dupRejected = false;
try {
  fleet.telemetry.insertOne(doc);
} catch (e) {
  dupRejected = e.code === 11000;
}
check(dupRejected, "duplicate insert signals the no-op via duplicate-key _id");
check(fleet.telemetry.countDocuments({ _id: evt }) === 1,
  "duplicate insert does not create a second row");
fleet.telemetry.deleteMany({ _id: evt });

// --- dedup ledger contract: one document per (consumer, event id) ---
const claim = {
  _id: "verify-consumer:" + evt,
  consumer: "verify-consumer",
  event_id: evt,
  device_id: "verify-dev",
  claimed_at: new Date(),
};
fleet.processed_events.insertOne(claim);
let secondRejected = false;
try {
  fleet.processed_events.insertOne(claim);
} catch (e) {
  secondRejected = e.code === 11000;
}
check(secondRejected, "a second processed_events document for one pair is refused");
check(fleet.processed_events.countDocuments({ consumer: "verify-consumer", event_id: evt }) === 1,
  "one processed_events document per consumer and event id");
fleet.processed_events.deleteMany({ consumer: "verify-consumer", event_id: evt });

if (failures > 0) {
  print(failures + " check(s) failed");
  quit(1);
}
print("all checks passed");
quit(0);
EOF

"${MONGOSH[@]}" "$(cat "$SCRIPT")"
