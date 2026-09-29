// FleetOps MongoDB bootstrap.
//
// Applied by the local stack's MongoDB on first startup — the stage-1 manifests mount this file
// into /docker-entrypoint-initdb.d. It creates the fleet collections, their required-field
// validators, and the index set defined in openspec/specs/mongo-data-model/spec.md; the field
// contract itself is documented in deploy/README.md. Re-running against an unchanged
// configuration is safe: existing collections and indexes are left alone.

// TELEMETRY_RETENTION_DAYS is the single named configuration value for heartbeat retention:
// the TTL index below is built from it, and the ingestion path (stage 3) reads the same
// FLEETOPS_TELEMETRY_RETENTION_DAYS variable so one change moves both.
const TELEMETRY_RETENTION_DAYS = Number(process.env.FLEETOPS_TELEMETRY_RETENTION_DAYS || 7);
const TELEMETRY_RETENTION_SECONDS = TELEMETRY_RETENTION_DAYS * 24 * 60 * 60;

// PROCESSED_EVENTS_RETENTION_DAYS is the single named configuration value for the consumer
// dedup ledger's retention: the TTL index below is built from it, and the consumer's ledger
// writes read the same FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS variable so one change moves
// both. The ledger is pipeline bookkeeping, so — unlike domain records — it does expire.
const PROCESSED_EVENTS_RETENTION_DAYS = Number(process.env.FLEETOPS_PROCESSED_EVENTS_RETENTION_DAYS || 7);
const PROCESSED_EVENTS_RETENTION_SECONDS = PROCESSED_EVENTS_RETENTION_DAYS * 24 * 60 * 60;

const fleet = db.getSiblingDB("fleetops");

function ensureCollection(name, options) {
  if (fleet.getCollectionInfos({ name: name }).length > 0) {
    return;
  }
  fleet.createCollection(name, options);
}

// devices: one heterogeneous document per device, _id = device identity.
ensureCollection("devices", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["model", "region", "current_fw", "status", "last_heartbeat"],
      properties: {
        model: { bsonType: "string" },
        region: { bsonType: "string" },
        current_fw: { bsonType: "string" },
        status: { bsonType: "string" },
        last_heartbeat: { bsonType: "date" },
      },
    },
  },
});
fleet.devices.createIndex({ region: 1, model: 1 }, { name: "idx_region_model" });
fleet.devices.createIndex({ status: 1 }, { name: "idx_status" });

// device_state_snapshots: the device workflow's authoritative state projected by the snapshot
// activity, one document per device, _id = device identity. The write path is monotone in
// snapshot_at, so the document always holds the newest projected state. Domain record: never
// expires.
ensureCollection("device_state_snapshots", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["region", "model", "current_fw", "online", "last_heartbeat", "config", "snapshot_at"],
      properties: {
        region: { bsonType: "string" },
        model: { bsonType: "string" },
        current_fw: { bsonType: "string" },
        online: { bsonType: "bool" },
        last_heartbeat: { bsonType: "date" },
        pending: {
          bsonType: "object",
          required: ["command_id", "device_id", "kind", "dispatched"],
          properties: {
            command_id: { bsonType: "string" },
            device_id: { bsonType: "string" },
            kind: { bsonType: "string" },
            firmware_id: { bsonType: "string" },
            version: { bsonType: "string" },
            checksum: { bsonType: "string" },
            reason: { bsonType: "string" },
            dispatched: { bsonType: "bool" },
          },
        },
        update_status: {
          bsonType: "object",
          required: ["firmware_id", "phase"],
          properties: {
            firmware_id: { bsonType: "string" },
            phase: { bsonType: "string" },
            progress_percent: { bsonType: ["int", "long"] },
            detail: { bsonType: "string" },
          },
        },
        config: {
          bsonType: "object",
          required: ["version"],
          properties: {
            version: { bsonType: ["int", "long"] },
          },
        },
        snapshot_at: { bsonType: "date" },
      },
    },
  },
});
fleet.device_state_snapshots.createIndex({ region: 1, model: 1 }, { name: "idx_region_model" });
fleet.device_state_snapshots.createIndex({ online: 1 }, { name: "idx_online" });

// firmware: metadata only, _id = firmware id; the binary lives in GridFS (see gridfs_id).
// models are the target device models the upload declared, checked against the devices
// registry at upload time; size and created_at record what the upload stored.
ensureCollection("firmware", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["version", "models", "checksum", "size", "created_at", "gridfs_id"],
      properties: {
        version: { bsonType: "string" },
        models: { bsonType: "array", items: { bsonType: "string" }, minItems: 1 },
        checksum: { bsonType: "string" },
        size: { bsonType: ["int", "long"] },
        created_at: { bsonType: "date" },
        gridfs_id: { bsonType: "string" },
      },
    },
  },
});
fleet.firmware.createIndex({ version: 1 }, { name: "idx_version", unique: true });

// rollouts: one document per rollout, _id = rollout id.
// status gains the non-terminal rolling_back: a rollout that has stopped deciding and is running
// the compensations it derived from the wave that failed it. The terminal rolled_back status is
// recorded only once every compensating step has run.
// rollback is the rollback the workflow writes as it compensates: why it happened, the plan's steps
// with what each achieved, the inventory the reconciliation established, and the devices it could
// not restore. It is deliberately optional, so a document written before it existed stays updatable
// by the new build and a document written with it stays valid for the previous validator.
ensureCollection("rollouts", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["firmware_id", "status", "temporal_wf_id", "region", "model"],
      properties: {
        firmware_id: { bsonType: "string" },
        status: { bsonType: "string" },
        temporal_wf_id: { bsonType: "string" },
        region: { bsonType: "string" },
        model: { bsonType: "string" },
        rollback: {
          bsonType: "object",
          required: ["outcome", "steps", "inventory", "unrestored_device_ids"],
          properties: {
            outcome: { bsonType: "string" },
            steps: {
              bsonType: "array",
              items: {
                bsonType: "object",
                required: ["kind", "status", "devices"],
                properties: {
                  kind: { bsonType: "string" },
                  // The wave a compensating step compensates; absent for the steps that
                  // compensate the bookkeeping or announce the rollback.
                  wave_id: { bsonType: "string" },
                  status: { bsonType: "string" },
                  devices: { bsonType: ["int", "long"] },
                  restored: { bsonType: ["int", "long"] },
                  failed: { bsonType: ["int", "long"] },
                  unreported: { bsonType: ["int", "long"] },
                  skipped: { bsonType: ["int", "long"] },
                  unavailable: { bsonType: ["int", "long"] },
                  agreed: { bsonType: ["int", "long"] },
                  corrected: { bsonType: ["int", "long"] },
                  unverified: { bsonType: ["int", "long"] },
                  detail: { bsonType: "string" },
                },
              },
            },
            inventory: {
              bsonType: "array",
              items: {
                bsonType: "object",
                required: ["version", "devices"],
                properties: {
                  version: { bsonType: "string" },
                  devices: { bsonType: ["int", "long"] },
                },
              },
            },
            unrestored_device_ids: {
              bsonType: "array",
              items: { bsonType: "string" },
            },
          },
        },
      },
    },
  },
});
fleet.rollouts.createIndex({ status: 1 }, { name: "idx_status" });
fleet.rollouts.createIndex({ firmware_id: 1 }, { name: "idx_firmware_id" });

// waves: one document per canary wave, _id = wave id; waves list in (rollout_id, percent) order.
// device_ids is the target set the wave was dispatched to and started_at opens its health
// window: a wave's health is evaluated over exactly those devices and never over heartbeats
// recorded before started_at, so re-resolving the target group cannot move the denominator.
// device_ids has no minItems — a canary share smaller than one device legitimately targets
// nobody, and that wave is recorded rather than skipped.
// failed_device_ids and unreported_device_ids are the outcomes the wave's dispatch collected: the
// devices that reported a failed update and the devices that never reported before the wave
// stopped waiting on them. They are written by every wave write, always as arrays — a wave whose
// devices all succeeded says so with two empty arrays — and they are deliberately not required, so
// a document written before they existed stays updatable by the new build and a document written
// by the new build stays valid for the previous validator.
ensureCollection("waves", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["rollout_id", "percent", "status", "success_rate", "device_ids", "started_at"],
      properties: {
        rollout_id: { bsonType: "string" },
        percent: { bsonType: ["int", "long"] },
        status: { bsonType: "string" },
        success_rate: { bsonType: ["double", "int", "long"] },
        device_ids: { bsonType: "array", items: { bsonType: "string" } },
        started_at: { bsonType: "date" },
        failed_device_ids: { bsonType: "array", items: { bsonType: "string" } },
        unreported_device_ids: { bsonType: "array", items: { bsonType: "string" } },
      },
    },
  },
});
fleet.waves.createIndex({ rollout_id: 1, percent: 1 }, { name: "idx_rollout_percent" });

// telemetry: heartbeat time-series shape — _id = event id, ts = measurement time, meta carries
// device identity plus region/model, metrics are top-level. This is a plain collection with a
// TTL index rather than a MongoDB time-series collection: time-series collections have no
// unique _id index and happily accept duplicate _id rows, so they cannot enforce the
// redelivery-no-op contract at the storage layer. See the change notes for the probe evidence.
ensureCollection("telemetry", {});
fleet.telemetry.createIndex({ "meta.device_id": 1, ts: -1 }, { name: "idx_device_ts" });
fleet.telemetry.createIndex(
  { "meta.region": 1, "meta.model": 1, ts: -1 },
  { name: "idx_region_model_ts" },
);
fleet.telemetry.createIndex(
  { ts: 1 },
  { name: "idx_ts_ttl", expireAfterSeconds: TELEMETRY_RETENTION_SECONDS },
);

// processed_events: the consumer dedup ledger, one document per (consumer, event id) pair,
// _id = "<consumer>:<event id>". processed_at is absent until the consumer's side effect is
// durable, and a document without it is unfinished work rather than an applied event — the
// consumer resumes it. Pipeline bookkeeping, not a domain record: the TTL index bounds it.
ensureCollection("processed_events", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["consumer", "event_id", "device_id", "claimed_at"],
      properties: {
        consumer: { bsonType: "string" },
        event_id: { bsonType: "string" },
        device_id: { bsonType: "string" },
        claimed_at: { bsonType: "date" },
        processed_at: { bsonType: "date" },
      },
    },
  },
});
fleet.processed_events.createIndex(
  { consumer: 1, event_id: 1 },
  { name: "idx_consumer_event", unique: true },
);
fleet.processed_events.createIndex({ device_id: 1 }, { name: "idx_device_id" });
fleet.processed_events.createIndex(
  { claimed_at: 1 },
  { name: "idx_claimed_at_ttl", expireAfterSeconds: PROCESSED_EVENTS_RETENTION_SECONDS },
);

// device_alerts: at most one alert document per device, _id = device identity, refreshed in
// place by the alerting consumer's monotone upsert (earliest first_seen_at, lowest
// min_health, newest last_seen_at). Domain record: never expires, and it never holds the raw
// heartbeat payload.
ensureCollection("device_alerts", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["device_id", "region", "model", "threshold", "min_health", "first_seen_at", "last_seen_at"],
      properties: {
        device_id: { bsonType: "string" },
        region: { bsonType: "string" },
        model: { bsonType: "string" },
        threshold: { bsonType: ["double", "int", "long"] },
        min_health: { bsonType: ["double", "int", "long"] },
        first_seen_at: { bsonType: "date" },
        last_seen_at: { bsonType: "date" },
      },
    },
  },
});
fleet.device_alerts.createIndex({ region: 1, model: 1 }, { name: "idx_region_model" });

print("fleetops bootstrap complete: telemetry retention " + TELEMETRY_RETENTION_DAYS + " days"
  + ", processed-events retention " + PROCESSED_EVENTS_RETENTION_DAYS + " days");
