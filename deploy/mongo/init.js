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
      },
    },
  },
});
fleet.rollouts.createIndex({ status: 1 }, { name: "idx_status" });
fleet.rollouts.createIndex({ firmware_id: 1 }, { name: "idx_firmware_id" });

// waves: one document per canary wave, _id = wave id; waves list in (rollout_id, percent) order.
ensureCollection("waves", {
  validationAction: "error",
  validationLevel: "strict",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["rollout_id", "percent", "status", "success_rate"],
      properties: {
        rollout_id: { bsonType: "string" },
        percent: { bsonType: ["int", "long"] },
        status: { bsonType: "string" },
        success_rate: { bsonType: ["double", "int", "long"] },
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

print("fleetops bootstrap complete: telemetry retention " + TELEMETRY_RETENTION_DAYS + " days");
