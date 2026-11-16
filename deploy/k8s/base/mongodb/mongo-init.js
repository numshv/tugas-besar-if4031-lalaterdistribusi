const env = process.env;
const topology = JSON.parse(env.TOPOLOGY_JSON);
const dbName = env.MONGO_DB;
const rootUser = env.MONGO_ROOT_USERNAME;
const rootPassword = env.MONGO_ROOT_PASSWORD;
const deadline = Date.now() + Number(env.INIT_TIMEOUT_SECONDS || 600) * 1000;

function log(msg, fields) {
  print(JSON.stringify(Object.assign({ time: new Date().toISOString(), level: "info", service: "mongo-init", msg }, fields || {})));
}

function errText(e) {
  return String((e && e.message) || e);
}

function uri(host) {
  const auth = `${encodeURIComponent(rootUser)}:${encodeURIComponent(rootPassword)}`;
  return `mongodb://${auth}@${host}/admin?authSource=admin&directConnection=true&serverSelectionTimeoutMS=3000&connectTimeoutMS=3000`;
}

function withAdmin(host, fn) {
  const conn = new Mongo(uri(host));
  try {
    const result = fn(conn.getDB("admin"));
    return result;
  } finally {
    conn.close();
  }
}

function cmd(db, doc) {
  try {
    const res = db.runCommand(doc);
    return res;
  } catch (e) {
    if (e && e.code !== undefined) return { ok: 0, code: e.code, codeName: e.codeName, errmsg: errText(e) };
    throw e;
  }
}

function must(res, what) {
  if (res.ok !== 1) throw new Error(`${what}: ${res.codeName || res.code} ${res.errmsg || ""}`);
  return res;
}

function until(what, fn) {
  for (;;) {
    try {
      const result = fn();
      if (result) return result;
    } catch (e) {
      log("retrying", { step: what, error: errText(e) });
    }
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${what}`);
    sleep(2000);
  }
}

function findPrimary(rs) {
  for (const m of rs.members) {
    try {
      const hello = withAdmin(m.host, (admin) => admin.runCommand({ hello: 1 }));
      if (hello.isWritablePrimary) return m.host;
    } catch (e) {
      continue;
    }
  }
  return null;
}

function waitPrimary(rs) {
  const primary = until(`primary of ${rs.name}`, () => findPrimary(rs));
  log("primary ready", { replset: rs.name, primary });
  return primary;
}

function ensureUser(db, user, password, roles) {
  if (db.getUser(user)) {
    db.updateUser(user, { pwd: password, roles });
    log("user updated", { db: db.getName(), user });
  } else {
    db.createUser({ user, pwd: password, roles });
    log("user created", { db: db.getName(), user });
  }
}

const monitorRoles = [
  { role: "clusterMonitor", db: "admin" },
  { role: "read", db: "config" },
];

function ensureShardLocalMonitor(rs, primary) {
  withAdmin(primary, (admin) => ensureUser(admin, env.MONGO_MONITOR_USERNAME, env.MONGO_MONITOR_PASSWORD, monitorRoles));
  log("shard-local monitor user ready", { replset: rs.name });
}

function ensureShards(admin) {
  const listed = must(cmd(admin, { listShards: 1 }), "listShards").shards.map((s) => s._id);
  for (const rs of topology.shards) {
    if (listed.includes(rs.name)) {
      log("shard already added", { shard: rs.name });
      continue;
    }
    const seed = `${rs.name}/${rs.members.map((m) => m.host).join(",")}`;
    must(cmd(admin, { addShard: seed, name: rs.name }), `addShard ${rs.name}`);
    log("shard added", { shard: rs.name, seed });
  }
}

function ensureHazardEvents(client) {
  const store = client.getDB(dbName);
  const ns = `${dbName}.hazard_events`;
  const col = store.getCollection("hazard_events");

  col.createIndex({ _id: "hashed" });
  const meta = client.getDB("config").getCollection("collections").findOne({ _id: ns });
  if (meta && meta.key && !meta.dropped) {
    log("collection already sharded", { ns, key: meta.key });
  } else {
    must(cmd(client.getDB("admin"), { shardCollection: ns, key: { _id: "hashed" } }), `shardCollection ${ns}`);
    log("collection sharded", { ns, key: { _id: "hashed" } });
  }

  for (const idx of col.getIndexes()) {
    const k = idx.key || {};
    if (idx.unique && k.source === 1 && k.source_ref_id === 1 && Object.keys(k).length === 2) {
      col.dropIndex(idx.name);
      log("dropped legacy unique index", { index: idx.name });
    }
  }

  col.createIndex({ ingested_at: -1, _id: -1 }, { name: "ingested_at_id_desc" });
  col.createIndex({ hazard_type: 1, ingested_at: -1, _id: -1 }, { name: "hazard_type_ingested_at_id_desc" });
  col.createIndex({ hazard_type: 1, occurred_at: -1, _id: -1 }, { name: "hazard_type_occurred_at_id_desc" });
  col.createIndex({ hazard_type: 1, occurred_at: 1 }, { name: "hazard_type_occurred_at_asc" });
  col.createIndex({ source: 1, source_ref_id: 1 }, { name: "source_source_ref_id" });
  col.createIndex({ source: 1, ingested_at: 1, _id: 1 }, { name: "source_ingested_at_id_asc" });
  log("indexes ready", { ns });

  if (!store.getCollectionNames().includes("fences")) {
    store.createCollection("fences");
    log("collection created", { ns: `${dbName}.fences` });
  }
}

function ensureVolcanoReference(client) {
  const col = client.getDB(dbName).getCollection("volcano_reference");
  if (col.countDocuments({}) > 0) {
    log("volcano_reference already seeded");
    return;
  }
  col.insertMany([
    { _id: "VOL-MERAPI",        name: "Gunung Merapi",             province: "DI Yogyakarta / Jawa Tengah", latitude: -7.5407, longitude: 110.4457 },
    { _id: "VOL-SEMERU",        name: "Gunung Semeru",             province: "Jawa Timur",                  latitude: -8.1077, longitude: 112.9224 },
    { _id: "VOL-SINABUNG",      name: "Gunung Sinabung",           province: "Sumatera Utara",              latitude:  3.1700, longitude:  98.3920 },
    { _id: "VOL-KELUD",         name: "Gunung Kelud",              province: "Jawa Timur",                  latitude: -7.9320, longitude: 112.3080 },
    { _id: "VOL-BROMO",         name: "Gunung Bromo",              province: "Jawa Timur",                  latitude: -7.9425, longitude: 112.9530 },
    { _id: "VOL-AGUNG",         name: "Gunung Agung",              province: "Bali",                        latitude: -8.3430, longitude: 115.5080 },
    { _id: "VOL-RAUNG",         name: "Gunung Raung",              province: "Jawa Timur",                  latitude: -8.1250, longitude: 114.0420 },
    { _id: "VOL-KRAKATAU",      name: "Gunung Anak Krakatau",      province: "Lampung",                     latitude: -6.1020, longitude: 105.4230 },
    { _id: "VOL-MARAPI",        name: "Gunung Marapi",             province: "Sumatera Barat",              latitude: -0.3810, longitude: 100.4730 },
    { _id: "VOL-LEWOTOBI",      name: "Gunung Lewotobi Laki-laki", province: "Nusa Tenggara Timur",         latitude: -8.5420, longitude: 122.7750 },
    { _id: "VOL-ILE-LEWOTOLOK", name: "Gunung Ile Lewotolok",      province: "Nusa Tenggara Timur",         latitude: -8.2720, longitude: 123.5050 },
    { _id: "VOL-DUKONO",        name: "Gunung Dukono",             province: "Maluku Utara",                latitude:  1.6930, longitude: 127.8940 },
    { _id: "VOL-IBU",           name: "Gunung Ibu",                province: "Maluku Utara",                latitude:  1.4880, longitude: 127.6300 },
    { _id: "VOL-GAMALAMA",      name: "Gunung Gamalama",           province: "Maluku Utara",                latitude:  0.8090, longitude: 127.3300 },
    { _id: "VOL-SLAMET",        name: "Gunung Slamet",             province: "Jawa Tengah",                 latitude: -7.2420, longitude: 109.2080 },
  ]);
  log("volcano_reference seeded", { count: col.countDocuments({}) });
}

function main() {
  waitPrimary(topology.config);
  for (const rs of topology.shards) {
    waitPrimary(rs);
    until(`monitor user on ${rs.name}`, () => {
      const primary = findPrimary(rs);
      if (!primary) return false;
      ensureShardLocalMonitor(rs, primary);
      return true;
    });
  }

  until("mongos ready", () => withAdmin(env.MONGOS_HOST, (admin) => admin.runCommand({ ping: 1 }).ok === 1));

  until("cluster setup via mongos", () => {
    const client = new Mongo(uri(env.MONGOS_HOST));
    try {
      const admin = client.getDB("admin");
      ensureShards(admin);
      must(cmd(admin, { setDefaultRWConcern: 1, defaultWriteConcern: { w: "majority", wtimeout: 5000 } }), "setDefaultRWConcern");
      ensureUser(client.getDB(dbName), env.MONGO_APP_USERNAME, env.MONGO_APP_PASSWORD, [{ role: "readWrite", db: dbName }]);
      ensureUser(admin, env.MONGO_MONITOR_USERNAME, env.MONGO_MONITOR_PASSWORD, monitorRoles);
      ensureHazardEvents(client);
      ensureVolcanoReference(client);
      return true;
    } finally {
      client.close();
    }
  });

  log("mongo init done", { shards: topology.shards.map((s) => s.name) });
}

try {
  main();
} catch (e) {
  print(JSON.stringify({ time: new Date().toISOString(), level: "error", service: "mongo-init", msg: "mongo init failed", error: errText(e) }));
  quit(1);
}
