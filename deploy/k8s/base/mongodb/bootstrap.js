const env = process.env;
const replsetName = env.MONGO_REPLSET;
const self = env.MONGO_SELF_HOST;
const topology = JSON.parse(env.TOPOLOGY_JSON);

const UNAUTHORIZED = 13;
const WRITE_CONCERN_FAILED = 64;
const ALREADY_INITIALIZED = 23;

function log(msg, fields) {
  print(JSON.stringify(Object.assign({ time: new Date().toISOString(), level: "info", service: "mongo-bootstrap", replset: replsetName, host: self, msg }, fields || {})));
}

function errText(e) {
  return String((e && e.message) || e);
}

function hello(host) {
  const conn = new Mongo(`mongodb://${host}/?directConnection=true&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000`);
  try {
    const res = conn.getDB("admin").runCommand({ hello: 1 });
    return res;
  } finally {
    conn.close();
  }
}

function run(doc) {
  const conn = new Mongo("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000");
  try {
    const res = conn.getDB("admin").runCommand(doc);
    return res;
  } catch (e) {
    if (e && e.code !== undefined) return { ok: 0, code: e.code, codeName: e.codeName, errmsg: errText(e) };
    throw e;
  } finally {
    conn.close();
  }
}

function findReplset() {
  if (topology.config.name === replsetName) return { rs: topology.config, isConfig: true };
  const rs = topology.shards.find((s) => s.name === replsetName);
  if (!rs) throw new Error(`replica set ${replsetName} not in topology`);
  return { rs, isConfig: false };
}

function peersDecision(rs) {
  let reachable = 0;
  for (const m of rs.members) {
    if (m.host === self) continue;
    try {
      const h = hello(m.host);
      if (h.setName) return "joined";
      reachable++;
    } catch (e) {
      log("peer unreachable", { peer: m.host, error: errText(e) });
    }
  }
  return reachable === rs.members.length - 1 ? "initiate" : "wait";
}

function initiate(rs, isConfig) {
  const config = {
    _id: rs.name,
    members: rs.members.map((m, i) => ({ _id: i, host: m.host, priority: m.priority })),
  };
  if (isConfig) config.configsvr = true;
  const res = run({ replSetInitiate: config });
  if (res.ok === 1 || res.code === ALREADY_INITIALIZED) {
    log("replica set initiated");
    return true;
  }
  log("initiate failed", { error: res.errmsg || res.codeName });
  return false;
}

function ensureRoot() {
  const res = run({
    createUser: env.MONGO_ROOT_USERNAME,
    pwd: env.MONGO_ROOT_PASSWORD,
    roles: [{ role: "root", db: "admin" }],
    writeConcern: { w: "majority", wtimeout: 60000 },
  });
  if (res.ok === 1 || res.code === WRITE_CONCERN_FAILED) {
    log("root user created via localhost exception");
    return true;
  }
  if (res.code === UNAUTHORIZED) {
    log("root user already exists");
    return true;
  }
  log("create root failed", { error: res.errmsg || res.codeName });
  return false;
}

function main() {
  const { rs, isConfig } = findReplset();
  const initiator = rs.members.reduce((a, b) => (b.priority > a.priority ? b : a));
  if (initiator.host !== self) {
    log("not the initiator, waiting to be added by the primary", { initiator: initiator.host });
    return;
  }

  for (;;) {
    try {
      const local = hello("127.0.0.1:27017");
      if (!local.setName) {
        const decision = peersDecision(rs);
        if (decision === "joined") {
          log("replica set already exists elsewhere, rejoining through initial sync");
          return;
        }
        if (decision === "initiate") initiate(rs, isConfig);
      } else if (local.isWritablePrimary) {
        if (ensureRoot()) return;
      }
    } catch (e) {
      log("local mongod not ready", { error: errText(e) });
    }
    sleep(3000);
  }
}

main();
