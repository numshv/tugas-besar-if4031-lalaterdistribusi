#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

NS="${NS:-sistem-informasi-kebencanaan-bnpb}"
DB="${MONGO_DB:-bnpb}"

ROOT_PW="$(kubectl -n "$NS" get secret bnpb-secrets -o jsonpath='{.data.MONGO_ROOT_PASSWORD}' | base64 -d)"
PLACEMENT="$(kubectl -n "$NS" get pods -l bnpb/mongo-member=true \
  -o jsonpath='{range .items[*]}{.metadata.labels.bnpb/replset}={.spec.nodeName}{"\n"}{end}' | grep -v '^cfg=' | tr '\n' ';')"

kubectl -n "$NS" exec ds/mongos -c mongos -- mongosh --quiet -u root --password="$ROOT_PW" --authenticationDatabase admin "$DB" --eval "
const placement = {};
for (const pair of '$PLACEMENT'.split(';').filter(Boolean)) {
  const [shard, node] = pair.split('=');
  (placement[shard] = placement[shard] || []).push(node);
}
const collections = ['hazard_events', 'exp_by_area', 'exp_by_ingested'];
for (const name of collections) {
  if (!db.getCollectionNames().includes(name)) { print('== ' + name + ': (missing)'); continue; }
  const perShard = {};
  db.getCollection(name).aggregate([{ \$collStats: { count: {} } }]).forEach(r => { perShard[r.shard] = r.count; });
  const shards = Object.keys(placement).sort();
  const total = shards.reduce((a, s) => a + (perShard[s] || 0), 0);
  print('== ' + name + ' (' + total + ' docs)');
  for (const s of shards) {
    const n = perShard[s] || 0;
    print('  ' + s.padEnd(8) + String(n).padStart(9) + '  ' + (total ? (100 * n / total).toFixed(1) : '0.0').padStart(5) + '%  on ' + placement[s].sort().join(','));
  }
  const counts = shards.map(s => perShard[s] || 0);
  const avg = total / Math.max(shards.length, 1);
  print('  shard skew max/avg = ' + (avg ? (Math.max(...counts) / avg).toFixed(2) : 'n/a'));
  const perNode = {};
  for (const s of shards) for (const node of placement[s]) perNode[node] = (perNode[node] || 0) + (perShard[s] || 0);
  for (const node of Object.keys(perNode).sort()) {
    print('  node ' + node.padEnd(20) + String(perNode[node]).padStart(9) + '  ' + (total ? (100 * perNode[node] / total).toFixed(1) : '0.0').padStart(5) + '% of all data');
  }
}
"
