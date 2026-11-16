#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
M="$ROOT/deploy/k8s/base/mongodb"
NET=bnpb-dev-mongo
IMG=mongo:7.0
PORT="${MONGO_DEV_PORT:-27017}"
ROOT_PW="${MONGO_DEV_ROOT_PASSWORD:-devroot}"
MEMBERS=(mongo-cfg-a mongo-shard0-a mongo-shard1-b)
TOPO='{"config":{"name":"cfg","members":[{"host":"mongo-cfg-a:27017","slot":"a","priority":2}]},"shards":[{"name":"shard-0","members":[{"host":"mongo-shard0-a:27017","slot":"a","priority":2}]},{"name":"shard-1","members":[{"host":"mongo-shard1-b:27017","slot":"b","priority":2}]}]}'

member() {
  local name="$1" replset="$2" role="$3"
  docker run -d --name "$name" --hostname "$name" --network "$NET" --memory 512m \
    -v "$NET-$name:/data/db" -v "$NET-keyfile:/keyfile:ro" \
    -v "$ROOT/deploy/k8s/overlays/dev/mongod.conf:/etc/mongo/mongod.conf:ro" \
    -v "$M/mongod-start.sh:/scripts/mongod-start.sh:ro" -v "$M/bootstrap.js:/scripts/bootstrap.js:ro" \
    -e MONGO_REPLSET="$replset" -e MONGO_ROLE="$role" -e MONGO_SELF_HOST="$name:27017" \
    -e TOPOLOGY_JSON="$TOPO" -e MONGO_ROOT_USERNAME=root -e MONGO_ROOT_PASSWORD="$ROOT_PW" \
    "$IMG" bash /scripts/mongod-start.sh >/dev/null
}

up() {
  docker network create "$NET" >/dev/null 2>&1 || true
  docker run --rm -v "$NET-keyfile:/keyfile" "$IMG" sh -c \
    '[ -f /keyfile/mongo-keyfile ] || openssl rand -base64 756 > /keyfile/mongo-keyfile; chown 999:999 /keyfile/mongo-keyfile; chmod 400 /keyfile/mongo-keyfile'
  member mongo-cfg-a cfg configsvr
  member mongo-shard0-a shard-0 shardsvr
  member mongo-shard1-b shard-1 shardsvr
  docker run -d --name mongo-dev-mongos --network "$NET" --network-alias mongodb --memory 256m \
    -p "127.0.0.1:$PORT:27017" -v "$NET-keyfile:/keyfile:ro" "$IMG" \
    mongos --configdb cfg/mongo-cfg-a:27017 --port 27017 --bind_ip_all --keyFile /keyfile/mongo-keyfile >/dev/null
  docker run --rm --network "$NET" -v "$M/mongo-init.js:/scripts/mongo-init.js:ro" \
    -e MONGOS_HOST=mongodb:27017 -e INIT_TIMEOUT_SECONDS=300 -e MONGO_DB=bnpb -e TOPOLOGY_JSON="$TOPO" \
    -e MONGO_ROOT_USERNAME=root -e MONGO_ROOT_PASSWORD="$ROOT_PW" \
    -e MONGO_APP_USERNAME=bnpb_app -e MONGO_APP_PASSWORD=devapp \
    -e MONGO_MONITOR_USERNAME=bnpb_monitor -e MONGO_MONITOR_PASSWORD=devmonitor \
    "$IMG" mongosh --nodb --quiet /scripts/mongo-init.js | tail -1
  echo "root: mongodb://root:$ROOT_PW@127.0.0.1:$PORT/?authSource=admin"
  echo "app:  mongodb://bnpb_app:devapp@127.0.0.1:$PORT/bnpb?authSource=bnpb"
}

down() {
  docker rm -f "${MEMBERS[@]}" mongo-dev-mongos >/dev/null 2>&1 || true
  for v in keyfile "${MEMBERS[@]}"; do docker volume rm "$NET-$v" >/dev/null 2>&1 || true; done
  docker network rm "$NET" >/dev/null 2>&1 || true
  echo "removed"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 1 ;;
esac
