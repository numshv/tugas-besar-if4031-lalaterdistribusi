#!/bin/bash
set -u

: "${MONGO_REPLSET:?}" "${MONGO_ROLE:?}" "${MONGO_SELF_HOST:?}"

docker-entrypoint.sh mongod \
  --config /etc/mongo/mongod.conf \
  "--$MONGO_ROLE" \
  --dbpath /data/db \
  --replSet "$MONGO_REPLSET" \
  --port 27017 \
  --bind_ip_all \
  --keyFile /keyfile/mongo-keyfile &
pid=$!

term() {
  kill -TERM "$pid" 2>/dev/null || true
}
trap term TERM INT

mongosh --nodb --quiet /scripts/bootstrap.js &

wait "$pid"
status=$?
if kill -0 "$pid" 2>/dev/null; then
  wait "$pid"
  status=$?
fi
exit "$status"
