#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="$ROOT/deploy/k8s/base/mongodb"
OUT="$BASE/generated"
DEV="$ROOT/deploy/k8s/overlays/dev"
DEL="$DEV/mongo-delete-replicas"

SHARDS=("0:a,b,c" "1:b,c,d" "2:c,a,d" "3:d,a,b")
CFG_SLOTS="a,b,c"
PORT=27017

member_yaml() {
  local name="$1" unit="$2" replset="$3" slot="$4" mode="$5" app="$6"
  cat <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $name
  labels:
    app: $app
    bnpb/mongo-unit: $unit
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    bnpb/mongo-unit: $unit
  ports:
    - name: mongo
      port: $PORT
      targetPort: $PORT
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: $name
  labels:
    app: $app
    bnpb/mongo-member: "true"
    bnpb/mongo-unit: $unit
spec:
  serviceName: $name
  replicas: 1
  selector:
    matchLabels:
      bnpb/mongo-unit: $unit
  template:
    metadata:
      labels:
        app: $app
        role: store
        bnpb/mongo-member: "true"
        bnpb/mongo-unit: $unit
        bnpb/replset: $replset
        bnpb/slot: $slot
    spec:
      nodeSelector:
        bnpb/slot: $slot
      terminationGracePeriodSeconds: 30
      initContainers:
        - name: keyfile
          image: mongo:7.0
          command:
            - sh
            - -c
            - cp /keyfile-src/mongo-keyfile /keyfile/mongo-keyfile && chown 999:999 /keyfile/mongo-keyfile && chmod 400 /keyfile/mongo-keyfile
          volumeMounts:
            - name: keyfile-src
              mountPath: /keyfile-src
              readOnly: true
            - name: keyfile
              mountPath: /keyfile
          resources:
            requests:
              memory: 512Mi
            limits:
              memory: 1Gi
      containers:
        - name: mongod
          image: mongo:7.0
          command: ["bash", "/scripts/mongod-start.sh"]
          env:
            - name: MONGO_REPLSET
              value: $replset
            - name: MONGO_ROLE
              value: $mode
            - name: MONGO_SELF_HOST
              value: $name:$PORT
            - name: TOPOLOGY_JSON
              valueFrom:
                configMapKeyRef:
                  name: mongo-topology
                  key: topology.json
            - name: MONGO_ROOT_USERNAME
              valueFrom:
                configMapKeyRef:
                  name: mongodb-config
                  key: MONGO_ROOT_USERNAME
            - name: MONGO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: bnpb-secrets
                  key: MONGO_ROOT_PASSWORD
          ports:
            - name: mongo
              containerPort: $PORT
          volumeMounts:
            - name: data
              mountPath: /data/db
            - name: keyfile
              mountPath: /keyfile
              readOnly: true
            - name: conf
              mountPath: /etc/mongo
              readOnly: true
            - name: scripts
              mountPath: /scripts
              readOnly: true
          resources:
            requests:
              memory: 512Mi
            limits:
              memory: 1Gi
          readinessProbe:
            exec:
              command: ["bash", "-c", "echo > /dev/tcp/127.0.0.1/$PORT"]
            periodSeconds: 5
            timeoutSeconds: 2
          livenessProbe:
            exec:
              command: ["bash", "-c", "echo > /dev/tcp/127.0.0.1/$PORT"]
            initialDelaySeconds: 30
            periodSeconds: 10
            timeoutSeconds: 2
            failureThreshold: 6
      volumes:
        - name: keyfile-src
          secret:
            secretName: mongo-keyfile
            defaultMode: 0400
        - name: keyfile
          emptyDir:
            medium: Memory
            sizeLimit: 1Mi
        - name: conf
          configMap:
            name: mongod-conf
        - name: scripts
          configMap:
            name: mongod-start
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        storageClassName: local-path
        resources:
          requests:
            storage: 5Gi
EOF
}

delete_yaml() {
  local kind="$1" api="$2" name="$3"
  cat <<EOF
\$patch: delete
apiVersion: $api
kind: $kind
metadata:
  name: $name
EOF
}

add_delete() {
  local name="$1"
  delete_yaml StatefulSet apps/v1 "$name" >"$DEL/$name-statefulset.yaml"
  delete_yaml Service v1 "$name" >"$DEL/$name-service.yaml"
  deletes+=("$name-statefulset.yaml" "$name-service.yaml")
}

members_json() {
  local prefix="$1" slots="$2" only_first="$3" first=1 out="" slot priority
  IFS=',' read -ra list <<<"$slots"
  for slot in "${list[@]}"; do
    if [ "$first" -eq 1 ]; then priority=2; else priority=1; fi
    [ -n "$out" ] && out="$out, "
    out="$out{\"host\": \"$prefix-$slot:$PORT\", \"slot\": \"$slot\", \"priority\": $priority}"
    first=0
    [ "$only_first" = "yes" ] && break
  done
  printf '%s' "$out"
}

topology_json() {
  local only_first="$1" shards="" entry n slots
  for entry in "${SHARDS[@]}"; do
    n="${entry%%:*}"
    slots="${entry#*:}"
    [ -n "$shards" ] && shards="$shards,"$'\n'
    shards="$shards    {\"name\": \"shard-$n\", \"members\": [$(members_json "mongo-shard$n" "$slots" "$only_first")]}"
  done
  cat <<EOF
{
  "config": {"name": "cfg", "members": [$(members_json "mongo-cfg" "$CFG_SLOTS" "$only_first")]},
  "shards": [
$shards
  ]
}
EOF
}

configdb() {
  local only_first="$1" hosts="" slot
  IFS=',' read -ra list <<<"$CFG_SLOTS"
  for slot in "${list[@]}"; do
    [ -n "$hosts" ] && hosts="$hosts,"
    hosts="${hosts}mongo-cfg-$slot:$PORT"
    [ "$only_first" = "yes" ] && break
  done
  printf 'cfg/%s' "$hosts"
}

rm -rf "$OUT" "$DEL"
mkdir -p "$OUT" "$DEL"
files=()
deletes=()

for entry in "${SHARDS[@]}"; do
  n="${entry%%:*}"
  IFS=',' read -ra slots <<<"${entry#*:}"
  for i in "${!slots[@]}"; do
    slot="${slots[$i]}"
    name="mongo-shard$n-$slot"
    member_yaml "$name" "shard$n-$slot" "shard-$n" "$slot" shardsvr mongo-shard >"$OUT/$name.yaml"
    files+=("$name.yaml")
    if [ "$i" -gt 0 ]; then add_delete "$name"; fi
  done
done

IFS=',' read -ra cfg_slots <<<"$CFG_SLOTS"
for i in "${!cfg_slots[@]}"; do
  slot="${cfg_slots[$i]}"
  name="mongo-cfg-$slot"
  member_yaml "$name" "cfg-$slot" cfg "$slot" configsvr mongo-cfg >"$OUT/$name.yaml"
  files+=("$name.yaml")
  if [ "$i" -gt 0 ]; then add_delete "$name"; fi
done

{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  for f in "${files[@]}"; do echo "  - $f"; done
} >"$OUT/kustomization.yaml"

topology_json no >"$BASE/topology.json"
configdb no >"$BASE/configdb.txt"
topology_json yes >"$DEV/mongo-topology.json"
configdb yes >"$DEV/mongo-configdb.txt"
{
  echo "apiVersion: kustomize.config.k8s.io/v1alpha1"
  echo "kind: Component"
  echo "patches:"
  for f in "${deletes[@]}"; do echo "  - path: $f"; done
} >"$DEL/kustomization.yaml"

echo "generated ${#files[@]} members in $OUT"
