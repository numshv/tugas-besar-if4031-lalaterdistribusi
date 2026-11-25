#!/usr/bin/env bash
# Usage: scripts/deploy.sh dev|demo
set -euo pipefail

MODE="${1:-}"
case "$MODE" in dev|demo) ;; *) echo "usage: $0 dev|demo" >&2; exit 1 ;; esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=sistem-informasi-kebencanaan-bnpb
CLUSTER="${K3D_CLUSTER:-bnpb}"
PART_OF=app.kubernetes.io/part-of=bnpb
SLOTS=(a b c d)
SECRET_KEYS=(
  BMKG_API_KEY PVMBG_TOKEN PVMBG_ADMIN_TOKEN
  CLIENT_MEDIA_SECRET CLIENT_FIELD_SECRET CLIENT_OPS_SECRET
  MONGO_ROOT_PASSWORD MONGO_APP_PASSWORD MONGO_MONITOR_PASSWORD
)
OPTIONAL_SECRET_KEYS=(AUDIT_TOKENS SATELLITE_TOKEN CLIENT_ANALYTICS_SECRET)
PRUNE_KINDS=(
  core/v1/ConfigMap core/v1/Service
  apps/v1/Deployment apps/v1/StatefulSet apps/v1/DaemonSet
  batch/v1/Job networking.k8s.io/v1/Ingress networking.k8s.io/v1/NetworkPolicy
  autoscaling/v2/HorizontalPodAutoscaler
)
OPTIONAL_SECRET_KEYS+=(GRAFANA_ADMIN_PASSWORD)

CHANGED_SERVICES=()

die() { echo "error: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

env_get() {
  local line
  line="$(grep -E "^$1=" "$ROOT/.env" | tail -1 | tr -d '\r')" || die "$1 missing in .env"
  line="${line#*=}"
  line="${line%\"}"
  echo "${line#\"}"
}

require_tools() {
  local t
  for t in "$@"; do command -v "$t" >/dev/null || die "$t not found in PATH"; done
}

k3d_nodes() {
  docker ps --filter "label=k3d.cluster=$CLUSTER" --format '{{.Names}} {{.Label "k3d.role"}}' |
    awk '$2 == "server" || $2 == "agent" { print $1 }'
}

load_dev_images() {
  step "build and load images into k3d cluster '$CLUSTER'"
  local svc images=() node img ref before
  CHANGED_SERVICES=()
  for svc in "$ROOT"/services/*/; do
    svc="$(basename "$svc")"
    before="$(docker image inspect -f '{{.Id}}' "bnpb/$svc:dev" 2>/dev/null || true)"
    docker build -q -t "bnpb/$svc:dev" -f "$ROOT/services/$svc/Dockerfile" "$ROOT" >/dev/null
    images+=("bnpb/$svc:dev")
    [ "$before" = "$(docker image inspect -f '{{.Id}}' "bnpb/$svc:dev")" ] || CHANGED_SERVICES+=("$svc")
  done
  k3d image import "${images[@]}" -c "$CLUSTER"

  # k3d image import silently skips multi-arch images, so stream amd64 copies via ctr.
  for img in mongo:7.0 apache/kafka:3.9.0 prom/prometheus:v2.55.1 grafana/grafana:11.3.1 jaegertracing/all-in-one:1.63.0; do
    docker image inspect "$img" >/dev/null 2>&1 || docker pull "$img"
    ref="docker.io/$img"
    [[ "$img" == */* ]] || ref="docker.io/library/$img"
    for node in $(k3d_nodes); do
      docker exec "$node" ctr -n k8s.io images ls -q | grep -qx "$ref" && continue
      echo "loading $img into $node"
      docker save --platform linux/amd64 "$img" | docker exec -i "$node" ctr -n k8s.io images import - >/dev/null
    done
  done
}

# Loading images into every node keeps the API server busy for a while; wait for it.
wait_api() {
  local i
  for i in $(seq 1 60); do
    kubectl get namespace default --request-timeout=5s >/dev/null 2>&1 && return 0
    sleep 2
  done
  die "Kubernetes API server is not ready"
}

apply_secrets() {
  step "namespace and secrets"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

  local key value literals=()
  for key in "${SECRET_KEYS[@]}"; do
    value="$(env_get "$key")"
    literals+=("--from-literal=$key=$value")
  done
  for key in "${OPTIONAL_SECRET_KEYS[@]}"; do
    value=""
    grep -qE "^$key=" "$ROOT/.env" && value="$(env_get "$key")"
    if [ -z "$value" ]; then
      echo "warning: $key not set in .env, skipped"
      continue
    fi
    literals+=("--from-literal=$key=$value")
  done
  kubectl -n "$NS" create secret generic bnpb-secrets "${literals[@]}" --dry-run=client -o yaml | kubectl apply -f -

  bash "$ROOT/scripts/gen-keys.sh"
  bash "$ROOT/scripts/gen-mongo-keyfile.sh"
  kubectl -n "$NS" create secret generic auth-jwt-key \
    --from-file=jwt_private.pem="$ROOT/infra/keys/jwt_private.pem" --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NS" create secret generic mongo-keyfile \
    --from-file=mongo-keyfile="$ROOT/infra/keys/mongo-keyfile" --dry-run=client -o yaml | kubectl apply -f -
  value="$(env_get PANTAI_AUTH_TOKEN)"
  kubectl -n "$NS" create secret generic pantai-token \
    --from-literal=token="$value" --dry-run=client -o yaml | kubectl apply -f -
  value=""
  grep -qE "^PANTAI_KEY=" "$ROOT/.env" && value="$(env_get PANTAI_KEY)"
  if [ -n "$value" ]; then
    kubectl -n "$NS" create secret generic pantai-key \
      --from-literal=key="$value" --dry-run=client -o yaml | kubectl apply -f -
  else
    echo "warning: PANTAI_KEY not set in .env, sensor-pantai runs without /var/run/pantai/key"
  fi
}

label_slots() {
  step "mongo slot labels"
  local list nodes i node slot current owner
  list="$(env_get MONGO_SLOT_NODES)"
  read -r -a nodes <<<"$list"
  [ "${#nodes[@]}" -eq 4 ] || die "MONGO_SLOT_NODES must list exactly 4 nodes (slots a b c d)"
  [ "$(printf '%s\n' "${nodes[@]}" | sort -u | wc -l)" -eq 4 ] || die "MONGO_SLOT_NODES lists a node twice"

  for i in 0 1 2 3; do
    node="${nodes[$i]}" slot="${SLOTS[$i]}"
    kubectl get node "$node" >/dev/null 2>&1 || die "node '$node' (slot $slot) does not exist"
    current="$(kubectl get node "$node" -o jsonpath='{.metadata.labels.bnpb/slot}')"
    [ -z "$current" ] || [ "$current" = "$slot" ] ||
      die "node '$node' already holds slot $current, .env wants $slot; relabel by hand if intended"
    owner="$(kubectl get nodes -l "bnpb/slot=$slot" -o jsonpath='{.items[*].metadata.name}')"
    [ -z "$owner" ] || [ "$owner" = "$node" ] ||
      die "slot $slot already belongs to '$owner', .env wants '$node'; relabel by hand if intended"
    [ "$current" = "$slot" ] || kubectl label node "$node" "bnpb/slot=$slot"
  done
  kubectl get nodes -L bnpb/slot
}

apply_manifests() {
  step "apply overlay $MODE"
  # Jobs are immutable, so they are recreated on every deploy; all of them are idempotent.
  kubectl -n "$NS" delete job -l "$PART_OF" --ignore-not-found

  local allow=() kind
  for kind in "${PRUNE_KINDS[@]}"; do allow+=("--prune-allowlist=$kind"); done
  kubectl apply -k "$ROOT/deploy/k8s/overlays/$MODE" --prune -l "$PART_OF" "${allow[@]}"
}

require_tools kubectl
[ -f "$ROOT/.env" ] || die ".env not found; copy .env.example first"
if [ "$MODE" = dev ]; then
  require_tools docker k3d
  load_dev_images
fi
wait_api
apply_secrets
label_slots
apply_manifests

# Dev images keep the tag "dev", so Kubernetes cannot see a rebuild; restart what changed.
for svc in "${CHANGED_SERVICES[@]}"; do
  kubectl -n "$NS" get deploy "$svc" >/dev/null 2>&1 && kubectl -n "$NS" rollout restart deploy "$svc"
done

step "pods"
kubectl -n "$NS" get pods -o wide
