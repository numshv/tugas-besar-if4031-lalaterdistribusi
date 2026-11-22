#!/usr/bin/env bash
# Runs next to p2-sustained.js (start both together): scales the aggregator 3 -> 5 -> 2 and
# rolls out a new revision while load is running. Disable the aggregator HPA first.
# Usage: loadtest/m2/p2-chaos.sh [ramp-seconds]
set -euo pipefail

NS=sistem-informasi-kebencanaan-bnpb
RAMP="${1:-60}"
at() { echo "[$(date +%T)] $*"; }

kubectl -n "$NS" delete hpa aggregator --ignore-not-found
kubectl -n "$NS" scale deploy/aggregator --replicas=3
sleep "$RAMP"
at "scale 3 -> 5"
kubectl -n "$NS" scale deploy/aggregator --replicas=5
sleep 40
at "scale 5 -> 2"
kubectl -n "$NS" scale deploy/aggregator --replicas=2
sleep 30
at "rolling update (new revision)"
kubectl -n "$NS" rollout restart deploy/aggregator
kubectl -n "$NS" rollout status deploy/aggregator --timeout=180s
at "back to 3 replicas"
kubectl -n "$NS" scale deploy/aggregator --replicas=3
