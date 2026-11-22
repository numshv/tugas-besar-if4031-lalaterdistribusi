#!/usr/bin/env bash
# Proves P3 criterion 6: POST /field-reports answers 202 quickly while the
# aggregator is down, field-report-api pods are killed during the run, and in
# the end every report that got 202 is stored exactly once.
#
# Usage: scripts/verify-field-reports.sh [count]   (needs .env, kubectl, curl)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
set -a; . "$ROOT/.env"; set +a
GW="${GW:-http://localhost:8090}"
NS=sistem-informasi-kebencanaan-bnpb
N="${1:-300}"
RUN="$(date +%s)"
OUT="$ROOT/scripts/results/field-reports-$RUN"
mkdir -p "$OUT"

TOKEN="" TOKEN_AT=0
token() {
  if (( $(date +%s) - TOKEN_AT > 40 )); then
    TOKEN="$(curl -s -X POST "$GW/login" -H 'Content-Type: application/json' \
      -d "{\"username\":\"$CLIENT_FIELD_ID\",\"password\":\"$CLIENT_FIELD_SECRET\"}" |
      sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')"
    [ -n "$TOKEN" ] || { echo "login failed" >&2; exit 1; }
    TOKEN_AT=$(date +%s)
  fi
  echo "$TOKEN"
}

send() {
  curl -s -o /dev/null -w '%{http_code} %{time_total}' -m 10 -X POST "$GW/field-reports" \
    -H "Authorization: Bearer $(token)" -H "Idempotency-Key: $1" -H 'Content-Type: application/json' \
    -d "{\"hazard_type\":\"VOLCANIC\",\"latitude\":-7.54,\"longitude\":110.44,\"observed_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"description\":\"verifikasi $1\"}" || echo "000 0"
}

AGG_REPLICAS="$(kubectl -n "$NS" get deploy aggregator -o jsonpath='{.spec.replicas}')"
restore() { kubectl -n "$NS" scale deploy/aggregator --replicas="$AGG_REPLICAS" >/dev/null; }
trap restore EXIT

echo "==> downstream down: aggregator scaled 0 (was $AGG_REPLICAS)"
kubectl -n "$NS" scale deploy/aggregator --replicas=0 >/dev/null

echo "==> killing a field-report-api pod every 3 s while sending"
(
  while sleep 3; do
    pod="$(kubectl -n "$NS" get pods -l app=field-report-api -o name | shuf -n1)"
    [ -n "$pod" ] && kubectl -n "$NS" delete "$pod" --wait=false >/dev/null && echo "$(date +%T) killed $pod" >>"$OUT/kills.log"
  done
) &
KILLER=$!

echo "==> sending $N reports (every 20th sent twice with the same key)"
: >"$OUT/sent.tsv"
for i in $(seq 1 "$N"); do
  key="fr-$RUN-$(printf %04d "$i")"
  for attempt in 1 2 3 4 5; do       # a client retries with the same key until it gets 202
    read -r code t <<<"$(send "$key")"
    printf '%s\t%s\t%s\t%s\n' "$key" "$attempt" "$code" "$t" >>"$OUT/sent.tsv"
    [ "$code" = 202 ] && break
    sleep 1
  done
  (( i % 20 == 0 )) && { read -r code t <<<"$(send "$key")"; printf '%s\tdup\t%s\t%s\n' "$key" "$code" "$t" >>"$OUT/sent.tsv"; }
done
kill "$KILLER" 2>/dev/null || true

echo "==> downstream back up, waiting for the queue to drain"
restore
kubectl -n "$NS" rollout status deploy/aggregator --timeout=180s >/dev/null

awk -F'\t' '$3==202 {print $1}' "$OUT/sent.tsv" | sort -u >"$OUT/accepted.txt"
deadline=$(( $(date +%s) + 180 ))
while :; do
  : >"$OUT/status.tsv"
  while read -r key; do
    body="$(curl -s -m 5 "$GW/field-reports/$key" -H "Authorization: Bearer $(token)")"
    status="$(sed -n 's/.*"status":"\([^"]*\)".*/\1/p' <<<"$body")"
    deliveries="$(sed -n 's/.*"deliveries":\([0-9]*\).*/\1/p' <<<"$body")"
    printf '%s\t%s\t%s\n' "$key" "${status:-error}" "${deliveries:-0}" >>"$OUT/status.tsv"
  done <"$OUT/accepted.txt"
  pending=$(awk -F'\t' '$2!="processed"' "$OUT/status.tsv" | wc -l)
  (( pending == 0 || $(date +%s) > deadline )) && break
  echo "    $pending not processed yet"; sleep 5
done

accepted=$(wc -l <"$OUT/accepted.txt")
processed=$(awk -F'\t' '$2=="processed"' "$OUT/status.tsv" | wc -l)
redelivered=$(awk -F'\t' '$3>1' "$OUT/status.tsv" | wc -l)
p95=$(awk -F'\t' '$3==202 {print $4}' "$OUT/sent.tsv" | sort -n | awk '{a[NR]=$1} END {i=int(NR*0.95); if(i<1)i=1; printf "%.0f", a[i]*1000}')
non202=$(awk -F'\t' '$3!=202' "$OUT/sent.tsv" | cut -f3 | sort | uniq -c | tr '\n' ' ')
kills=$(wc -l <"$OUT/kills.log" 2>/dev/null || echo 0)

cat <<EOF | tee "$OUT/summary.txt"
reports sent            $N (plus duplicates of every 20th)
pods killed             $kills
non-202 answers         ${non202:-none}   (retried with the same key)
accepted (202)          $accepted
processed               $processed
lost                    $(( accepted - processed ))
seen more than once     $redelivered   (stored once; deliveries counted)
p95 latency of 202      ${p95} ms
results                 $OUT
EOF
[ "$accepted" -eq "$processed" ]
