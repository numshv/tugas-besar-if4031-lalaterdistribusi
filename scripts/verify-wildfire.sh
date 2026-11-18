#!/usr/bin/env bash
# Evidence for P4 criteria 4, 5 and 8 on the cluster.
#   1. wait until wildfire-processor has rebuilt from feed offset 0
#   2. kill it KILLS times while it is working (criterion 5); every new pod
#      reconnects and rebuilds (criterion 8)
#   3. stop it for BACKLOG seconds so the feed runs ahead, start it again and
#      time how long it takes to be current (criterion 4)
#   4. compare the fires the processor holds with the HazardEvents stored by
#      the aggregator: same IDs, same detections, nothing missing or doubled
#
# Usage: scripts/verify-wildfire.sh   (needs .env with AUDIT_TOKENS, kubectl, curl, python)
#   KILLS=3 BACKLOG=300 PROM_URL=http://localhost:9090 scripts/verify-wildfire.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
set -a; . "$ROOT/.env"; set +a
GW="${GW:-http://localhost:8090}"
NS=sistem-informasi-kebencanaan-bnpb
KILLS="${KILLS:-3}"
BACKLOG="${BACKLOG:-300}"
PY="${PY:-python}"
AUDIT="${AUDIT_TOKENS%%,*}"
OUT="$ROOT/scripts/results/wildfire-$(date +%s)"
mkdir -p "$OUT"

stats() { curl -sf -m 5 -H "Authorization: Bearer $AUDIT" "$GW/internal/wildfire/stats" || echo '{}'; }
field() { "$PY" -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"; }

wait_caught_up() { # label -> seconds until caught_up and lag 0
  local start=$(date +%s) s
  while :; do
    s="$(stats)"
    if [ "$(field caught_up <<<"$s")" = True ] && [ "$(field lag_events <<<"$s")" = 0 ]; then break; fi
    (( $(date +%s) - start > 600 )) && { echo "timeout waiting for $1" >&2; exit 1; }
    sleep 2
  done
  echo "$s" >"$OUT/stats-$1.json"
  echo $(( $(date +%s) - start ))
}

pod() { kubectl -n "$NS" get pods -l app=wildfire-processor -o name | head -1; }

echo "==> waiting for the first rebuild"
echo "    current after $(wait_caught_up start) s"

echo "==> criterion 5/8: killing the processor $KILLS times while it works"
for i in $(seq 1 "$KILLS"); do
  sleep $(( 10 + RANDOM % 30 ))
  p="$(pod)"
  echo "    $(date +%T) kill $p (offset $(stats | field offset))"
  kubectl -n "$NS" delete "$p" --wait=false >/dev/null
  sleep 3
  echo "    current again after $(wait_caught_up "kill-$i") s, rebuild $(field rebuild_seconds <"$OUT/stats-kill-$i.json") s"
done

echo "==> criterion 4: processor stopped for $BACKLOG s while the feed runs ahead"
kubectl -n "$NS" scale deploy/wildfire-processor --replicas=0 >/dev/null
sleep "$BACKLOG"
kubectl -n "$NS" scale deploy/wildfire-processor --replicas=1 >/dev/null
CATCHUP=$(wait_caught_up backlog)
echo "    current again after $CATCHUP s (rebuild from 0 included)"

echo "==> comparing processor fires with stored HazardEvents"
sleep 5
curl -sf -H "Authorization: Bearer $AUDIT" "$GW/internal/wildfire/fires" >"$OUT/processor-fires.json"
: >"$OUT/stored.jsonl"
cursor=""
while :; do
  page="$(curl -sf -G -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/events" \
    --data-urlencode source=SATELIT --data-urlencode limit=1000 ${cursor:+--data-urlencode "cursor=$cursor"})"
  echo "$page" >>"$OUT/stored.jsonl"
  cursor="$(field next_cursor <<<"$page")"
  [ -z "$cursor" ] || [ "$cursor" = None ] && break
done
"$PY" - "$OUT" <<'EOF' | tee "$OUT/compare.txt"
import json, sys, collections
out = sys.argv[1]
proc = json.load(open(f"{out}/processor-fires.json"))
want = {f["source_ref_id"]: tuple(f["attributes"]["detection_keys"]) for f in proc["items"]}
stored = []
for line in open(f"{out}/stored.jsonl"):
    stored += json.loads(line)["items"]
ids = collections.Counter(e["source_ref_id"] for e in stored)
got = {e["source_ref_id"]: tuple(e["attributes"]["detection_keys"]) for e in stored}
missing = sorted(set(want) - set(got))
extra = sorted(set(got) - set(want))
differ = sorted(i for i in set(want) & set(got) if want[i] != got[i])
doubled = sorted(i for i, n in ids.items() if n > 1)
print(f"processor fires   {len(want)}  digest {proc['digest'][:16]}")
print(f"stored events     {len(stored)}")
print(f"missing           {len(missing)} {missing[:5]}")
print(f"extra (stale)     {len(extra)} {extra[:5]}")
print(f"different content {len(differ)} {differ[:5]}")
print(f"doubled           {len(doubled)} {doubled[:5]}")
sys.exit(1 if missing or extra or differ or doubled else 0)
EOF

if [ -n "${PROM_URL:-}" ]; then
  q='histogram_quantile(0.95, sum(rate(hazard_store_latency_seconds_bucket{source="SATELIT"}[10m])) by (le))'
  p95="$(curl -sf -G "$PROM_URL/api/v1/query" --data-urlencode "query=$q" | "$PY" -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "no data")')"
  echo "p95 feed arrival -> stored: $p95 s" | tee -a "$OUT/compare.txt"
else
  echo "PROM_URL not set: read p95 from the aggregator's hazard_store_latency_seconds{source=\"SATELIT\"}"
fi
echo "results in $OUT"
