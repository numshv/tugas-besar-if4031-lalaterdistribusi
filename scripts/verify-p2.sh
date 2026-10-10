#!/usr/bin/env bash
# Run from the repository root with Git Bash, Linux Bash, or macOS Bash.
set -u

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "Run this script from inside the git repository."
  exit 1
}
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and configure the services."
  exit 1
fi
set -a
. <(sed 's/\r$//' "$ENV_FILE")
set +a

API="http://localhost:${CLIENT_API_HOST_PORT:-8080}"
AUTH="http://localhost:${AUTH_HOST_PORT:-8084}"
PVMBG="http://localhost:${PVMBG_HOST_PORT:-8082}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:1.0.0}"
K6_NETWORK="${K6_NETWORK:-bnpb-m1_app-net}"
BODY="$(mktemp)"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT="evidence/p2/verify-p2-${RUN_ID}.txt"
K6_DIR="evidence/p2/k6-${RUN_ID}"
mkdir -p evidence/p2 "$K6_DIR"
trap 'rm -f "$BODY"' EXIT
exec > >(tee "$OUT") 2>&1

PASS=0
FAIL=0
FIELD_TOKEN=""
OUTAGE_TOUCHED=0

record() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    PASS=$((PASS + 1))
    printf 'PASS | %s | expected=%s actual=%s\n' "$label" "$expected" "$actual"
  else
    FAIL=$((FAIL + 1))
    printf 'FAIL | %s | expected=%s actual=%s\n' "$label" "$expected" "$actual"
  fi
}

info() {
  printf 'INFO | %s | %s\n' "$1" "$2"
}

request() {
  curl -sS --connect-timeout 3 --max-time 10 -o "$BODY" -w '%{http_code}' "$@"
}

json_value() {
  sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"
}

number_is() {
  awk -v a="$1" -v b="$2" -v op="$3" 'BEGIN {
    if (a == "" || a == "NA" || b == "") exit 1
    if (op == "<") r = (a + 0 < b + 0)
    else if (op == "<=") r = (a + 0 <= b + 0)
    else if (op == ">=") r = (a + 0 >= b + 0)
    else if (op == ">") r = (a + 0 > b + 0)
    else exit 2
    exit !r
  }' && echo yes || echo no
}

container_value() {
  docker inspect --format "$2" "$1" 2>/dev/null
}

outage() {
  local enabled="$1"
  curl -sS --connect-timeout 3 --max-time 8 -o "$BODY" -w '%{http_code}' \
    -X POST "$PVMBG/admin/outage" \
    -H "X-PVMBG-Admin-Token: ${PVMBG_ADMIN_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d "{\"enabled\":${enabled}}"
}

restore_outage() {
  if [ "$OUTAGE_TOUCHED" = 1 ]; then
    curl -sS --connect-timeout 3 --max-time 8 -o /dev/null \
      -X POST "$PVMBG/admin/outage" \
      -H "X-PVMBG-Admin-Token: ${PVMBG_ADMIN_TOKEN}" \
      -H 'Content-Type: application/json' \
      -d '{"enabled":false}' || true
  fi
}
trap 'restore_outage; rm -f "$BODY"' EXIT
trap 'restore_outage; exit 130' INT
trap 'restore_outage; exit 143' TERM

login_field() {
  local code
  code="$(curl -sS --connect-timeout 3 --max-time 8 -o "$BODY" -w '%{http_code}' \
    -X POST "$AUTH/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${CLIENT_FIELD_ID}\",\"password\":\"${CLIENT_FIELD_SECRET}\"}")" || code=000
  record "Field client login for probe token" 200 "$code"
  FIELD_TOKEN="$(json_value access_token < "$BODY")"
  if [ -z "$FIELD_TOKEN" ]; then
    record "Field access token present" yes no
    return 1
  fi
  record "Field access token present" yes yes
}

probe_api() {
  local hazard_type="$1"
  local result
  result="$(curl -sS --connect-timeout 3 --max-time 8 -o "$BODY" -w '%{http_code} %{time_total}' \
    -H "Authorization: Bearer ${FIELD_TOKEN}" \
    "${API}/hazards?type=${hazard_type}&limit=5")" || result="000 0"
  PROBE_CODE="${result%% *}"
  PROBE_SECONDS="${result#* }"
  PROBE_BODY="$(cat "$BODY")"
  PROBE_STALE_COUNT="$(grep -o '"stale_since"' "$BODY" | wc -l | tr -d ' ')"
}

run_k6() {
  local mode="$1" seismic_vus="$2" volcanic_vus="$3" duration="$4" k6_log="$K6_DIR/${1}.txt"
  local result
  info "k6 scenario" "$mode (${seismic_vus} seismic VUs + ${volcanic_vus} volcanic VUs, ${duration})"
  MSYS_NO_PATHCONV=1 docker run --rm -i \
    --network "$K6_NETWORK" \
    -e "SCENARIO=${mode}" \
    -e "DURATION=${duration}" \
    -e "SEISMIC_VUS=${seismic_vus}" \
    -e "VOLCANIC_VUS=${volcanic_vus}" \
    -e "CLIENT_API_URL=http://client-api:8080" \
    -e "AUTH_URL=http://auth-service:8084" \
    -e "LT_USER=${CLIENT_FIELD_ID}" \
    -e "LT_PASS=${CLIENT_FIELD_SECRET}" \
    "$K6_IMAGE" run - < scripts/p2-loadtest.js | tee "$k6_log"
  result="${PIPESTATUS[0]}"
  return "$result"
}

k6_metric() {
  local log="$1" metric="$2"
  sed -n "s/^P2METRIC ${metric}=//p" "$log" | tail -n 1
}

run_latency_isolation() {
  local pvmbg_min pvmbg_max source_code latency
  printf '\n[1] HOL isolation: 3-second PVMBG delay and BMKG-only latency\n'
  pvmbg_min="$(container_value bnpb-pvmbg-mock '{{range .Config.Env}}{{println .}}{{end}}' \
    | sed -n 's/^PVMBG_DELAY_MIN_MS=//p' | head -n 1)"
  pvmbg_max="$(container_value bnpb-pvmbg-mock '{{range .Config.Env}}{{println .}}{{end}}' \
    | sed -n 's/^PVMBG_DELAY_MAX_MS=//p' | head -n 1)"
  record "Live PVMBG minimum delay is 3000 ms" 3000 "${pvmbg_min:-missing}"
  record "Live PVMBG maximum delay is 3000 ms" 3000 "${pvmbg_max:-missing}"

  for i in 1 2 3; do
    source_code="$(curl -sS --connect-timeout 3 --max-time 8 -o /dev/null \
      -w '%{http_code}' -H "Authorization: Bearer ${PVMBG_TOKEN}" \
      "$PVMBG/volcanic-reports")" || source_code=000
    latency="$(curl -sS --connect-timeout 3 --max-time 8 -o /dev/null \
      -w '%{time_total}' -H "Authorization: Bearer ${PVMBG_TOKEN}" \
      "$PVMBG/volcanic-reports")" || latency=0
    record "Direct PVMBG request #${i} succeeds" 200 "$source_code"
    record "Direct PVMBG request #${i} takes at least 2.9 seconds" yes \
      "$(number_is "$latency" 2.9 '>=' )"
    info "Direct PVMBG request #${i} time_total" "${latency}s"
  done

  local baseline_log concurrent_log k6_status baseline_p95 concurrent_p95
  baseline_log="$K6_DIR/iso-a.txt"
  concurrent_log="$K6_DIR/iso-b.txt"

  if run_k6 iso-a 10 0 60s; then
    record "Seismic-only baseline k6 completed" yes yes
    baseline_p95="$(k6_metric "$baseline_log" seismic_p95_ms)"
    record "Seismic-only baseline p95 is below 300 ms" yes \
      "$(number_is "$baseline_p95" 300 '<')"
    record "Seismic-only baseline unexpected status rate below 1%" yes \
      "$(number_is "$(k6_metric "$baseline_log" unexpected_rate)" 0.01 '<')"
    info "Seismic-only p95 latency" "${baseline_p95}ms"
  else
    record "Seismic-only baseline k6 completed" yes no
    baseline_p95=NA
  fi

  if run_k6 iso-b 10 10 60s; then
    record "Concurrent seismic + volcanic k6 completed" yes yes
    concurrent_p95="$(k6_metric "$concurrent_log" seismic_p95_ms)"
    record "Concurrent seismic p95 is below 300 ms" yes \
      "$(number_is "$concurrent_p95" 300 '<')"
    record "Concurrent unexpected status rate below 1%" yes \
      "$(number_is "$(k6_metric "$concurrent_log" unexpected_rate)" 0.01 '<')"
    info "Concurrent seismic p95 latency" "${concurrent_p95}ms"
    info "Concurrent seismic maximum latency" "$(k6_metric "$concurrent_log" seismic_max_ms)ms"
    if [ "$baseline_p95" != NA ]; then
      info "Concurrent p95 minus seismic-only p95" \
        "$(awk -v a="$baseline_p95" -v b="$concurrent_p95" 'BEGIN { printf "%.2f", b-a }')ms"
    fi
  else
    record "Concurrent seismic + volcanic k6 completed" yes no
  fi
}

run_sustained_load() {
  local log="$K6_DIR/load.txt" run_started restart_agg restart_api run_code panic_count
  printf '\n[2] Sustained load: 50 seismic + 20 volcanic clients for 60 seconds\n'
  run_started="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  restart_agg="$(container_value bnpb-aggregator '{{.RestartCount}}')"
  restart_api="$(container_value bnpb-client-api '{{.RestartCount}}')"

  if run_k6 load 50 20 60s; then
    run_code=0
  else
    run_code=$?
  fi
  record "Sustained k6 run completed" 0 "$run_code"
  record "Sustained load duration is at least 60 seconds" yes \
    "$(number_is "$(k6_metric "$log" test_run_duration_ms)" 60000 '>=')"
  record "At least 50 virtual users were active" yes \
    "$(number_is "$(k6_metric "$log" vus_max)" 50 '>=')"
  record "Unexpected response rate is below 1% (429 excluded)" yes \
    "$(number_is "$(k6_metric "$log" unexpected_rate)" 0.01 '<')"
  info "API requests" "$(k6_metric "$log" api_requests)"
  info "API p95 latency" "$(k6_metric "$log" api_p95_ms)ms"
  info "Controlled 429 rejections (excluded from error rate)" "$(k6_metric "$log" rejected_429)"

  record "Aggregator still running" true "$(container_value bnpb-aggregator '{{.State.Running}}')"
  record "Client API still running" true "$(container_value bnpb-client-api '{{.State.Running}}')"
  record "Aggregator did not restart during load" "$restart_agg" \
    "$(container_value bnpb-aggregator '{{.RestartCount}}')"
  record "Client API did not restart during load" "$restart_api" \
    "$(container_value bnpb-client-api '{{.RestartCount}}')"

  panic_count=0
  for container in bnpb-aggregator bnpb-client-api; do
    local container_panics
    container_panics="$(docker logs --since "$run_started" "$container" 2>&1 | grep -ci panic || true)"
    panic_count=$((panic_count + container_panics))
  done
  record "No panic in aggregator/client API logs during load" 0 "$panic_count"
}

run_outage_recovery() {
  local code direct_down t0 detected=-1 elapsed i outage_result direct_up
  local start_agg start_api end_agg end_api source_errors source_errors_before
  printf '\n[3] PVMBG outage, graceful stale responses, and recovery\n'
  login_field || return
  start_agg="$(container_value bnpb-aggregator '{{.State.StartedAt}}')"
  start_api="$(container_value bnpb-client-api '{{.State.StartedAt}}')"

  outage_result="$(outage false)" || outage_result=000
  code="${outage_result%% *}"
  record "Reset PVMBG outage to off before test" 200 "$code"
  sleep 7
  probe_api VOLCANIC
  record "Healthy baseline volcanic API status" 200 "$PROBE_CODE"
  record "Healthy baseline has no stale_since" 0 "$PROBE_STALE_COUNT"

  OUTAGE_TOUCHED=1
  source_errors_before="$(docker logs bnpb-aggregator 2>&1 \
    | grep -ci 'pvmbg poll failed' || true)"
  t0="$(date +%s)"
  outage_result="$(outage true)" || outage_result=000
  code="${outage_result%% *}"
  record "PVMBG outage enabled" 200 "$code"
  if [ "$code" = 200 ]; then
    grep -Fq '"outage":true' "$BODY"
    record "PVMBG mock reports outage enabled" yes "$([ "$?" -eq 0 ] && echo yes || echo no)"
  fi

  direct_down="$(curl -sS --connect-timeout 3 --max-time 6 -o /dev/null \
    -w '%{http_code}' -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    "$PVMBG/volcanic-reports")" || direct_down=000
  record "Direct PVMBG request fails with 503 during outage" 503 "$direct_down"

  for i in $(seq 1 22); do
    sleep 2
    probe_api VOLCANIC
    if [ "$PROBE_CODE" = 200 ] && [ "$PROBE_STALE_COUNT" -gt 0 ]; then
      detected="$(date +%s)"
      break
    fi
  done
  if [ "$detected" -ge 0 ]; then
    elapsed=$((detected - t0))
    record "Volcanic response becomes stale during outage" yes yes
    record "Outage detection takes at most 30 seconds" yes "$(number_is "$elapsed" 30 '<=')"
    info "Outage-to-stale detection time" "${elapsed}s"
  else
    record "Volcanic response becomes stale during outage" yes no
    record "Outage detection takes at most 30 seconds" yes no
  fi

  for i in 1 2 3; do
    probe_api SEISMIC
    record "Seismic remains available during outage (sample ${i})" 200 "$PROBE_CODE"
    record "Seismic does not hang (sample ${i}, <1s)" yes \
      "$(number_is "$PROBE_SECONDS" 1 '<')"
    probe_api VOLCANIC
    record "Volcanic request remains HTTP 200 during outage (sample ${i})" 200 "$PROBE_CODE"
    record "Volcanic response marks events stale (sample ${i})" yes \
      "$([ "$PROBE_STALE_COUNT" -gt 0 ] && echo yes || echo no)"
    record "Volcanic request does not hang (sample ${i}, <1s)" yes \
      "$(number_is "$PROBE_SECONDS" 1 '<')"
  done

  source_errors="$(docker logs bnpb-aggregator 2>&1 \
    | grep -ci 'pvmbg poll failed' || true)"
  source_errors=$((source_errors - source_errors_before))
  if [ "$source_errors" -lt 0 ]; then source_errors=0; fi
  record "Aggregator logged PVMBG poll failures" yes \
    "$([ "$source_errors" -gt 0 ] && echo yes || echo no)"

  t0="$(date +%s)"
  outage_result="$(outage false)" || outage_result=000
  code="${outage_result%% *}"
  record "PVMBG outage disabled" 200 "$code"
  OUTAGE_TOUCHED=0

  direct_up="$(curl -sS --connect-timeout 3 --max-time 8 -o /dev/null \
    -w '%{http_code}' -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    "$PVMBG/volcanic-reports")" || direct_up=000
  record "Direct PVMBG request succeeds after recovery" 200 "$direct_up"

  detected=-1
  for i in $(seq 1 30); do
    sleep 2
    probe_api VOLCANIC
    if [ "$PROBE_CODE" = 200 ] && [ "$PROBE_STALE_COUNT" -eq 0 ]; then
      detected="$(date +%s)"
      break
    fi
  done
  if [ "$detected" -ge 0 ]; then
    elapsed=$((detected - t0))
    record "Stale marker clears after recovery" yes yes
    record "Recovery takes at most 30 seconds" yes "$(number_is "$elapsed" 30 '<=')"
    info "Outage-off to recovery time" "${elapsed}s"
  else
    record "Stale marker clears after recovery" yes no
    record "Recovery takes at most 30 seconds" yes no
  fi

  probe_api SEISMIC
  record "Seismic still available after recovery" 200 "$PROBE_CODE"
  end_agg="$(container_value bnpb-aggregator '{{.State.StartedAt}}')"
  end_api="$(container_value bnpb-client-api '{{.State.StartedAt}}')"
  record "Aggregator was not restarted for outage recovery" "$start_agg" "$end_agg"
  record "Client API was not restarted for outage recovery" "$start_api" "$end_api"
}

printf 'P2 verification started: %s\n' "$(date -Is)"
printf 'Evidence file: %s\n' "$OUT"
printf 'Services are tested in place; this verifier does not recreate containers or change token TTL.\n'

for container in bnpb-pvmbg-mock bnpb-aggregator bnpb-client-api bnpb-auth-service; do
  state="$(container_value "$container" '{{.State.Status}}')"
  record "${container} is running" running "$state"
done

scenario="${1:-all}"
case "$scenario" in
  all|1|2|3) ;;
  *)
    echo "Usage: bash scripts/verify-p2.sh [all|1|2|3]"
    exit 2
    ;;
esac

if [ "$scenario" = all ] || [ "$scenario" = 1 ]; then
  run_latency_isolation
fi
if [ "$scenario" = all ] || [ "$scenario" = 2 ]; then
  run_sustained_load
fi
if [ "$scenario" = all ] || [ "$scenario" = 3 ]; then
  run_outage_recovery
fi

printf '\n================ SUMMARY: PASS=%s FAIL=%s ================\n' "$PASS" "$FAIL"
printf 'Evidence file: %s\n' "$OUT"
printf 'k6 logs: %s\n' "$K6_DIR"
[ "$FAIL" -eq 0 ]
