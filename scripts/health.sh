#!/bin/sh
set -u

report() {
  name=$1
  where=$2
  body=$3
  if [ -n "$body" ]; then
    echo "OK    $name ($where) $body"
  else
    echo "FAIL  $name ($where)"
  fi
}

check() {
  report "$1" "$2" "$(curl -fsS --max-time 5 "http://localhost:$2/health" 2>/dev/null)"
}

check_internal() {
  report "$1" "internal $2" "$(docker exec "bnpb-$1" wget -qO- "http://127.0.0.1:$2/health" 2>/dev/null)"
}

check client-api              "${CLIENT_API_HOST_PORT:-8080}"
check bmkg-mock               "${BMKG_HOST_PORT:-8081}"
check pvmbg-mock              "${PVMBG_HOST_PORT:-8082}"
check_internal aggregator     "${AGGREGATOR_PORT:-8083}"
check auth-service            "${AUTH_HOST_PORT:-8084}"
check consumer-dashboard      "${CONSUMER_DASHBOARD_HOST_PORT:-8085}"
check consumer-notifier       "${CONSUMER_NOTIFIER_HOST_PORT:-8086}"
check consumer-regional       "${CONSUMER_REGIONAL_HOST_PORT:-8087}"
