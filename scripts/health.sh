#!/bin/sh
set -u

check() {
  name=$1
  port=$2
  body=$(curl -fsS --max-time 5 "http://localhost:$port/health" 2>/dev/null) \
    && echo "OK    $name ($port) $body" \
    || echo "FAIL  $name ($port)"
}

check client-api         "${CLIENT_API_HOST_PORT:-8080}"
check bmkg-mock          "${BMKG_HOST_PORT:-8081}"
check pvmbg-mock         "${PVMBG_HOST_PORT:-8082}"
check aggregator         "${AGGREGATOR_HOST_PORT:-8083}"
check auth-service       "${AUTH_HOST_PORT:-8084}"
check consumer-dashboard "${CONSUMER_DASHBOARD_HOST_PORT:-8085}"
check consumer-notifier  "${CONSUMER_NOTIFIER_HOST_PORT:-8086}"
check consumer-regional  "${CONSUMER_REGIONAL_HOST_PORT:-8087}"
