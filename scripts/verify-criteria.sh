#!/usr/bin/env bash
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
. "$ENV_FILE"
set +a

API="http://localhost:${CLIENT_API_HOST_PORT:-8080}"
AUTH="http://localhost:${AUTH_HOST_PORT:-8084}"
BMKG="http://localhost:${BMKG_HOST_PORT:-8081}"
PVMBG="http://localhost:${PVMBG_HOST_PORT:-8082}"
BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT

mkdir -p evidence
OUT="evidence/criteria-verify-$(date +%Y%m%d-%H%M%S).txt"
exec > >(tee "$OUT") 2>&1

PASS=0
FAIL=0

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

request() {
  curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' "$@"
}

json_value() {
  sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"
}

printf 'Criteria verification started: %s\n' "$(date -Is)"

printf '\n[1] BMKG/PVMBG credentials are source-specific\n'
bmkg_code="$(request -H "X-BMKG-Key: ${BMKG_API_KEY}" "$BMKG/seismic-events")"
record "BMKG credential accepted by BMKG" 200 "$bmkg_code"
pvmbg_code="$(request -H "Authorization: Bearer ${PVMBG_TOKEN}" "$PVMBG/volcanic-reports")"
record "PVMBG credential accepted by PVMBG" 200 "$pvmbg_code"
cross_to_pvmbg="$(request -H "Authorization: Bearer ${BMKG_API_KEY}" "$PVMBG/volcanic-reports")"
record "BMKG credential rejected by PVMBG" 403 "$cross_to_pvmbg"
cross_to_bmkg="$(request -H "X-BMKG-Key: ${PVMBG_TOKEN}" "$BMKG/seismic-events")"
record "PVMBG credential rejected by BMKG" 403 "$cross_to_bmkg"

printf '\n[2] Media scope returns Ringkasan only and server rejects raw fields\n'
media_response="$(curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' \
  -X POST "$AUTH/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${CLIENT_MEDIA_ID}\",\"password\":\"${CLIENT_MEDIA_SECRET}\"}")"
record "Media client login" 200 "$media_response"
MEDIA_TOKEN="$(json_value access_token < "$BODY")"
if [ -z "$MEDIA_TOKEN" ]; then
  record "Media access token issued" non-empty empty
else
  record "Media access token issued" non-empty non-empty
  media_code="$(request -H "Authorization: Bearer ${MEDIA_TOKEN}" "$API/hazards?type=SEISMIC")"
  record "Media summary request" 200 "$media_code"
  if [ "$media_code" = 200 ]; then
    summary_ok=1
    for field in hazard_id source hazard_type severity area_name occurred_at ingested_at; do
      grep -Fq "\"$field\"" "$BODY" || summary_ok=0
    done
    record "Media response contains all summary fields" 1 "$summary_ok"
    raw_found=0
    for field in source_ref_id latitude longitude attributes; do
      grep -Fq "\"$field\"" "$BODY" && raw_found=1
    done
    record "Media response contains no raw fields" 0 "$raw_found"
    raw_code="$(request -H "Authorization: Bearer ${MEDIA_TOKEN}" \
      "$API/hazards?type=SEISMIC&fields=latitude,longitude,source_ref_id,attributes")"
    record "Media explicit raw-field request rejected by server" 403 "$raw_code"
  else
    record "Media response contains all summary fields" 1 unavailable
    record "Media response contains no raw fields" 0 unavailable
    record "Media explicit raw-field request rejected by server" 403 unavailable
  fi
fi

field_response="$(curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' \
  -X POST "$AUTH/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${CLIENT_FIELD_ID}\",\"password\":\"${CLIENT_FIELD_SECRET}\"}")"
record "Field client login for raw-field control" 200 "$field_response"
FIELD_TOKEN="$(json_value access_token < "$BODY")"
if [ -z "$FIELD_TOKEN" ]; then
  record "Field client may request raw fields (control)" 200 unavailable
else
  field_raw_code="$(request -H "Authorization: Bearer ${FIELD_TOKEN}" \
    "$API/hazards?type=SEISMIC&fields=latitude")"
  record "Field client may request raw fields (control)" 200 "$field_raw_code"
fi

printf '\n[3] Field access token expires naturally and refresh rotates credentials\n'
live_ttl="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' bnpb-auth-service 2>/dev/null \
  | sed -n 's/^ACCESS_TOKEN_TTL_SECONDS=//p' | head -n 1)"
if ! [[ "$live_ttl" =~ ^[1-9][0-9]*$ ]]; then
  record "Read live auth-service token TTL" positive-integer unavailable
  live_ttl="${ACCESS_TOKEN_TTL_SECONDS:-60}"
fi
printf 'Live access token TTL: %s seconds\n' "$live_ttl"
field_login="$(curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' \
  -X POST "$AUTH/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${CLIENT_FIELD_ID}\",\"password\":\"${CLIENT_FIELD_SECRET}\"}")"
record "Field client login before expiry wait" 200 "$field_login"
OLD_ACCESS="$(json_value access_token < "$BODY")"
OLD_REFRESH="$(json_value refresh_token < "$BODY")"
if [ -z "$OLD_ACCESS" ] || [ -z "$OLD_REFRESH" ]; then
  record "Initial access and refresh tokens issued" both-present missing
else
  record "Initial access and refresh tokens issued" both-present both-present
  before_expiry="$(request -H "Authorization: Bearer ${OLD_ACCESS}" "$API/hazards?type=SEISMIC&limit=1")"
  record "Initial field access token works" 200 "$before_expiry"
  printf 'Waiting %s seconds for natural access-token expiry...\n' "$((live_ttl + 2))"
  sleep "$((live_ttl + 2))"
  expired_code="$(request -H "Authorization: Bearer ${OLD_ACCESS}" "$API/hazards?type=SEISMIC&limit=1")"
  record "Old access token rejected after natural expiry" 401 "$expired_code"

  refresh_code="$(curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' \
    -X POST "$AUTH/refresh" -H 'Content-Type: application/json' \
    -d "{\"refresh_token\":\"${OLD_REFRESH}\"}")"
  record "Refresh succeeds without another login" 200 "$refresh_code"
  NEW_ACCESS="$(json_value access_token < "$BODY")"
  NEW_REFRESH="$(json_value refresh_token < "$BODY")"
  if [ -z "$NEW_ACCESS" ] || [ -z "$NEW_REFRESH" ]; then
    record "Refresh returns rotated access and refresh tokens" both-present missing
    record "New access token works" 200 unavailable
    record "Old access token remains rejected after refresh" 401 unavailable
    record "Rotated-out refresh token rejected" 401 unavailable
  else
    record "Refresh returns rotated access and refresh tokens" both-present both-present
    new_code="$(request -H "Authorization: Bearer ${NEW_ACCESS}" "$API/hazards?type=SEISMIC&limit=1")"
    record "New access token works" 200 "$new_code"
    old_after_refresh="$(request -H "Authorization: Bearer ${OLD_ACCESS}" "$API/hazards?type=SEISMIC&limit=1")"
    record "Old access token remains rejected after refresh" 401 "$old_after_refresh"
    rotated_refresh_code="$(curl -sS --max-time 8 -o "$BODY" -w '%{http_code}' \
      -X POST "$AUTH/refresh" -H 'Content-Type: application/json' \
      -d "{\"refresh_token\":\"${OLD_REFRESH}\"}")"
    record "Rotated-out refresh token rejected" 401 "$rotated_refresh_code"
  fi
fi

printf '\n[4] No runtime credentials are committed or hardcoded; example config exists\n'
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  record "Runtime .env is not tracked" yes no
else
  record "Runtime .env is not tracked" yes yes
fi
if git ls-files --error-unmatch infra/keys/jwt_private.pem >/dev/null 2>&1; then
  record "JWT private key is not tracked" yes no
else
  record "JWT private key is not tracked" yes yes
fi
if git check-ignore -q .env; then
  record "Runtime .env is git-ignored" yes yes
else
  record "Runtime .env is git-ignored" yes no
fi
if git check-ignore -q infra/keys/jwt_private.pem; then
  record "JWT private key is git-ignored" yes yes
else
  record "JWT private key is git-ignored" yes no
fi
if [ -f .env.example ] && git ls-files --error-unmatch .env.example >/dev/null 2>&1; then
  record ".env.example exists and is tracked" yes yes
else
  record ".env.example exists and is tracked" yes no
fi

secrets=(BMKG_API_KEY PVMBG_TOKEN PVMBG_ADMIN_TOKEN CLIENT_MEDIA_SECRET CLIENT_FIELD_SECRET CLIENT_OPS_SECRET MONGO_ROOT_PASSWORD MONGO_APP_PASSWORD)
audit_failed=0
for name in "${secrets[@]}"; do
  value="${!name:-}"
  if [ -z "$value" ]; then
    printf 'FAIL | %s is configured for the audit\n' "$name"
    audit_failed=1
    continue
  fi
  if git grep -F -l -- "$value" -- '*.go' 'infra/docker-compose.yml' >/dev/null 2>&1; then
    printf 'FAIL | %s value appears in tracked source/config\n' "$name"
    audit_failed=1
  fi
  if git log --all --format=%H -S"$value" -- . ':!.env.example' | grep -q .; then
    printf 'FAIL | %s value appears in committed history outside .env.example\n' "$name"
    audit_failed=1
  fi
done
record "Configured secret values absent from source and committed history" 0 "$audit_failed"

placeholder_failed=0
for name in "${secrets[@]}"; do
  if ! grep -Eq "^${name}=.*(change-me|example|placeholder|replace)" .env.example; then
    printf 'FAIL | %s does not have an example placeholder\n' "$name"
    placeholder_failed=1
  fi
done
record ".env.example secret values are placeholders" 0 "$placeholder_failed"

printf '\n================ SUMMARY: PASS=%s FAIL=%s ================\n' "$PASS" "$FAIL"
printf 'Evidence file: %s\n' "$OUT"
[ "$FAIL" -eq 0 ]
