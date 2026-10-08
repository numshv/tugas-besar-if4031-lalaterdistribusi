# IF4031 Milestone 1


## Status

Foundation only. Every service boots and answers `/health`; business logic is not
implemented yet.

| Component | State |
| --- | --- |
| `pkg/` — logging, correlation, httpx, canonical contract | done |
| Kafka (KRaft) + Kafka UI, topic created on startup | done |
| MongoDB + init script + volcano reference table | done |
| Compose orchestration, health checks, network isolation | done |
| bmkg-mock, pvmbg-mock | done |
| aggregator, canonical store access, consumers | stub |
| auth-service, client-api | stub |

## Prerequisites

Docker Engine with Compose v2, Go 1.25 (local development only), `curl`, `openssl`.

## Running from scratch

```bash
cp .env.example .env     # replace every "change-me" value
./scripts/gen-keys.sh    # RS256 key pair for auth-service
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
./scripts/health.sh      # every service must report OK
```

Stopping:

```bash
docker compose --env-file .env -f infra/docker-compose.yml down      # keeps data
docker compose --env-file .env -f infra/docker-compose.yml down -v   # drops data
```

The compose file lives in `infra/`, so `--env-file .env -f infra/docker-compose.yml`
is required for the root `.env` to remain the single source of configuration.

## Ports

Container ports and host ports are separate. `*_PORT` is what the process listens
on inside the container and what `*_BASE_URL` points at; `*_HOST_PORT` only maps
that onto your machine. Resolve host port clashes by changing `*_HOST_PORT` alone.

| Service | Container port | Host variable |
| --- | --- | --- |
| client-api | 8080 | `CLIENT_API_HOST_PORT` |
| bmkg-mock | 8081 | `BMKG_HOST_PORT` |
| pvmbg-mock | 8082 | `PVMBG_HOST_PORT` |
| aggregator | 8083 | not published |
| auth-service | 8084 | `AUTH_HOST_PORT` |
| consumer-dashboard | 8085 | `CONSUMER_DASHBOARD_HOST_PORT` |
| consumer-notifier | 8086 | `CONSUMER_NOTIFIER_HOST_PORT` |
| consumer-regional | 8087 | `CONSUMER_REGIONAL_HOST_PORT` |
| kafka-ui | 8080 | `KAFKA_UI_PORT` |
| kafka | 9092 | not published |
| mongodb | 27017 | not published |

Host defaults match the container port. On Windows, port 8080 is often reserved by
WinNAT; check with `netsh interface ipv4 show excludedportrange protocol=tcp` and
change `CLIENT_API_HOST_PORT` if it is.

## Configuration

Every variable with an example value lives in [`.env.example`](.env.example).
Credentials, JWT signing keys, and per-environment settings are read only from
`.env` and `infra/keys/`; neither is committed.

`PVMBG_TOKEN` must start with `pvmbg_v1.` — the mock refuses to start otherwise,
so a misconfigured credential surfaces at boot rather than during a demo.

## Source mocks

| Agency | Endpoint | Credential |
| --- | --- | --- |
| BMKG | `GET /seismic-events?since=` | `X-BMKG-Key: <key>` |
| BMKG | `GET /tsunami-warnings?since=` | `X-BMKG-Key: <key>` |
| PVMBG | `GET /volcanic-reports?since=` | `Authorization: Bearer pvmbg_v1.<secret>` |
| PVMBG | `POST /admin/schema-version` | `X-PVMBG-Admin-Token: <token>` |
| PVMBG | `POST /admin/outage` | `X-PVMBG-Admin-Token: <token>` |
| both | `GET /health` | none |

- `since` is an RFC 3339 timestamp and is **inclusive** (`>= since`). Omitting it
  returns everything; an unparseable value returns 400. Boundary records are
  re-delivered on every poll, which is safe because `hazard_id` is deterministic.
- `/tsunami-warnings` filters on the **related event's** `occurred_at`. The
  contract gives `TsunamiWarning` no timestamp of its own, and `estimated_arrival`
  describes when a wave arrives, not when the record appeared.
- Credentials are agency-scoped. A BMKG key presented to PVMBG fails the token
  structure check; a PVMBG token presented to BMKG fails the key comparison.
- The PVMBG admin plane uses its own header and keeps working during an outage —
  it is the only way to end one. Outage is operator-controlled; there is no timer.
- PVMBG `/health` reports its current `outage` and `schema_version`.
- Mock state is in memory. Restarting a mock resets generated records, schema
  version, and outage back to the values in `.env`.

## Test scenarios

```bash
set -a; . ./.env; set +a
B="http://localhost:$BMKG_HOST_PORT"; K="X-BMKG-Key: $BMKG_API_KEY"
P="http://localhost:$PVMBG_HOST_PORT"
T="Authorization: Bearer $PVMBG_TOKEN"
A="X-PVMBG-Admin-Token: $PVMBG_ADMIN_TOKEN"
C="docker compose --env-file .env -f infra/docker-compose.yml"
```

Problem 1 — additive schema change while the system is running:

```bash
curl -s -H "$T" "$P/volcanic-reports"      # no confidence_level
$C ps pvmbg-mock                            # note the uptime
curl -s -X POST -H "$A" -d '{"version":2}' "$P/admin/schema-version"
curl -s -H "$T" "$P/volcanic-reports"      # confidence_level present
$C ps pvmbg-mock                            # uptime unchanged, no restart
```

Problem 2 — PVMBG outage and recovery:

```bash
curl -s -X POST -H "$A" -d '{"enabled":true}' "$P/admin/outage"
curl -s -w ' -> %{http_code} in %{time_total}s\n' -H "$T" "$P/volcanic-reports"
curl -s -o /dev/null -w 'BMKG still serving: %{http_code}\n' -H "$K" "$B/seismic-events"
curl -s "$P/health"
curl -s -X POST -H "$A" -d '{"enabled":false}' "$P/admin/outage"
curl -s -o /dev/null -w 'recovered: %{http_code}\n' -H "$T" "$P/volcanic-reports"
```

Problem 3 — credentials do not cross agency boundaries:

```bash
curl -s -o /dev/null -w 'BMKG key at PVMBG: %{http_code}\n' -H "Authorization: Bearer $BMKG_API_KEY" "$P/volcanic-reports"
curl -s -o /dev/null -w 'PVMBG token at BMKG: %{http_code}\n' -H "X-BMKG-Key: $PVMBG_TOKEN" "$B/seismic-events"
```

On PowerShell use `curl.exe` rather than `curl`, `NUL` rather than `/dev/null`,
and escape JSON bodies as `'{\"version\":2}'` — PowerShell 5.1 strips unescaped
quotes before they reach the executable.

## Canonical store isolation

Two Docker networks are used: `app-net` carries service traffic, while `store-net`
is `internal: true` and holds only `mongodb` and `aggregator`. MongoDB is not
published to the host, so no other service can reach the canonical store — not
even by name:

```bash
docker compose --env-file .env -f infra/docker-compose.yml \
  exec client-api wget -qO- --timeout=3 http://mongodb:27017/   # fails to resolve
docker compose --env-file .env -f infra/docker-compose.yml \
  exec aggregator wget -qO- --timeout=3 http://mongodb:27017/   # connects
```

## Layout

```
pkg/        shared across services: logging, correlation, httpx, canonical contract
services/   one directory per service, each with its own Dockerfile
infra/      compose file, broker config, canonical store init, JWT keys
scripts/    operational helpers
loadtest/   load testing configuration and results
docs/       report material
```

## Conventions

- One `go.mod` at the repository root.
- A service must not import another service's `internal/`; only `pkg/` is shared.
- Canonical contract: [`pkg/hazard/event.go`](pkg/hazard/event.go). `hazard_id` is
  deterministic (`HZ-<SOURCE>-<source_ref_id>`), so repeated polling upserts
  idempotently in both the canonical store and the consumers.
- Source fields without a canonical counterpart are passed through to `attributes`
  verbatim, including fields that appear mid-run such as `confidence_level`.
- Kafka topic `bnpb.hazard-events.v1`, partition key `hazard_id`, JSON value,
  at-least-once delivery. Each consumer uses its own `group.id`.
- Every service logs JSON to stdout, carries a correlation ID through
  `X-Correlation-ID` (generated when absent), and records the latency of each
  outbound call.
