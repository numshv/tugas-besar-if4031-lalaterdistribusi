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
| bmkg-mock, pvmbg-mock | stub |
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
| aggregator | 8083 | `AGGREGATOR_HOST_PORT` |
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
