<div align="center">

# **BNPB Hazard Gateway**
### IF4031 - Arsitektur Aplikasi Terdistribusi

<p>
A distributed gateway that collects seismic and volcanic data from BMKG and PVMBG,
converts it into one canonical format, and distributes it to BNPB's downstream clients.
</p>

<p>
  <img alt="Project Status" src="https://img.shields.io/badge/status-milestone_1-blue.svg">
</p>

</div>

---

## Table of Contents

- [About](#about)
  - [Architecture](#architecture)
  - [Technologies](#technologies)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running](#running)
- [Test Scenarios](#test-scenarios)
- [Configuration](#configuration)
  - [Ports](#ports)
  - [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Contributors](#contributors)

---

## About

This project is the Milestone 1 implementation of the IF4031 assignment: a coordination
platform that connects BMKG, PVMBG, and BNPB. The BNPB gateway polls seismic data from
BMKG and volcanic data from PVMBG, converts both into one canonical format
(`HazardEvent`), stores the result, publishes every event to Kafka, and serves it to
downstream clients with different access rights.

BMKG and PVMBG are mock services built to the contract in the assignment spec. BNPB is
made of several independent services, each running in its own container.

### Architecture

```
  bmkg-mock ◄──┐ REST polling
  pvmbg-mock ◄─┤
               aggregator ───── mongodb          (store-net, internal only)
                 │     ▲
         publish │     │ REST /internal/*
                 ▼     │
               kafka   client-api ◄───── clients (Bearer JWT)
                 │          │ JWKS
                 ▼          ▼
   consumer-dashboard   auth-service ◄── clients (login, refresh)
   consumer-notifier
   consumer-regional
```

| Service | Role |
| --- | --- |
| `bmkg-mock` | Simulated BMKG. Seismic events and tsunami warnings, 50–150 ms delay, `X-BMKG-Key` header. |
| `pvmbg-mock` | Simulated PVMBG. Volcanic reports, 0.5–3 s delay, Bearer token, admin endpoints for schema change and outage. |
| `aggregator` | Polls both sources, maps records to `HazardEvent`, upserts them into MongoDB, publishes them to Kafka. Serves an internal API to `client-api`. |
| `mongodb` | Canonical store. It sits on `store-net` with the aggregator only, so no other service can reach it. |
| `auth-service` | Issues RS256 access and refresh tokens to the three clients and publishes its public key as JWKS. |
| `client-api` | Public read API. Verifies the token and returns only the fields allowed for its scope. |
| `kafka` | Broker. Topic `bnpb.hazard-events.v1`, 3 partitions, 24 h retention. |
| `consumer-dashboard`, `consumer-notifier`, `consumer-regional` | Independent subscribers, each with its own consumer group. |
| `kafka-ui` | Optional web UI for inspecting the topic. |

Every service exposes `GET /health`, writes JSON logs to stdout, and logs the latency
of each outbound call. A correlation ID travels in the `X-Correlation-ID` header and in
Kafka message headers, so one request or poll cycle can be followed across services.

### Technologies

* **Go 1.25** for every service
* **Docker Compose** for orchestration
* **Apache Kafka 3.9** (KRaft mode) as the message broker
* **MongoDB 7.0** as the canonical store
* **JWT (RS256)** with a JWKS endpoint for client authentication

## Getting Started

### Prerequisites

* Docker with Compose v2
* A bash shell with `curl` and `openssl` (Linux, macOS, or Git Bash on Windows)
* Go 1.25, only to run the unit tests outside Docker

### Installation

```bash
git clone https://github.com/numshv/tugas-besar-if4031-lalaterdistribusi.git
cd tugas-besar-if4031-lalaterdistribusi
cp .env.example .env
bash scripts/gen-keys.sh
```

- The values in `.env.example` work for a local run. Change the secrets before using
  them anywhere else.
- `gen-keys.sh` creates `infra/keys/jwt_private.pem`, which `auth-service` uses to sign
  tokens. Run it before the first start. If compose was started without the key, Docker
  leaves an empty directory in its place; running the script again cleans that up.

### Running

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
bash scripts/health.sh
```

The first start takes one to two minutes. `health.sh` should report `OK` for all eight
services. `kafka-init` exits once the topic exists; that is expected.

Stopping:

```bash
docker compose --env-file .env -f infra/docker-compose.yml down      # keep data
docker compose --env-file .env -f infra/docker-compose.yml down -v   # also delete MongoDB, Kafka and auth data
```

Unit tests: `go test ./...`

**Windows.** Run the scripts from Git Bash and `export MSYS_NO_PATHCONV=1` first;
otherwise Git Bash rewrites container paths such as `/run/keys/...`. Port 8080 is often
reserved by Windows (`netsh interface ipv4 show excludedportrange protocol=tcp`); if it
is, change `CLIENT_API_HOST_PORT` in `.env`.

## Test Scenarios

Run these from the repository root in bash. The first block loads `.env` and defines
a few helpers used below.

```bash
export MSYS_NO_PATHCONV=1      # Git Bash on Windows only
set -a; . ./.env; set +a
DC="docker compose --env-file .env -f infra/docker-compose.yml"
BMKG=http://localhost:$BMKG_HOST_PORT
PVMBG=http://localhost:$PVMBG_HOST_PORT
AUTH=http://localhost:$AUTH_HOST_PORT
API=http://localhost:$CLIENT_API_HOST_PORT
ADMIN="X-PVMBG-Admin-Token: $PVMBG_ADMIN_TOKEN"

login() { curl -s -X POST "$AUTH/login" -H 'Content-Type: application/json' -d "{\"username\":\"$1\",\"password\":\"$2\"}"; }
field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"; }      # read one string field from JSON
code()  { curl -s -o /dev/null -w '%{http_code}\n' "$@"; }   # print only the HTTP status
lag()   { docker exec bnpb-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1"; }
```

Access tokens last 60 seconds. If a request suddenly returns 401, log in again.

### Problem 1: schema change at runtime

```bash
# PVMBG starts on schema v1, without confidence_level
curl -s -H "Authorization: Bearer $PVMBG_TOKEN" "$PVMBG/volcanic-reports" | grep -c confidence_level   # 0

# Canonical events from both sources
OPS=$(login "$CLIENT_OPS_ID" "$CLIENT_OPS_SECRET" | field access_token)
curl -s -H "Authorization: Bearer $OPS" "$API/hazards?type=SEISMIC"
curl -s -H "Authorization: Bearer $OPS" "$API/hazards?type=VOLCANIC"

# Note when both containers started
docker inspect -f '{{.Name}} {{.State.StartedAt}}' bnpb-pvmbg-mock bnpb-aggregator

# Switch PVMBG to schema v2 while everything is running, then wait for a new report
curl -s -X POST -H "$ADMIN" -d '{"version":2}' "$PVMBG/admin/schema-version"
sleep 15

# New events have confidence_level in attributes, older events do not
OPS=$(login "$CLIENT_OPS_ID" "$CLIENT_OPS_SECRET" | field access_token)
curl -s -H "Authorization: Bearer $OPS" "$API/hazards?type=VOLCANIC" | grep -o '"attributes":{[^}]*}' | head -5

# Start times are unchanged: nothing was restarted
docker inspect -f '{{.Name}} {{.State.StartedAt}}' bnpb-pvmbg-mock bnpb-aggregator
```

Unknown fields follow `UNKNOWN_FIELD_POLICY` (`passthrough` by default).

### Problem 2: slow source, load, and outage

Latency and load are measured with [k6](https://k6.io) using
[`loadtest/loadtest.js`](loadtest/loadtest.js). It runs in Docker on the compose
network, so k6 does not need to be installed. The script has two modes:

| Mode | Virtual users | Duration | Passes when |
| --- | --- | --- | --- |
| `latency` | 10 SEISMIC + 10 VOLCANIC | 60 s | p95 of SEISMIC requests < 300 ms and error rate < 1% |
| `load` | 50 SEISMIC + 20 VOLCANIC (70 parallel) | 60 s | error rate < 1% |

HTTP 429 counts as a controlled rejection, not an error. If any occur, k6 reports them
as `rejected_429`.

To generate repeatable P2 evidence for HOL isolation, sustained load, and PVMBG
outage/recovery, run the verifier from the repository root in Bash:

```bash
bash scripts/verify-p2.sh all
```

The script writes a timestamped report and k6 logs under `evidence/p2/`. It expects
the running PVMBG mock to use a fixed 3000 ms delay, which it verifies before the
latency runs. It does not recreate containers or change the auth token TTL; the k6
runner renews its Field client login during long tests. Run only one scenario with
`bash scripts/verify-p2.sh 1`, `2`, or `3`.

```bash
loadtest() {
  docker run --rm -i --network bnpb-m1_app-net \
    -e SCENARIO="$1" \
    -e CLIENT_API_URL="http://client-api:$CLIENT_API_PORT" \
    -e AUTH_URL="http://auth-service:$AUTH_PORT" \
    -e LT_USER="$CLIENT_FIELD_ID" -e LT_PASS="$CLIENT_FIELD_SECRET" \
    grafana/k6:1.0.0 run - < loadtest/loadtest.js
}

# The test logs in once, so the token must outlive the run
ACCESS_TOKEN_TTL_SECONDS=600 $DC up -d --no-deps auth-service

# BMKG-only latency while PVMBG takes exactly 3 seconds
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 $DC up -d --no-deps pvmbg-mock
sleep 12
$DC logs --since 10s aggregator | grep volcanic-reports       # duration_ms around 3000
loadtest latency | tee loadtest/output-latency.txt            # p(95) of {kind:seismic} under 300 ms

# 70 parallel connections for 60 seconds
loadtest load | tee loadtest/output-load.txt                  # http_req_failed under 1%

# Back to the values in .env
$DC up -d --no-deps pvmbg-mock auth-service
```

Recreating `pvmbg-mock` also resets its schema version and outage flag. Our own runs are
in `loadtest/output-*-3.txt`, with the k6 summary export in `loadtest/result-*-3.json`.

PVMBG outage and recovery:

```bash
FIELD=$(login "$CLIENT_FIELD_ID" "$CLIENT_FIELD_SECRET" | field access_token)
curl -s -X POST -H "$ADMIN" -d '{"enabled":true}' "$PVMBG/admin/outage"
code -H "Authorization: Bearer $PVMBG_TOKEN" "$PVMBG/volcanic-reports"            # 503
sleep 8
code -H "Authorization: Bearer $FIELD" "$API/hazards?type=SEISMIC"                # 200
curl -s -H "Authorization: Bearer $FIELD" "$API/hazards?type=VOLCANIC" | grep -o '"stale_since":"[^"]*"' | head -1

curl -s -X POST -H "$ADMIN" -d '{"enabled":false}' "$PVMBG/admin/outage"
sleep 8
curl -s -H "Authorization: Bearer $FIELD" "$API/hazards?type=VOLCANIC" | grep -c stale_since   # 0
```

### Problem 3: authentication and trust

Run the end-to-end acceptance checks below from the repository root after starting
the services. The verifier uses the live auth-service TTL, waits for natural expiry,
and writes status-only evidence (never tokens or credential values):

```bash
bash scripts/verify-criteria.sh
```

It checks source-specific BMKG/PVMBG credentials, Media field classification and
server-side rejection, Field token expiry/refresh, and the committed-secret audit.
The expiry check takes the configured access-token TTL plus two seconds (60 seconds
by default).

```bash
# Credentials do not work across agencies
code -H "Authorization: Bearer $BMKG_API_KEY" "$PVMBG/volcanic-reports"    # 403
code -H "X-BMKG-Key: $PVMBG_TOKEN" "$BMKG/seismic-events"                  # 403

# Media gets summary fields only, and raw fields are refused
MEDIA=$(login "$CLIENT_MEDIA_ID" "$CLIENT_MEDIA_SECRET" | field access_token)
curl -s -H "Authorization: Bearer $MEDIA" "$API/hazards?type=SEISMIC"                         # no latitude, longitude, attributes
code -H "Authorization: Bearer $MEDIA" "$API/hazards?type=SEISMIC&fields=latitude,longitude"   # 403

# Field team token expires, refresh issues a new one
PAIR=$(login "$CLIENT_FIELD_ID" "$CLIENT_FIELD_SECRET")
OLD_ACCESS=$(echo "$PAIR" | field access_token)
OLD_REFRESH=$(echo "$PAIR" | field refresh_token)
code -H "Authorization: Bearer $OLD_ACCESS" "$API/hazards?type=SEISMIC"    # 200
sleep 65
code -H "Authorization: Bearer $OLD_ACCESS" "$API/hazards?type=SEISMIC"    # 401, expired
PAIR=$(curl -s -X POST "$AUTH/refresh" -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$OLD_REFRESH\"}")
NEW_ACCESS=$(echo "$PAIR" | field access_token)
code -H "Authorization: Bearer $NEW_ACCESS" "$API/hazards?type=SEISMIC"    # 200
code -H "Authorization: Bearer $OLD_ACCESS" "$API/hazards?type=SEISMIC"    # 401
code -X POST "$AUTH/refresh" -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$OLD_REFRESH\"}"   # 401, single use

# No credentials in git
git ls-files | grep -E '(^|/)\.env$|\.pem$'       # no output
git check-ignore .env infra/keys/jwt_private.pem   # both listed
```

### Problem 4: independent services and flexible storage

```bash
# Stop, rebuild and restart one service; nothing else restarts
TOKEN=$(login "$CLIENT_FIELD_ID" "$CLIENT_FIELD_SECRET" | field access_token)
docker ps --filter name=bnpb- --format '{{.Names}}' | sort | xargs docker inspect -f '{{.Name}} {{.State.StartedAt}}' > /tmp/before.txt
$DC stop auth-service
code -H "Authorization: Bearer $TOKEN" "$API/hazards?type=SEISMIC"    # 200, existing tokens still work
$DC build auth-service
$DC up -d --no-deps auth-service
docker ps --filter name=bnpb- --format '{{.Names}}' | sort | xargs docker inspect -f '{{.Name}} {{.State.StartedAt}}' > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt                                    # only auth-service differs

# Records from before and after the schema change sit side by side (run Problem 1 first)
docker exec bnpb-mongodb mongosh -u "$MONGO_ROOT_USERNAME" -p "$MONGO_ROOT_PASSWORD" \
  --authenticationDatabase admin "$MONGO_DB" --quiet --eval '
  db.hazard_events.find({hazard_type: "VOLCANIC"}, {_id: 0, source_ref_id: 1, attributes: 1})
    .sort({occurred_at: -1}).limit(6).forEach(printjson)'

# Only the aggregator can reach MongoDB
$DC exec client-api wget -qO- --timeout=3 http://mongodb:27017/   # bad address
$DC exec aggregator wget -qO- --timeout=3 http://mongodb:27017/   # "trying to access MongoDB over HTTP"
```

### Problem 5: fan-out through Kafka

```bash
# Two consumers in separate groups read the same topic
lag bnpb.dashboard
lag bnpb.notifier
curl -s "http://localhost:$CONSUMER_DASHBOARD_HOST_PORT/dashboard"
$DC logs --tail 5 consumer-notifier

# Stop one consumer: the producer and the other consumer carry on
docker stop bnpb-consumer-notifier
sleep 30
lag bnpb.notifier                                    # LAG grows
curl -s "http://localhost:$CONSUMER_DASHBOARD_HOST_PORT/dashboard" | grep -o '"total":[0-9]*'   # still growing

# Start it again: it resumes from its committed offset and catches up
docker start bnpb-consumer-notifier
sleep 10
lag bnpb.notifier                                    # LAG back to 0
$DC logs --since 15s consumer-notifier | grep notification_sent
```

<!-- TODO: add a new consumer to the existing topic without changing the producer -->

### Following one correlation ID

```bash
ID=$($DC logs consumer-notifier | grep notification_sent | tail -1 | grep -o '"correlation_id":"[^"]*"' | cut -d'"' -f4)
$DC logs bmkg-mock pvmbg-mock aggregator consumer-dashboard consumer-notifier | grep "$ID"
```

The same ID appears in the source mock, the aggregator's outbound calls, and both consumers.

## Configuration

### Ports

`*_PORT` is the port a service listens on inside its container. `*_HOST_PORT` is where
it appears on your machine. To fix a port clash, change only `*_HOST_PORT`.

| Service | Container port | Host port (variable) |
| --- | --- | --- |
| client-api | 8080 | 8080 (`CLIENT_API_HOST_PORT`) |
| bmkg-mock | 8081 | 8081 (`BMKG_HOST_PORT`) |
| pvmbg-mock | 8082 | 8082 (`PVMBG_HOST_PORT`) |
| aggregator | 8083 | not published |
| auth-service | 8084 | 8084 (`AUTH_HOST_PORT`) |
| consumer-dashboard | 8085 | 8085 (`CONSUMER_DASHBOARD_HOST_PORT`) |
| consumer-notifier | 8086 | 8086 (`CONSUMER_NOTIFIER_HOST_PORT`) |
| consumer-regional | 8087 | 8087 (`CONSUMER_REGIONAL_HOST_PORT`) |
| kafka-ui | 8080 | 8090 (`KAFKA_UI_PORT`) |
| kafka | 9092 | not published |
| mongodb | 27017 | not published |

### Environment Variables

All variables live in `.env`, with example values in [`.env.example`](.env.example).
`.env` and `infra/keys/` are git-ignored.

**General**

| Variable | Default | Description |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

**BMKG mock**

| Variable | Default | Description |
| --- | --- | --- |
| `BMKG_PORT` | `8081` | Listen port |
| `BMKG_API_KEY` | | Key expected in `X-BMKG-Key` |
| `BMKG_DELAY_MIN_MS`, `BMKG_DELAY_MAX_MS` | `50`, `150` | Response delay range |
| `BMKG_SEED_COUNT` | `20` | Historical records created at startup (minimum 20) |
| `BMKG_EVENT_INTERVAL_SECONDS` | `10` | How often a new event appears |

**PVMBG mock**

| Variable | Default | Description |
| --- | --- | --- |
| `PVMBG_PORT` | `8082` | Listen port |
| `PVMBG_TOKEN` | | Bearer token for data endpoints. Must start with `pvmbg_v1.` |
| `PVMBG_ADMIN_TOKEN` | | Token for `/admin/*`, sent in `X-PVMBG-Admin-Token`. Must differ from `PVMBG_TOKEN` |
| `PVMBG_DELAY_MIN_MS`, `PVMBG_DELAY_MAX_MS` | `500`, `3000` | Response delay range |
| `PVMBG_SEED_COUNT` | `20` | Historical records created at startup (minimum 20) |
| `PVMBG_EVENT_INTERVAL_SECONDS` | `10` | How often a new report appears |
| `PVMBG_OUTAGE_MODE` | `error` | Behaviour during an outage: `error` answers 503 at once, `timeout` holds the request until the caller gives up |
| `PVMBG_SCHEMA_VERSION` | `1` | Schema at startup: `1` without `confidence_level`, `2` with it |

**Aggregator**

| Variable | Default | Description |
| --- | --- | --- |
| `AGGREGATOR_PORT` | `8083` | Listen port of the internal API |
| `POLL_INTERVAL_SECONDS` | `3` | Polling interval for each source |
| `BMKG_BASE_URL`, `PVMBG_BASE_URL` | | Source addresses inside the Docker network |
| `HTTP_CLIENT_TIMEOUT_MS` | `5000` | Timeout for calls to the sources |
| `AGG_MAX_CONCURRENCY` | `64` | Concurrent requests allowed on the internal API; the rest get 429 |
| `UNKNOWN_FIELD_POLICY` | `passthrough` | Source fields with no canonical counterpart: `passthrough` copies them into `attributes`, `drop` discards them |

The aggregator also reads `BMKG_API_KEY`, `PVMBG_TOKEN`, the `MONGO_*` values and the
`KAFKA_*` values.

**Auth service**

| Variable | Default | Description |
| --- | --- | --- |
| `AUTH_PORT` | `8084` | Listen port |
| `ACCESS_TOKEN_TTL_SECONDS` | `60` | Access token lifetime |
| `REFRESH_TOKEN_TTL_SECONDS` | `3600` | Refresh token lifetime |
| `JWT_ISSUER`, `JWT_AUDIENCE` | `bnpb-auth`, `client-api` | `iss` and `aud` claims, checked by `client-api` |
| `JWT_PRIVATE_KEY_PATH` | `/run/keys/jwt_private.pem` | Signing key path inside the container |
| `CLIENT_MEDIA_ID`, `CLIENT_MEDIA_SECRET` | | Media / press credentials |
| `CLIENT_FIELD_ID`, `CLIENT_FIELD_SECRET` | | Field team (BASARNAS / Pemda) credentials |
| `CLIENT_OPS_ID`, `CLIENT_OPS_SECRET` | | BNPB internal ops credentials |

**Client API**

| Variable | Default | Description |
| --- | --- | --- |
| `CLIENT_API_PORT` | `8080` | Listen port |
| `AGGREGATOR_BASE_URL` | | Aggregator address inside the Docker network |
| `AUTH_JWKS_URL` | | Where the public key is fetched from |
| `MAX_CONCURRENT_REQUESTS` | `200` | Concurrent requests allowed; the rest get 429 |

**Kafka and consumers**

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `kafka:9092` | Broker address |
| `KAFKA_TOPIC` | `bnpb.hazard-events.v1` | Topic for `HazardEvent` |
| `KAFKA_TOPIC_PARTITIONS`, `KAFKA_TOPIC_RETENTION_MS` | `3`, `86400000` | Used when the topic is created |
| `KAFKA_CLUSTER_ID` | | KRaft cluster ID |
| `KAFKA_GROUP_DASHBOARD`, `KAFKA_GROUP_NOTIFIER`, `KAFKA_GROUP_REGIONAL` | | Consumer group of each consumer |
| `CONSUMER_DASHBOARD_PORT`, `CONSUMER_NOTIFIER_PORT`, `CONSUMER_REGIONAL_PORT` | `8085`, `8086`, `8087` | Listen ports |
| `NOTIFIER_SEND_DELAY_MS` | `0` | Artificial delay per notification, to simulate a slow consumer |

**MongoDB**

| Variable | Default | Description |
| --- | --- | --- |
| `MONGO_ROOT_USERNAME`, `MONGO_ROOT_PASSWORD` | | Admin user, created on the first start |
| `MONGO_APP_USERNAME`, `MONGO_APP_PASSWORD` | | User the aggregator connects with (`readWrite` on `MONGO_DB` only) |
| `MONGO_DB` | `bnpb` | Database name |

## API Reference

### Source mocks

| Service | Endpoint | Credential |
| --- | --- | --- |
| BMKG | `GET /seismic-events?since=` | `X-BMKG-Key: <key>` |
| BMKG | `GET /tsunami-warnings?since=` | `X-BMKG-Key: <key>` |
| PVMBG | `GET /volcanic-reports?since=` | `Authorization: Bearer pvmbg_v1.<secret>` |
| PVMBG | `POST /admin/schema-version` with `{"version": 1 \| 2}` | `X-PVMBG-Admin-Token: <token>` |
| PVMBG | `POST /admin/outage` with `{"enabled": true \| false}` | `X-PVMBG-Admin-Token: <token>` |
| both | `GET /health` | none |

- `since` is an RFC 3339 timestamp and is inclusive. Without it, all records are returned.
- Tsunami warnings are filtered by the time of their related seismic event, since the
  contract gives warnings no timestamp of their own.
- A missing credential gets 401. A wrong one, or one belonging to the other agency, gets 403.
- Mock data lives in memory. Restarting a mock resets its schema version and outage
  flag to the `.env` values and generates records with new IDs.

### Auth service

| Endpoint | Body | Returns |
| --- | --- | --- |
| `POST /login` | `{"username": "...", "password": "..."}` | `access_token`, `refresh_token` |
| `POST /refresh` | `{"refresh_token": "..."}` | A new pair. The old refresh token is revoked. |
| `GET /.well-known/jwks.json` | | Public key for verifying tokens |

### Client API

`GET /hazards?type=SEISMIC|VOLCANIC[&since=<RFC 3339>][&fields=<a,b>]` with
`Authorization: Bearer <access_token>`. Results are sorted newest first.

| Client | Credentials | Scope | Fields returned |
| --- | --- | --- | --- |
| Media / press | `CLIENT_MEDIA_*` | `media` | Summary only: `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at` |
| Field team | `CLIENT_FIELD_*` | `field` | All fields |
| BNPB internal ops | `CLIENT_OPS_*` | `internal` | All fields |

- A `media` token that asks for a raw field (`source_ref_id`, `latitude`, `longitude`,
  `attributes`) through `fields` is refused with 403. For other scopes `fields` has no effect.
- While PVMBG cannot be reached, volcanic events carry `stale_since`: the last time the
  aggregator polled PVMBG successfully.

### Consumers

- `GET /dashboard` on `consumer-dashboard`: totals per severity and hazard type, and the
  latest 100 events.
- `consumer-notifier` logs a `notification_sent` line for every `SIAGA` or `AWAS` event.

## Project Structure

Each directory under `services/` has its own `Dockerfile`, a `main.go`, and an
`internal/` package that no other service imports.

<pre>
tugas-besar-if4031-lalaterdistribusi/
├── infra/
│   ├── broker/
│   │   └── create-topics.sh      # Creates the Kafka topic on startup
│   ├── keys/                     # JWT signing key (generated, git-ignored)
│   ├── store/
│   │   └── mongo-init.js         # App user, indexes, volcano reference table
│   └── docker-compose.yml        # Starts the whole system
│
├── pkg/
│   ├── correlation/              # Correlation ID helpers
│   ├── hazard/                   # HazardEvent type and hazard_id generator
│   ├── httpx/                    # HTTP server and client with request and latency logging
│   ├── kafkax/                   # Kafka consumer loop and duplicate filter
│   └── logging/                  # JSON logger
│
├── services/
│   ├── bmkg-mock/                # Simulated BMKG
│   ├── pvmbg-mock/               # Simulated PVMBG
│   ├── aggregator/               # Polling, mapping, canonical store, publishing
│   ├── auth-service/             # Login, refresh, JWKS
│   ├── client-api/               # Public API with scope-based field filtering
│   ├── consumer-dashboard/       # Kafka consumer: dashboard summary
│   ├── consumer-notifier/        # Kafka consumer: SIAGA and AWAS notifications
│   └── consumer-regional/        # Kafka consumer: regional feed
│
├── scripts/
│   ├── gen-keys.sh               # Generates the JWT signing key
│   └── health.sh                 # Checks /health of every service
│
├── loadtest/                     # Load test configuration and results
├── docs/                         # Report material
├── .env.example                  # Every configuration variable with an example value
├── go.mod
└── README.md
</pre>

## Contributors

<div align="center">

| Name | NIM | Responsibilities |
| :--- | :---: | :--- |
| Noumisyifa Nabila Nareswari | 13523058 | Aggregator: polling, schema mapping, canonical store, Kafka publisher |
| Mayla Yaffa Ludmilla | 13523050 | Auth service, client API, Kafka consumers, load testing |
| Muhammad Edo Raduputu Aprima | 13523096 | BMKG and PVMBG mocks, infrastructure (Compose, Kafka, MongoDB), shared packages |

</div>
