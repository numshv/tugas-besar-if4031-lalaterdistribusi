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
otherwise Git Bash rewrites container paths such as `/run/keys/...`. The two
`verify-*.sh` scripts are the exception: they need the variable unset, which is why
they are started with `env -u MSYS_NO_PATHCONV` below. Port 8080 is often
reserved by Windows (`netsh interface ipv4 show excludedportrange protocol=tcp`); if it
is, change `CLIENT_API_HOST_PORT` in `.env`.

### Kubernetes (Milestone 2)

Everything runs in the namespace `sistem-informasi-kebencanaan-bnpb`. Manifests are plain
Kustomize: `deploy/k8s/base/<service>/` per service, `overlays/dev` (k3d, local images) and
`overlays/demo` (VM cluster, images from GHCR, HPA on).

**Topology declaration** (node-b and node-c: fill in before the demo):

| Node | Role | Mongo slot | VM or native | Physical host | Host CPU / RAM |
|---|---|---|---|---|---|
| node-a | control plane + worker (never shut down by scenarios) | a | VM: QEMU + WHPX, Ubuntu 24.04, 4 vCPU / 6 GB | Laptop 1: Lenovo 21SY, Windows 11 Pro | Intel Core Ultra 5 225U (12 cores / 14 threads) / 15 GB |
| node-b | worker | b | VM | _laptop_ | _cpu / ram_ |
| node-c | worker | c | VM | _laptop_ | _cpu / ram_ |
| node-d | worker | d | VM: QEMU + WHPX, Ubuntu 24.04, 2 vCPU / 3.5 GB | Laptop 1 (same host as node-a) | Intel Core Ultra 5 225U (12 cores / 14 threads) / 15 GB |

Scenarios that shut a node down target node-b, node-c or node-d only. node-a and node-d share
one physical host: the scenarios stop one VM at a time, but losing that whole laptop would take
two copies of shard-2 (a, c, d) and shard-3 (a, b, d) at once. Nodes are joined over Tailscale
(`NODE_IP=<tailscale ip>`, `FLANNEL_IFACE=tailscale0`); node-a and node-d also have a direct
QEMU link (192.168.100.0/24) that Tailscale uses between them.

**Cluster.** Demo nodes are VMs with their own kernel (k3d shares the host kernel and is for
development only).

```bash
# every node: same k3s version, pinned through the installer's own variable
export INSTALL_K3S_VERSION=v1.37.2+k3s1
# node-a
sudo -E NODE_NAME=node-a bash infra/k3s/install-server.sh          # prints the join token
# node-b, node-c, node-d
sudo -E NODE_NAME=node-b SERVER_IP=<node-a ip> TOKEN=<token> bash infra/k3s/install-agent.sh
# nodes on different laptops over Tailscale: add NODE_IP=<tailscale ip> FLANNEL_IFACE=tailscale0
```

With VirtualBox, turn off host time sync so clock-skew scenarios are not undone:
`VBoxManage setextradata <vm> "VBoxInternal/Devices/VMMDev/0/Config/GetHostTimeDisabled" 1`.
QEMU guests have no host time sync. In every VM, sync the clock once at boot and then leave NTP
off (`systemd-timesyncd` disabled, a oneshot unit runs `timedatectl set-ntp true`, waits for
`NTPSynchronized=yes`, then `set-ntp false`), so a skew stays until the next reboot.

Development cluster: `k3d cluster create bnpb --agents 3 -p "8090:80@loadbalancer"`.

**Deploy (one command).** `scripts/deploy.sh dev` builds the images and loads them into k3d;
`scripts/deploy.sh demo` pulls them from GHCR (built by `.github/workflows/images.yml` on every
push to `dev` and `main`; the packages must be public, or add an `imagePullSecret`). Each build
is tagged with its commit SHA and its branch; only `main` moves `latest`, which `overlays/demo`
uses. Before the exam, pin the demo to one tested commit so that pods rescheduled during a node
kill pull the same code:

```bash
SHA=$(git rev-parse origin/main)
sed -i "s/newTag: latest/newTag: $SHA/" deploy/k8s/overlays/demo/kustomization.yaml
```

Both create the Secrets from `.env`, label the Mongo slots from `MONGO_SLOT_NODES`, apply the
overlay and prune objects that were removed from the manifests. Then install the Tester Agent:

```bash
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
```

**Entry point.** Traefik on port 80 of any node (k3d: `http://localhost:8090`):

| Path | Service |
|---|---|
| `/login`, `/refresh`, `/.well-known/` | auth-service |
| `/hazards` (and every other path) | client-api |
| `/field-reports` | field-report-api |
| `/internal/audit/`, `/internal/ops/`, `/analytics` | aggregator |
| `/internal/wildfire/` | wildfire-processor |
| `/grafana/` (dashboards "BNPB Overview", "BNPB Reliability (P2 & P5)" and "BNPB Storage (P1)"), `/jaeger/`, `/prometheus/` | observability |

**Configuration added in Milestone 2** (`.env` unless noted):

| Variable | Where | Meaning |
|---|---|---|
| `MONGO_SLOT_NODES` | `.env` | Node for Mongo slots a b c d, in that order |
| `PANTAI_AUTH_TOKEN` | `.env` → Secret `pantai-token` | Shared token of Sensor Pantai and coastal-ingester |
| `AUDIT_TOKENS`, `SATELLITE_TOKEN`, `GRAFANA_ADMIN_PASSWORD` | `.env` (optional) | Audit API tokens, Feed Satelit token, Grafana admin password |
| `LEASE_DURATION_SECONDS` / `LEASE_RENEW_DEADLINE_SECONDS` / `LEASE_RETRY_PERIOD_SECONDS` | aggregator env | Leader lease timing (15 / 10 / 2) |
| `SENSOR_REQUEST_TIMEOUT_MS`, `SENSOR_MAX_RETRIES`, `SENSOR_BREAKER_FAILURES`, `SENSOR_BREAKER_OPEN_MS`, `SENSOR_MAX_CONCURRENCY` | `deploy/k8s/base/coastal-ingester` | Timeout 2000, 2 retries with jitter, breaker opens after 5 failures for 10 s, bulkhead 2 |
| `SENSOR_STUCK_AFTER_MS`, `SENSOR_STALE_AFTER_SECONDS` | same | Sensor data counts as stale when `generated_at` stops for 15 s, or a caught-up page is more than 900 s (sensor time) behind |
| `CLIENT_API_MEDIA_MAX_CONCURRENCY`, `COASTAL_STALE_AFTER_SECONDS` | `deploy/k8s/base/client-api` | Media may hold 40 concurrent requests; coastal readings older than 30 s are returned with `stale: true` |

**Triggering the P2 and P5 scenarios.**

```bash
NS=sistem-informasi-kebencanaan-bnpb
kubectl -n $NS get leases                                   # who polls BMKG, PVMBG and Sensor Pantai, and the epoch
kubectl -n $NS delete pod <holder of poller-bmkg>           # P2.3 failover: a new holder within ~20 s, epoch + 1
# P2.5 freeze: on the VM host, VBoxManage controlvm <vm> pause; wait 30 s; VBoxManage controlvm <vm> resume
#   QEMU node (node-d): send "stop" then "cont" to its monitor (tcp 127.0.0.1:4444 on laptop 1)
#   the woken pod logs "lease no longer ours" and sends nothing; Grafana shows the epoch change
# P2.4 clock skew and node kills: Tester Agent, e.g. kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>
k6 run loadtest/m2/p2-sustained.js & loadtest/m2/p2-chaos.sh   # P2.6 + P2.8 (10,000 VUs, scale 3-5-2, rollout)
k6 run -e VUS=400 loadtest/m2/p2-throughput.js              # P2.7, once with 1 and once with 3 aggregator replicas
kubectl -n $NS scale deploy/sensor-pantai --replicas=0      # P5.4: breaker opens, client-api answers stale at once
kubectl -n $NS scale deploy/sensor-pantai --replicas=1      #        recovers without intervention
k6 run loadtest/m2/p5-shedding.js                           # P5.5: Media gets 429, Tim Lapangan stays at 200
kubectl -n $NS exec deploy/field-receiver -- wget -qO- http://127.0.0.1:8089/alerts   # P5.1 alerts and latency
```

k6 needs the client credentials as `-e MEDIA_ID=.. -e MEDIA_SECRET=..` (and `FIELD_`, `INTERNAL_`)
plus `-e BASE_URL=http://<node>`; run it from a machine outside the cluster.

### MongoDB sharded cluster (Kubernetes)

HazardEvents live in a MongoDB sharded cluster: 4 shards × 3 copies spread over 4 node
slots, so no node holds all data and every shard survives the loss of one node.

| | slot a | slot b | slot c | slot d |
|---|---|---|---|---|
| shard-0 | primary | replica | replica | – |
| shard-1 | – | primary | replica | replica |
| shard-2 | replica | – | primary | replica |
| shard-3 | replica | replica | – | primary |
| config server (`cfg`) | ✓ | ✓ | ✓ | – |
| mongos | ✓ | ✓ | ✓ | ✓ |

The aggregator still connects to `mongodb:27017`, which now points to the mongos on each
node. NetworkPolicies only let the aggregator (and the `mongo-init` Job) reach mongos and
the shard members. The `dev` overlay keeps the same 4 slots but runs one member per shard
and one config server, with less memory, so it fits a laptop k3d cluster.

```bash
# Once per cluster: give every node a slot (dev: k3d cluster create bnpb-dev --agents 3)
scripts/label-nodes.sh <node-a> <node-b> <node-c> <node-d>

# Once per cluster: secrets (MONGO_ROOT_PASSWORD, MONGO_APP_PASSWORD, MONGO_MONITOR_PASSWORD live in bnpb-secrets)
scripts/gen-mongo-keyfile.sh
kubectl -n sistem-informasi-kebencanaan-bnpb create secret generic mongo-keyfile \
  --from-file=mongo-keyfile=infra/keys/mongo-keyfile

# Deploy (dev or demo)
kubectl apply -k deploy/k8s/overlays/dev
kubectl -n sistem-informasi-kebencanaan-bnpb logs -f job/mongo-init
```

Every member starts empty. The highest-priority member of each replica set
(`mongod-start.sh` + `bootstrap.js`) runs `replSetInitiate` and creates the root user through
the localhost exception, but only when every peer is reachable and none already belongs to a
replica set, so a member whose disk was wiped just rejoins and resyncs. The `mongo-init` Job
is idempotent: it waits for the primaries, adds the shards, creates the `bnpb_app` and
`bnpb_monitor` users, shards `bnpb.hazard_events` on `{_id: "hashed"}`, creates the indexes
and seeds `volcano_reference`. If an older cluster has the single-member `mongodb` headless Service,
delete it before applying (`clusterIP` cannot change in place).

To change the layout, edit the table at the top of `scripts/gen-mongo-manifests.sh` and
run it; it rewrites `deploy/k8s/base/mongodb/generated/`, the topology files and the
dev overlay's delete patches.

For local work without Kubernetes, `scripts/dev-mongo-sharded.sh up` starts a small sharded
cluster in Docker (one config server, two single-member shards, mongos on
`127.0.0.1:27017`) using the same bootstrap and init scripts; `down` removes it. The store
contract tests run against the in-memory store on every `go test`, and also against Mongo
when `STORE_TEST_MONGO_URI` is set:

```bash
scripts/dev-mongo-sharded.sh up
STORE_TEST_MONGO_URI='mongodb://root:devroot@127.0.0.1:27017/?authSource=admin' \
  go test ./services/aggregator/internal/store/ -race -count=1
# stops shard-1 for real and checks reads come back partial instead of failing
STORE_TEST_DOCKER=1 STORE_TEST_MONGO_URI='mongodb://root:devroot@127.0.0.1:27017/?authSource=admin' \
  go test ./services/aggregator/internal/storemon/ -run PartialRead -v -count=1
scripts/dev-mongo-sharded.sh down
```

The aggregator probes every storage unit with `hello` every 2 seconds and marks a unit
down after two failed probes in a row. Reads that cross shards answer with
`partial` and `unavailable_units` instead of failing: `STORE_PARTIAL_MODE=literal` (default)
sets `partial` whenever a data unit is down, `data` only when a whole shard is unreachable.
Units are named per member (`shard-2-c`, `cfg-a`) so the name survives a failover; the node
comes from the Kubernetes API (read-only Role `aggregator-pod-reader`). `GET /metrics` on the
aggregator exposes `store_unit_up`, `store_unit_primary`, `store_docs`, `store_data_bytes`,
`store_ops_total`, `store_repl_ops_total`, `store_query_duration_seconds` and
`store_partial_reads_total`, labelled by `unit`, `shard` and `node`. Here `node` is the node that
holds the unit, not the node of the aggregator pod; Prometheus keeps it because the `bnpb-pods` job
sets `honor_labels: true`. Every aggregator replica
exports the same unit series, so deduplicate with `max by (node, unit)` first. Write skew
across nodes (1.0 means perfectly even):

```promql
max(sum by (node) (max by (node, unit) (rate(store_ops_total{op="insert"}[1m]))))
/
avg(sum by (node) (max by (node, unit) (rate(store_ops_total{op="insert"}[1m]))))
```

### Writing HazardEvents from other services (`hazard-upserts`)

The aggregator is the only writer of HazardEvents. Polled sources (BMKG, PVMBG) are stored
directly; every other producer (wildfire processor, coastal ingester, dataset loader) sends
messages to `bnpb.hazard-upserts.v1` (6 partitions, 7-day retention), which all aggregator
replicas consume as group `bnpb.aggregator.hazard-upserts`. Build messages with
`hazard.EncodeUpsert` instead of writing JSON by hand:

```go
enc, err := hazard.EncodeUpsert(hazard.UpsertMessage{
    Op: hazard.OpUpsert, Source: hazard.SourceSatelit, SourceRefID: "fire-0a41c7", Version: 1042,
    HazardType: hazard.TypeWildfire, Severity: hazard.SeveritySiaga, AreaName: "Kalimantan Tengah",
    Latitude: -2.51234, Longitude: 113.8045, OccurredAt: occurredAt, Attributes: attrs,
}, hazard.UpsertMeta{CorrelationID: cid, Traceparent: tp})
// write enc.Key, enc.Value and enc.Headers to bnpb.hazard-upserts.v1
```

- The key `source|source_ref_id` keeps every update of one event on one partition, in order.
- `version` must increase with every change of the same event (feed offset for wildfires,
  `observed_at` in ms for coastal readings); an equal or lower version is ignored.
- `op: retract` only needs `source`, `source_ref_id` and `version`: the event stays and is
  marked retracted, and a later, higher version revives it.
- The aggregator computes `hazard_id` and `ingested_at`; values sent by producers are ignored.
- Invalid messages (bad JSON, invalid UTF-8, unknown op/source/type, version ≤ 0, over
  1 MB) are parked and counted in `hazard_upserts_dead_lettered_total{reason}`; the
  partition keeps moving. Storage or publish failures are retried in place and the offset is
  committed only after the event is stored and published.
- Stored events are re-published to `bnpb.hazard-events.v1` with headers `X-Hazard-Version`
  and `X-Hazard-Op`; consumers deduplicate on `hazard_id` + version, so updates and
  retractions are not mistaken for duplicates.
- `hazard_store_latency_seconds{source}` measures `produced_at` → stored (P4 p95).

### Latest events across every shard

`GET /hazards` without `type` returns the newest events of every hazard type, newest
`ingested_at` first, read across all shards. The body stays a JSON array like M1; paging uses
`X-Next-Cursor`, and when part of the storage is unreachable the response carries
`X-Partial: true` and `X-Unavailable-Units`. Retracted events are hidden. The aggregator
caches the first page for 1 second and collapses concurrent misses into one query, so the
load test target `GET /hazards?limit=50` does not hammer MongoDB.

```bash
curl -s -i -H "Authorization: Bearer $FIELD" "http://localhost:8080/hazards?limit=50"
```

### Audit API

Served by the aggregator under `/internal/audit/*` (the ingress routes it there) and protected
by `Authorization: Bearer <token>`, where the token is one of the comma-separated values in
`AUDIT_TOKENS` (Secret `bnpb-secrets`). Without any token configured the endpoints answer 503
`audit_not_configured`; at most 16 audit requests run at once per aggregator pod, the rest get 429. Errors use
`{"error":{"code":"...","message":"..."}}`; storage problems answer 503 or `partial: true`,
never 500.

| Endpoint | What it returns |
|---|---|
| `GET /internal/audit/events?source&since&limit&cursor` | Every HazardEvent field, ascending by `ingested_at` then `hazard_id`; `since` filters `ingested_at >=`; `limit` 1–1000 (default 100); `next_cursor` is `null` on the last page |
| `GET /internal/audit/checksum?source` | `count` and SHA-256 over the `source_ref_id` values, sorted by byte, joined by `\n`, no trailing newline, lowercase hex. Without `source` it covers every source (`"source": null`) |
| `GET /internal/audit/location?hazard_id` | The shard that holds the event (asked from mongos) and its reachable copies: `{node, unit, role}`; 404 if the event does not exist |

`AUDIT_INCLUDE_RETRACTED` (default `false`) decides whether retracted events count in
`/events` and `/checksum`; both endpoints always follow the same rule.

### Loading the historical earthquake catalog (P1)

The loader ships inside the aggregator image (`/usr/local/bin/loader`) and sends the catalog
through `hazard-upserts` as `source: BMKG`, keeping each row's id as `source_ref_id` byte for
byte. It reads CSV, JSON or JSON Lines, recognises common column names (`id`, `time`,
`latitude`, `longitude`, `mag`, `depth`, `place`, ...) and accepts `--col-*`, `--time-layout`
and `--tz` when the file differs. Rows go through the same BMKG mapping as live events; a row
without a region gets the nearest province. Invalid rows are skipped and listed;
`--strict` stops at the first one. It must run inside the cluster (Kafka advertises
`kafka:9092`).

The BMKG mock keeps generating random `BMKG` events, which would change the checksum. Put it
in quiet mode (no seed, no new events) before a checksum run:

```bash
NS=sistem-informasi-kebencanaan-bnpb
kubectl -n $NS set env deploy/bmkg-mock BMKG_EVENT_INTERVAL_SECONDS=0

# 1. expected count and checksum of the file, nothing is sent (compare with the assistant's)
kubectl -n $NS run loader-check --rm -i --restart=Never --image=bnpb/aggregator:dev \
  --command -- loader --mode=load --file=- --dry-run < katalog.csv

# 2. load it (or edit --url in deploy/k8s/jobs/loader.yaml and kubectl apply it)
kubectl -n $NS run loader --rm -i --restart=Never --image=bnpb/aggregator:dev \
  --env KAFKA_BROKERS=kafka:9092 --command -- loader --mode=load --file=- --rate=500 < katalog.csv

# 3. simulate a burst in one region, then remove it again (ids sim-spike-<run>-NNNNNN)
kubectl -n $NS run spike --rm -i --restart=Never --image=bnpb/aggregator:dev --env KAFKA_BROKERS=kafka:9092 \
  --command -- loader --mode=spike --run=s1 --count=5000 --rate=200 --region="Selatan Jawa Barat" --lat=-8.1 --lon=107.6
kubectl -n $NS run cleanup --rm -i --restart=Never --image=bnpb/aggregator:dev --env KAFKA_BROKERS=kafka:9092 \
  --command -- loader --mode=cleanup --run=s1 --count=5000

# 4. the Audit checksum must now equal step 1
curl -s -H "Authorization: Bearer $AUDIT" "http://<aggregator>/internal/audit/checksum?source=BMKG"
```

`purge` (used by cleanup) is only accepted for `source_ref_id` values starting with `sim-`;
anything else is parked, so real data cannot be deleted this way. Before the real catalog
arrives, `loader --mode=generate --count=20000 --seed=7 --out=sample.csv` writes a synthetic
catalog with a realistic skew (about 70% along the Sumatra–Java subduction, 20%
Sulawesi–Maluku, 10% elsewhere) to rehearse the whole flow.

**Shard-key experiments.** With `STORE_EXPERIMENTS=on` on the aggregator, every write is also
copied into `exp_by_area` (shard key `{area_name: 1}`) and `exp_by_ingested`
(`{ingested_at: 1}`), both with a 1 MB chunk size so the balancer really splits them. Load the
same data, then compare all three collections per shard and per node:

```bash
scripts/p1-distribution.sh   # docs per shard, per node, and max/avg skew for each collection
```

`store_docs{collection=...}` shows the same split in Grafana. Turn the flag off and drop the
experiment collections after the session (`db.exp_by_area.drop(); db.exp_by_ingested.drop()`;
the next `mongo-init` run recreates them empty).

```bash
AUDIT=change-me-audit
curl -s -H "Authorization: Bearer $AUDIT" "http://localhost:8083/internal/audit/checksum?source=BMKG"
curl -s -H "Authorization: Bearer $AUDIT" "http://localhost:8083/internal/audit/events?limit=5"
curl -s -H "Authorization: Bearer $AUDIT" "http://localhost:8083/internal/audit/location?hazard_id=<id>"
```

### Analytics dashboard (P1)

Open `http://<ingress>/analytics` in a browser and sign in as `analytics-viewer`
(`CLIENT_ANALYTICS_ID` / `CLIENT_ANALYTICS_SECRET`; BNPB Internal Ops also works). The
`analytics` scope is read-only: it can load the dashboard data but is refused by client-api
and by the ops endpoints. The page keeps the token in memory only and refreshes it before it
expires; leaving the page or a failed refresh asks for the password again. Without
`CLIENT_ANALYTICS_SECRET` the auth-service simply has no analytics client.

It shows every HazardEvent in the chosen range (24 h, 7 d, 30 d, 1 y, all, or a custom UTC
range), counted by occurrence time:

- a line chart per hazard kind: Gempa, Tsunami, Gunung api, Kebakaran hutan, Pesisir.
  `hazard_type` has four values; Tsunami is the `SEISMIC` events whose
  `attributes.potential_tsunami` is `true`, and Gempa the other `SEISMIC` events, so the five
  kinds never overlap and add up to the total;
- counts per severity, and a sortable, searchable table per `area_name`;
- a table of the five totals next to the chart, so no number depends on color alone.

`GET /analytics/data?range=30d&bucket=day` (or `from`/`to`) returns the same numbers as JSON.
The aggregator computes them with one `$group` through mongos and caches each range for 5
seconds. When a shard cannot be reached, that aggregate fails, so it falls back to a
`find` with `allowPartialResults` through mongos, counts in Go and answers
`partial: true`; the page then shows a red banner and marks every number with `≥`. A dead
unit with the data still served by its replicas shows a yellow banner and exact numbers.
Retracted events follow `AUDIT_INCLUDE_RETRACTED`, the same rule as the Audit API.

To prove the numbers equal a direct count from the Audit API, recount every page of
`/internal/audit/events` and compare category, severity, area and bucket totals:

```bash
CLIENT_ANALYTICS_SECRET=... AUDIT_TOKENS=... \
  go run ./services/aggregator/cmd/verify-analytics --base=http://<ingress> --bucket=week
```

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
lag()   { docker exec bnpb-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1"; echo; }
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
```

The same checks, plus the outage scenario below, can also run in one go with
`scripts/verify-p2.sh`. It needs the 3-second PVMBG delay set above, so run it before
going back to `.env`. It writes a PASS/FAIL report and the k6 logs to `evidence/p2/`;
`1`, `2` or `3` in place of `all` runs one scenario only.

```bash
env -u MSYS_NO_PATHCONV bash scripts/verify-p2.sh all   # SUMMARY: FAIL=0

# Back to the values in .env
$DC up -d --no-deps pvmbg-mock auth-service
```

Recreating `pvmbg-mock` also resets its schema version and outage flag. Our own runs are
in `loadtest/output-*-3.txt`, with the k6 summary export in `loadtest/result-*-3.json`,
and in `evidence/p2/`.

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

`scripts/verify-criteria.sh` runs every check in this section and writes a PASS/FAIL
report to `evidence/`, without tokens or credential values. It waits for the access
token to expire, so it takes about a minute and a half: every validator accepts a token up to
30 seconds past `exp` (`jwtauth.ClockLeeway`) so that clock skew between nodes does not cause
spurious 401s.

```bash
env -u MSYS_NO_PATHCONV bash scripts/verify-criteria.sh   # SUMMARY: FAIL=0
```

The same checks by hand:

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
sleep 95
code -H "Authorization: Bearer $OLD_ACCESS" "$API/hazards?type=SEISMIC"    # 401, expired (TTL 60 s + 30 s leeway)
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
  const v1 = {hazard_type: "VOLCANIC", "attributes.confidence_level": {$exists: false}};
  const v2 = {hazard_type: "VOLCANIC", "attributes.confidence_level": {$exists: true}};
  print("without confidence_level:", db.hazard_events.countDocuments(v1));
  print("with confidence_level:   ", db.hazard_events.countDocuments(v2));
  printjson(db.hazard_events.findOne(v1, {_id: 0, source_ref_id: 1, attributes: 1}));
  printjson(db.hazard_events.findOne(v2, {_id: 0, source_ref_id: 1, attributes: 1}))'

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
$DC logs --since 15s consumer-notifier | grep notification_sent   # missed SIAGA/AWAS events, if any
```

A new subscriber only needs a new consumer group. Below, a second copy of the regional
consumer joins with the group `bnpb.new` while everything is running. The aggregator,
the topic and the compose file stay as they are.

```bash
docker inspect -f '{{.Name}} {{.State.StartedAt}}' bnpb-aggregator bnpb-kafka
$DC run -d --no-deps --name bnpb-consumer-new -e KAFKA_GROUP_ID=bnpb.new consumer-regional
sleep 10
lag bnpb.new                                                       # reads from the first offset, LAG 0
docker logs bnpb-consumer-new | grep -c regional_feed_forwarded    # every event in the topic
docker inspect -f '{{.Name}} {{.State.StartedAt}}' bnpb-aggregator bnpb-kafka   # unchanged

# Clean up, so the demo can be repeated
docker stop bnpb-consumer-new && docker rm bnpb-consumer-new
docker exec bnpb-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group bnpb.new
```

### Following one correlation ID

```bash
ID=$($DC logs consumer-notifier | grep notification_sent | tail -1 | grep -o '"correlation_id":"[^"]*"' | cut -d'"' -f4)
$DC logs bmkg-mock pvmbg-mock aggregator consumer-dashboard consumer-notifier consumer-regional | grep "$ID"
```

The same ID appears in the source mock, the aggregator's outbound calls, and every consumer.

### M2 Problem 3: schema changes and the dead letter queue

**Declared schema policy.** The aggregator accepts a change automatically when it
does not touch a field the mapper requires: a new field, a missing optional field, or
fields in a different order. A change to a required field (renamed, different type, or
missing) is not guessed. The record is parked with reason `schema_incompatible` until
Ops adapt the mapping rules. Required fields are `report_id`, `volcano_id`,
`alert_level`, `reported_at` (PVMBG) and `event_id`, `magnitude`, `occurred_at` (BMKG).

| PVMBG mock version | Change | Aggregator |
| --- | --- | --- |
| `2` | `confidence_level` added | accepted, kept in `attributes` |
| `6` | `ash_column_height_m` removed | accepted |
| `3` | `alert_level` renamed to `level` | parked |
| `4` | `alert_level` becomes a number | parked |
| `5` | `reported_at` removed | parked |

**What is set aside, and how.**

| Failure | Status | `retryable` | What happens next |
| --- | --- | --- | --- |
| Response cannot be read (truncated, invalid UTF-8, nested > 32, larger than 4 MiB) | `parked` | `false` | Whole body kept; the poll window is fetched again |
| One record does not match the contract | `parked` | `false` | Only that record is kept; the rest of the response is stored |
| Storage or Kafka unavailable | `retrying` | `true` | Retried after 1 s, 2 s, 4 s, ... up to 5 min; parked after 8 attempts |

Messages are stored once (the id is a hash of the content) in the aggregator's
`unprocessed` collection. The audit listing takes an assistants' audit token
(`AUDIT_TOKENS`, comma separated). The ops endpoints take an audit token or an Internal
Ops JWT; Media and Field tokens get 403.

| Endpoint | Purpose |
| --- | --- |
| `GET /internal/audit/unprocessed?source=&limit=&cursor=` | Lampiran A listing (retrying and parked) |
| `GET /internal/ops/unprocessed?source=&limit=&cursor=` | Same listing for Internal Ops |
| `GET /internal/ops/unprocessed/{message_id}` | One message with its exact payload |
| `POST /internal/ops/replay?source=PVMBG` | Reprocess every parked message of a source, oldest first |

**Scenario** (Kubernetes, through the ingress on port 8090):

```bash
GW=http://localhost:8090
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/pvmbg-mock 8082:8082 &
PVMBG=http://localhost:8082
AUDIT="${AUDIT_TOKENS%%,*}"
OPS=$(curl -s -X POST "$GW/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"$CLIENT_OPS_ID\",\"password\":\"$CLIENT_OPS_SECRET\"}" | field access_token)

# 1. Incompatible change: records are parked, other sources keep flowing
curl -s -X POST -H "$ADMIN" -d '{"version":3}' "$PVMBG/admin/schema-version"
sleep 15
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/unprocessed?source=PVMBG"

# 2. Broken payloads: nothing crashes, the next responses are processed
for k in truncated deep invalid_utf8 oversize; do
  curl -s -X POST -H "$ADMIN" -d "{\"kind\":\"$k\",\"count\":1}" "$PVMBG/admin/corrupt"; sleep 5
done
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l app=aggregator   # RESTARTS stays 0

# 3. Only Internal Ops may look
code -H "Authorization: Bearer $MEDIA" "$GW/internal/ops/unprocessed"   # 403

# 4. Adapt the system, then replay: everything recovers, no duplicates
cp deploy/k8s/base/aggregator/mapping-rules.example.json deploy/k8s/base/aggregator/mapping-rules.json
bash scripts/deploy.sh dev            # new ConfigMap hash restarts the aggregator
curl -s -X POST -H "Authorization: Bearer $OPS" "$GW/internal/ops/replay?source=PVMBG"
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/unprocessed?source=PVMBG"
```

After the replay only the four broken responses from step 2 are still listed. Their
bytes can never be read, so they stay parked as evidence; the data they should have
carried was fetched again by the next poll.

### M2 Problem 3: field reports

Tim Lapangan sends reports to `POST /field-reports` (served by `field-report-api`).
The `Idempotency-Key` header is required: the client creates one key per report and
reuses it on every retry. The key becomes the `report_id`.

```bash
curl -s -X POST "$GW/field-reports" -H "Authorization: Bearer $FIELD" \
  -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' \
  -d '{"hazard_type":"VOLCANIC","latitude":-7.54,"longitude":110.44,
       "observed_at":"2026-11-18T08:00:00Z","description":"hujan abu di desa"}'
curl -s "$GW/field-reports/<report_id>" -H "Authorization: Bearer $FIELD"   # pending or processed
```

| Field | Rule |
| --- | --- |
| `hazard_type` | `SEISMIC`, `VOLCANIC`, `WILDFIRE` or `COASTAL` |
| `latitude`, `longitude` | required, valid coordinates |
| `observed_at` | required, RFC 3339 |
| `description` | required, at most 2000 characters |
| `area_name`, `severity_estimate` | optional |

**Delivery guarantee.** `202` is sent only after Kafka acknowledged the report with
`acks=all`; if Kafka does not answer within 3 s the client gets `503` and retries with
the same key. The aggregator reads the topic with at-least-once delivery (the offset
is committed only after the report is stored, and a failing store is retried, never
skipped) and stores idempotently (`report_id` is the document id, written only on
insert). Together: every report that got `202` is processed exactly once in effect.
Only `field` and `internal` scopes may send; `media` gets 403.

`scripts/verify-field-reports.sh [count]` scales the aggregator to 0, sends reports
while deleting a `field-report-api` pod every 3 s, resends some with the same key,
brings the aggregator back and checks that every accepted report is stored once.

### M2 Problem 4: wildfire stream

`wildfire-processor` reads the Feed Satelit SSE stream, groups hotspots into fires
(Definisi Kebakaran: confidence not low, ≤ 1.0 km and ≤ 360 min apart, chained, at least
2 detections) and sends every change to `bnpb.hazard-upserts.v1`. The aggregator is the
only writer of HazardEvents and applies a message only when its `version` is newer.

| Decision | Choice |
| --- | --- |
| Source of truth | The feed itself. It replays from any offset, so the processor keeps its state in memory. After a restart it loads the newest snapshot and replays only the rest of the feed; without a usable snapshot it replays from offset 0. Nothing is sent until it reaches the offset that was latest at startup. |
| Snapshot | Every 60 s and on shutdown, right after a batch reached Kafka: the whole engine state, gob + gzip, in the compacted topic `bnpb.wildfire-snapshots.v1` (one key, Kafka keeps the newest). Kafka is not tied to the processor's node, so a rescheduled pod finds it. A snapshot from another feed session or another config is ignored. Measured: 168k events → 5 MB, saved in 1.1 s, restored in 0.9 s; a full replay of 432k events took 94 s. |
| Fire ID | `fire-` + hash of the detection that arrived first (lowest feed offset). Late detections arrive later, so they never rename a fire; a replay gives the same IDs. On a merge the older ID stays and the other is retracted. |
| Version | Feed offset + 1 of the event that last changed the fire. A replay produces the same versions, so the aggregator ignores what it already has. |
| Vanished fire | Retracted (`op: retract`), not deleted: the document stays with `retracted: true` and is hidden from the default listings. |
| Lateness | Watermark = newest detection time − 360 min, capped by the feed's `virtual_now` from heartbeats. A detection older than the watermark is not applied but listed at `/internal/wildfire/late` and counted. Updates and retracts always apply. |
| Final | A fire is final (`attributes.final`) once the watermark passes its last detection + 360 min: no new detection can join it any more. |
| Province | Nearest centroid among 38 provinces, computed from a CC BY 4.0 GeoJSON (see `internal/fire/province.go`). |
| Availability | 1 replica, `Recreate`, rescheduled 30 s after a node failure: the feed accepts one connection per group. |
| Feed session reset | Detected (offset goes back or profile changes) and logged; raise `VERSION_EPOCH` so the new session's versions are newer than the stored ones. |

Internal endpoints (audit token or Internal Ops JWT):

| Endpoint | Purpose |
| --- | --- |
| `GET /internal/wildfire/stats` | Offsets, lag, watermark, fires by state, single / late / low-confidence / duplicate counts, rebuild time |
| `GET /internal/wildfire/fires` | Every current fire and a digest of them |
| `GET /internal/wildfire/late?limit=&after_offset=` | Late detections |

Metrics on `:8086/metrics`: `wildfire_lag_events`, `wildfire_watermark_lag_seconds`,
`wildfire_feed_latest_offset`, `wildfire_processed_offset`, `wildfire_detections{state}`,
`wildfire_fires{state}`, `wildfire_feed_reconnects_total{reason}`, `wildfire_offset_gaps`,
`wildfire_rebuild_seconds`, `wildfire_restored_offset`, `wildfire_snapshot_offset`,
`wildfire_snapshot_bytes`, `wildfire_emit_delay_seconds`. End-to-end latency (feed
arrival until stored) is the aggregator's `hazard_store_latency_seconds{source="SATELIT"}`.

Checks:

```bash
# Run the engine over a recorded feed and compare with an O(n^2) recomputation
go run ./services/wildfire-processor/cmd/wildfire-check -in events.jsonl -out fires.json -snapshot
# On the cluster: kill the processor, let the feed run ahead, compare with stored events
scripts/verify-wildfire.sh
```

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
| `PVMBG_SCHEMA_VERSION` | `1` | Schema at startup, `1` to `6` (see the schema table under Source mocks) |

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
| PVMBG | `POST /admin/schema-version` with `{"version": 1..6}` | `X-PVMBG-Admin-Token: <token>` |
| PVMBG | `POST /admin/outage` with `{"enabled": true \| false}` | `X-PVMBG-Admin-Token: <token>` |
| PVMBG | `POST /admin/corrupt` with `{"kind": "...", "count": N}` | `X-PVMBG-Admin-Token: <token>` |
| both | `GET /health` | none |

PVMBG schema versions. Versions 3 to 6 build on version 2.

| Version | Change | Kind |
| --- | --- | --- |
| `1` | Base contract, no `confidence_level` | baseline |
| `2` | Adds `confidence_level` | compatible |
| `3` | `alert_level` renamed to `level` | incompatible |
| `4` | `alert_level` becomes a number (`1` Normal to `4` Awas) | incompatible |
| `5` | Required `reported_at` removed | incompatible |
| `6` | Optional `ash_column_height_m` removed | compatible |

`POST /admin/corrupt` breaks the next `count` responses of `GET /volcanic-reports`
(default 1, max 1000, `0` cancels), then the mock serves normal JSON again. `GET /health`
shows what is still pending.

| `kind` | Body served |
| --- | --- |
| `truncated` | Valid JSON cut in half |
| `deep` | Arrays nested 100,000 levels deep |
| `invalid_utf8` | A string value starting with bytes `0xff 0xfe` |
| `oversize` | Valid JSON padded past 8 MiB |

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

`GET /hazards?type=SEISMIC|VOLCANIC[&since=<RFC 3339>][&fields=<a,b>][&limit=<n>][&cursor=<c>]`
with `Authorization: Bearer <access_token>`. Results are sorted newest first, 100 per
page by default (`limit` up to 500). When more results exist, the response carries an
`X-Next-Cursor` header; pass its value as `cursor` to get the next page. When the
concurrency limit is reached, the API answers 429 instead of queueing.

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
- `GET /regional[?area=<area_name>]` on `consumer-regional`: the latest event per area,
  as forwarded to the regional government portal.

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
│   ├── consumer-regional/        # Kafka consumer: regional feed
│   ├── field-report-api/         # POST /field-reports for Tim Lapangan
│   └── wildfire-processor/       # Feed Satelit stream to wildfire HazardEvents
│
├── scripts/
│   ├── gen-keys.sh               # Generates the JWT signing key
│   ├── health.sh                 # Checks /health of every service
│   ├── verify-p2.sh              # Problem 2 checks (latency, load, outage), uses p2-loadtest.js
│   └── verify-criteria.sh        # Problem 3 checks (credentials, scopes, token expiry)
│
├── loadtest/                     # k6 script and our load test results
├── evidence/                     # Reports written by the verify-*.sh scripts
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
