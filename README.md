<div align="center">

# **BNPB Hazard Gateway**
### IF4031 - Arsitektur Aplikasi Terdistribusi

<p>
A distributed disaster-coordination platform. It collects seismic, volcanic, wildfire and
coastal data from four sources, converts it into one canonical format, stores it in a sharded
cluster, and distributes it to BNPB's downstream clients on a four-node Kubernetes cluster.
</p>

<p>
  <img alt="Project Status" src="https://img.shields.io/badge/status-milestone_2-blue.svg">
</p>

</div>

---

## Table of Contents

- [About](#about)
  - [Architecture](#architecture)
  - [Data paths](#data-paths)
  - [Technologies](#technologies)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Kubernetes cluster](#kubernetes-cluster)
  - [Deploy](#deploy)
  - [Entry point](#entry-point)
  - [Docker Compose (Milestone 1 services)](#docker-compose-milestone-1-services)
- [Milestone 2 Problems](#milestone-2-problems)
  - [Problem 1: sharded storage, Audit API and analytics](#problem-1-sharded-storage-audit-api-and-analytics)
  - [Problem 2: replicated aggregator and leader election](#problem-2-replicated-aggregator-and-leader-election)
  - [Problem 3: schema changes, dead letters and field reports](#problem-3-schema-changes-dead-letters-and-field-reports)
  - [Problem 4: wildfire stream](#problem-4-wildfire-stream)
  - [Problem 5: unreliable coastal sensor](#problem-5-unreliable-coastal-sensor)
  - [Observability](#observability)
- [Milestone 1 Test Scenarios](#milestone-1-test-scenarios)
- [Configuration](#configuration)
  - [Ports](#ports)
  - [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Contributors](#contributors)

---

## About

This project is the Milestone 2 implementation of the IF4031 assignment. BMKG and PVMBG are
independent source agencies; BNPB is the coordination centre that collects their data, converts
it into the canonical `HazardEvent`, stores it, and serves it to Media, Tim Lapangan, and BNPB
Internal Ops with different access rights.

Milestone 2 extends Milestone 1 in scale and in scope:

- **Four sources.** Seismic data from BMKG and volcanic data from PVMBG (mock services built to
  the M1 contract), hotspots from the assistants' **Feed Satelit** (grouped into `WILDFIRE`
  events), and observations from the assistants' **Sensor Pantai** (`COASTAL`), which is
  deliberately unreliable.
- **Field reports.** Tim Lapangan can now send reports through `POST /field-reports`.
- **Kubernetes.** Every component runs on k3s across four VMs on three laptops, joined over
  Tailscale. The aggregator runs as three replicas, storage is a MongoDB sharded cluster
  (4 shards × 3 copies), and Kafka runs three brokers with replication factor 3.
- **Observability.** Every service exports Prometheus metrics and OpenTelemetry traces, shown in
  Grafana and Jaeger, instead of reading logs by hand.

Each of the five Milestone 2 problems is described under [Milestone 2 Problems](#milestone-2-problems).

### Architecture

```
                          ┌──────────────── Kubernetes (k3s), namespace sistem-informasi-kebencanaan-bnpb ────────────────┐
                          │                                                                                               │
  Media / Pers ──┐        │   ┌─ auth-service (JWT, JWKS)          bmkg-mock   pvmbg-mock       sensor-pantai             │
  Tim Lapangan ──┼─ REST ─┼─► traefik ─┬─ client-api ──REST──┐          ▲ REST polling ▲               ▲ REST polling     │
  Internal Ops ──┘  +JWT  │            ├─ field-report-api ─┐ │          │              │               │                  │
                          │            └─ /internal, /analytics ─► aggregator ×3 ──────┘       coastal-ingester ×2        │
                          │                                 │      │    ▲  (Lease per source)        │                      │
                          │                                 │      │    │ consume upserts,           │ critical / upserts / │
                          │                                 ▼      ▼    │ field reports              ▼ rejected             │
                          │                              ┌───────── Kafka (3 brokers, RF 3) ─────────────┐                  │
                          │                              └──┬────────────┬───────────┬──────────────┬───┘                  │
                          │                    consumer-dashboard  consumer-notifier  consumer-regional  field-receiver      │
                          │                                                                                               │
                          │   aggregator ──► mongos (every node) ──► shard-0..3 (3 copies each) + config servers           │
                          │   wildfire-processor ──► Kafka hazard-upserts      prometheus, grafana, jaeger                │
                          └──────────────────────────────────────▲────────────────────────────────────────────────────────┘
                                                                 │ SSE
                                                            Feed Satelit (outside the cluster)
```

| Component | Replicas | `role` label | Role |
| --- | --- | --- | --- |
| Traefik (k3s built-in) | 1 + ServiceLB on every node | - | Single entry point for every HTTP request from outside the cluster |
| `auth-service` | 1 | `api` | Issues RS256 JWTs per scope (`media`, `field`, `internal`, `analytics`) and publishes JWKS |
| `client-api` | 2 (HPA 2–4) | `api` | Client read API: verifies JWT, filters fields per scope, sheds Media load first |
| `field-report-api` | 2 | `api` | Accepts `POST /field-reports` and answers 202 only after Kafka `acks=all` |
| `aggregator` | 3 (HPA 3–5) | `aggregator` | Polls BMKG and PVMBG (one leader per source), maps to `HazardEvent`, the only writer to MongoDB, Audit API, analytics dashboard, dead letter queue |
| `wildfire-processor` | 1 | `consumer` | Reads Feed Satelit over SSE and groups hotspots into fires |
| `coastal-ingester` | 2 (one active leader) | `consumer` | The only client of Sensor Pantai: resilience layers, contract validation, critical alert path |
| `field-receiver` | 1 | `consumer` | Tim Lapangan's receiver for critical coastal alerts |
| `consumer-dashboard`, `consumer-notifier`, `consumer-regional` | 1 each | `consumer` | Milestone 1 HazardEvent consumers |
| `bmkg-mock`, `pvmbg-mock` | 1 each | `mock` | Simulated source agencies (M1 contract, plus schema and corruption modes in PVMBG) |
| `sensor-pantai` | 1 | `sensor` | Sensor Pantai image from the assistants |
| Kafka (KRaft) | 3 | `broker` | Message broker, replication factor 3, `min.insync.replicas=2` |
| MongoDB | 12 shard members + 3 config servers | `store` | 4 shards × 3 copies |
| `mongos` | 4 (DaemonSet) | `other` | MongoDB router, one per node |
| Prometheus, Grafana, Jaeger | 1 each | `other` | Metrics, dashboards, traces |

### Data paths

1. **Client reads.** Client → Traefik → client-api → aggregator → mongos → shards. client-api
   never calls a source, so its answers do not depend on BMKG, PVMBG or Sensor Pantai.
2. **BMKG and PVMBG polling.** Only the replica holding the source's Lease polls it. Results are
   written to MongoDB and published to `bnpb.hazard-events.v1`.
3. **Sensor Pantai.** coastal-ingester sends critical observations to `bnpb.critical-alerts.v1`
   (read by field-receiver), valid observations to `bnpb.hazard-upserts.v1`, and contract
   violations to `bnpb.sensor-rejected.v1`.
4. **Feed Satelit.** wildfire-processor reads the SSE stream, keeps its grouping state in the
   compacted topic `bnpb.wildfire-snapshots.v1`, and sends `WILDFIRE` events to
   `bnpb.hazard-upserts.v1`.
5. **Field reports.** field-report-api writes each report to `bnpb.field-reports.v1` before
   answering 202; the aggregator stores it.
6. **Observability.** Prometheus scrapes `/metrics` of every pod, every service sends traces to
   Jaeger over OTLP, and the trace context travels in HTTP headers and Kafka message headers.

Every HazardEvent write from a source other than BMKG and PVMBG goes through
`bnpb.hazard-upserts.v1`, so the aggregator stays the only writer of the store. NetworkPolicies
enforce it: only aggregator pods (and the `mongo-init` Job) can open connections to mongos and
the shard members.

Every service exposes `GET /health` and `GET /metrics`, writes JSON logs with `correlation_id`
and `trace_id`, and passes `X-Correlation-ID` and W3C `traceparent` along HTTP calls and Kafka
messages.

### Technologies

| Need | Technology |
| --- | --- |
| Language | Go 1.25 for every service |
| Orchestration | k3s v1.37.2 on VMs (k3d for development), Kustomize, `scripts/deploy.sh` |
| Nodes and network | VirtualBox and QEMU VMs with Ubuntu 24.04, Tailscale (WireGuard) between laptops |
| Entry point | Traefik (k3s Ingress) |
| Broker | Apache Kafka 3.9 (KRaft), 3 brokers, RF 3, `min.insync.replicas=2` |
| Storage | MongoDB 7.0 sharded cluster |
| Leader election | Kubernetes Lease through client-go `leaderelection`, `leaseTransitions` as fencing token |
| Resilience | failsafe-go (timeout, retry with jitter, circuit breaker, bulkhead) |
| Authentication | JWT (RS256) with a JWKS endpoint |
| Metrics and dashboards | Prometheus 2.55, Grafana 11.3 |
| Tracing | OpenTelemetry SDK → Jaeger 1.63 |
| Autoscaling | HorizontalPodAutoscaler + metrics-server |
| Load testing | k6 |
| Images | GitHub Container Registry, built by GitHub Actions |

## Getting Started

### Prerequisites

* Four VMs with Ubuntu 24.04, each with its own kernel, reachable from one another (we use Tailscale)
* `kubectl`, `git`, and a bash shell (Linux, macOS, or Git Bash on Windows) on the operator machine
* [k6](https://k6.io) for the load tests, run from a machine outside the cluster
* For development only: Docker and [k3d](https://k3d.io); Go 1.25 to run the unit tests

### Installation

```bash
git clone https://github.com/numshv/tugas-besar-if4031-lalaterdistribusi.git
cd tugas-besar-if4031-lalaterdistribusi
cp .env.example .env
bash scripts/gen-keys.sh
bash scripts/gen-mongo-keyfile.sh
```

- Fill in `.env`: client credentials, `MONGO_SLOT_NODES` (the node for Mongo slots a b c d, in
  that order), `PANTAI_AUTH_TOKEN`, `SATELLITE_TOKEN`, `AUDIT_TOKENS` and
  `GRAFANA_ADMIN_PASSWORD`. The example values only work for a local run.
- `gen-keys.sh` creates `infra/keys/jwt_private.pem`, which `auth-service` uses to sign tokens;
  `gen-mongo-keyfile.sh` creates the keyfile the MongoDB members use to authenticate each other.
- `.env` and `infra/keys/` are git-ignored. Never commit tokens, keys or the kubeconfig.

Unit tests: `go test ./...`

### Kubernetes cluster

Everything runs in the namespace `sistem-informasi-kebencanaan-bnpb`. Manifests are plain
Kustomize: `deploy/k8s/base/<service>/` per service, `overlays/dev` (k3d, local images) and
`overlays/demo` (VM cluster, images from GHCR, HPA on through `components/hpa`).

**Topology declaration:**

| Node | Role | Mongo slot | VM or native | Physical host | Host CPU / RAM |
|---|---|---|---|---|---|
| node-a | control plane + worker (never shut down by scenarios) | a | VM: QEMU + WHPX, Ubuntu 24.04, 4 vCPU / 6 GB | Laptop 1: Lenovo 21SY, Windows 11 Pro | Intel Core Ultra 5 225U (12 cores / 14 threads) / 15 GB |
| node-b | worker | b | VM: VirtualBox 7.2.2, Ubuntu 24.04, 1 vCPU / 6 GB | Laptop 2: ASUS VivoBook M1403QA, Windows 11 Home | AMD Ryzen 7 5800H (8 cores / 16 threads) / 16 GB |
| node-c | worker | c | VM: VirtualBox 7.2.4, Ubuntu 24.04, 2 vCPU / 6 GB | Laptop 3: Lenovo 83DK, Windows 11 | AMD Ryzen 7 8840HS (8 cores / 16 threads) / 14 GB |
| node-d | worker | d | VM: QEMU + WHPX, Ubuntu 24.04, 2 vCPU / 3.5 GB | Laptop 1 (same host as node-a) | Intel Core Ultra 5 225U (12 cores / 14 threads) / 15 GB |

Scenarios that shut a node down target node-b, node-c or node-d only. node-a and node-d share
one physical host: the scenarios stop one VM at a time, but losing that whole laptop would take
two copies of shard-2 (a, c, d) and shard-3 (a, b, d) at once. Nodes are joined over Tailscale
(`NODE_IP=<tailscale ip>`, `FLANNEL_IFACE=tailscale0`); node-a and node-d also have a direct
QEMU link (192.168.100.0/24) that Tailscale uses between them. node-b has 1 vCPU: laptop 2 runs
Hyper-V (WSL2 and Windows security features), so VirtualBox runs on top of it (NEM mode), where a
2-vCPU guest hangs at boot.

Demo nodes are VMs with their own kernel (k3d shares the host kernel and is for development only):

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

### Deploy

```bash
scripts/deploy.sh demo     # VM cluster, images from GHCR
scripts/deploy.sh dev      # k3d, builds the images and loads them into the cluster
```

Both create the Secrets from `.env`, label the Mongo slots from `MONGO_SLOT_NODES`, apply the
overlay and prune objects that were removed from the manifests. Images are built by
`.github/workflows/images.yml` on every push to `dev` and `main` (the packages must be public, or
add an `imagePullSecret`). Each build is tagged with its commit SHA and its branch; only `main`
moves `latest`, which `overlays/demo` uses. Before the exam, pin the demo to one tested commit so
that pods rescheduled during a node kill pull the same code:

```bash
SHA=$(git rev-parse origin/main)
sed -i "s/newTag: latest/newTag: $SHA/" deploy/k8s/overlays/demo/kustomization.yaml
```

Then install the Tester Agent and check the result:

```bash
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl exec -n tester-agent daemonset/tester-agent -- /app list

kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide     # every pod Running
kubectl -n sistem-informasi-kebencanaan-bnpb get leases           # who polls BMKG, PVMBG and Sensor Pantai
```

### Entry point

Traefik listens on port 80 of every node (k3d: `http://localhost:8090`). The examples below use
`GW=http://<any node>`.

| Path | Service |
|---|---|
| `/login`, `/refresh`, `/.well-known/` | auth-service |
| `/hazards` (and every other path) | client-api |
| `/field-reports` | field-report-api |
| `/internal/audit/`, `/internal/ops/`, `/analytics` | aggregator |
| `/internal/wildfire/` | wildfire-processor |
| `/grafana/`, `/jaeger/`, `/prometheus/` | observability (see [Observability](#observability)) |

Access tokens last 60 seconds; log in again on a 401.

```bash
set -a; . ./.env; set +a
GW=http://<any node>
login() { curl -s -X POST "$GW/login" -H 'Content-Type: application/json' -d "{\"username\":\"$1\",\"password\":\"$2\"}"; }
field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"; }
code()  { curl -s -o /dev/null -w '%{http_code}\n' "$@"; }
FIELD=$(login "$CLIENT_FIELD_ID" "$CLIENT_FIELD_SECRET" | field access_token)
curl -s -H "Authorization: Bearer $FIELD" "$GW/hazards?limit=5"
```

### Docker Compose (Milestone 1 services)

`infra/docker-compose.yml` still starts the Milestone 1 services (mocks, aggregator with a single
MongoDB, auth-service, client-api, Kafka and the three consumers) on one machine. It is kept for
the [Milestone 1 test scenarios](#milestone-1-test-scenarios); the Milestone 2 services only run
on Kubernetes.

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
bash scripts/health.sh
docker compose --env-file .env -f infra/docker-compose.yml down      # keep data
docker compose --env-file .env -f infra/docker-compose.yml down -v   # also delete MongoDB, Kafka and auth data
```

**Windows.** Run the scripts from Git Bash and `export MSYS_NO_PATHCONV=1` first; otherwise Git
Bash rewrites container paths such as `/run/keys/...`. The `verify-*.sh` scripts are the
exception: they need the variable unset, so start them with `env -u MSYS_NO_PATHCONV`. Port 8080
is often reserved by Windows (`netsh interface ipv4 show excludedportrange protocol=tcp`); if it
is, change `CLIENT_API_HOST_PORT` in `.env`.

## Milestone 2 Problems

### Problem 1: sharded storage, Audit API and analytics

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

The aggregator connects to `mongodb:27017`, which points to the mongos on each node.
`bnpb.hazard_events` is sharded on `{_id: "hashed"}` (`_id` is the UUIDv5 `hazard_id`), so the
spread does not depend on region or time. Writes use `writeConcern: majority`; reads use
`readConcern: majority`. The `dev` overlay keeps the same 4 slots but runs one member per shard
and one config server, with less memory, so it fits a laptop k3d cluster.

```bash
# Once per cluster: give every node a slot (scripts/deploy.sh does this from MONGO_SLOT_NODES)
scripts/label-nodes.sh <node-a> <node-b> <node-c> <node-d>

# Once per cluster: the keyfile secret (MONGO_ROOT_PASSWORD, MONGO_APP_PASSWORD, MONGO_MONITOR_PASSWORD live in bnpb-secrets)
kubectl -n sistem-informasi-kebencanaan-bnpb create secret generic mongo-keyfile \
  --from-file=mongo-keyfile=infra/keys/mongo-keyfile

kubectl -n sistem-informasi-kebencanaan-bnpb logs -f job/mongo-init
```

Every member starts empty. The highest-priority member of each replica set
(`mongod-start.sh` + `bootstrap.js`) runs `replSetInitiate` and creates the root user through
the localhost exception, but only when every peer is reachable and none already belongs to a
replica set, so a member whose disk was wiped just rejoins and resyncs. The `mongo-init` Job
is idempotent: it waits for the primaries, adds the shards, creates the `bnpb_app` and
`bnpb_monitor` users, shards `bnpb.hazard_events`, creates the indexes and seeds
`volcano_reference`. If an older cluster has the single-member `mongodb` headless Service,
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

**Health and partial reads.** The aggregator probes every storage unit with `hello` every 2
seconds and marks a unit down after two failed probes in a row. Reads that cross shards answer
with `partial` and `unavailable_units` instead of failing: `STORE_PARTIAL_MODE=literal` (default)
sets `partial` whenever a data unit is down, `data` only when a whole shard is unreachable.
Units are named per member (`shard-2-c`, `cfg-a`) so the name survives a failover; the node
comes from the Kubernetes API (read-only Role `aggregator-pod-reader`). `GET /metrics` on the
aggregator exposes `store_unit_up`, `store_unit_primary`, `store_docs`, `store_data_bytes`,
`store_ops_total`, `store_repl_ops_total`, `store_query_duration_seconds` and
`store_partial_reads_total`, labelled by `unit`, `shard` and `node`. Here `node` is the node that
holds the unit, not the node of the aggregator pod; Prometheus keeps it because the `bnpb-pods` job
sets `honor_labels: true`. Every aggregator replica exports the same unit series, so deduplicate
with `max by (node, unit)` first. Write skew across nodes (1.0 means perfectly even):

```promql
max(sum by (node) (max by (node, unit) (rate(store_ops_total{op="insert"}[1m]))))
/
avg(sum by (node) (max by (node, unit) (rate(store_ops_total{op="insert"}[1m]))))
```

**Latest events across every shard.** `GET /hazards` without `type` returns the newest events of
every hazard type, newest `ingested_at` first, read across all shards. The body stays a JSON
array like M1; paging uses `X-Next-Cursor`, and when part of the storage is unreachable the
response carries `X-Partial: true` and `X-Unavailable-Units`. Retracted events are hidden. The
aggregator caches the first page for 1 second and collapses concurrent misses into one query, so
the load test target `GET /hazards?limit=50` does not hammer MongoDB.

```bash
curl -s -i -H "Authorization: Bearer $FIELD" "$GW/hazards?limit=50"
```

**Audit API.** Served by the aggregator under `/internal/audit/*` and protected by
`Authorization: Bearer <token>`, where the token is one of the comma-separated values in
`AUDIT_TOKENS` (Secret `bnpb-secrets`). Without any token configured the endpoints answer 503
`audit_not_configured`; at most 16 audit requests run at once per aggregator pod, the rest get
429. Errors use `{"error":{"code":"...","message":"..."}}`; storage problems answer 503 or
`partial: true`, never 500.

| Endpoint | What it returns |
|---|---|
| `GET /internal/audit/events?source&since&limit&cursor` | Every HazardEvent field, ascending by `ingested_at` then `hazard_id`; `since` filters `ingested_at >=`; `limit` 1–1000 (default 100); `next_cursor` is `null` on the last page |
| `GET /internal/audit/checksum?source` | `count` and SHA-256 over the `source_ref_id` values, sorted by byte, joined by `\n`, no trailing newline, lowercase hex. Without `source` it covers every source (`"source": null`) |
| `GET /internal/audit/location?hazard_id` | The shard that holds the event (asked from mongos) and its reachable copies: `{node, unit, role}`; 404 if the event does not exist |
| `GET /internal/audit/unprocessed?source&limit&cursor` | Dead letter listing (see Problem 3) |

`AUDIT_INCLUDE_RETRACTED` (default `false`) decides whether retracted events count in
`/events` and `/checksum`; both endpoints always follow the same rule.

```bash
AUDIT="${AUDIT_TOKENS%%,*}"
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/checksum?source=BMKG"
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/events?limit=5"
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/location?hazard_id=<id>"
```

**Loading the historical earthquake catalog.** The loader ships inside the aggregator image
(`/usr/local/bin/loader`) and sends the catalog through `hazard-upserts` as `source: BMKG`,
keeping each row's id as `source_ref_id` byte for byte. It reads CSV, JSON or JSON Lines,
recognises common column names (`id`, `time`, `latitude`, `longitude`, `mag`, `depth`, `place`,
...) and accepts `--col-*`, `--time-layout` and `--tz` when the file differs. Rows go through the
same BMKG mapping as live events; a row without a region gets the nearest province. Invalid rows
are skipped and listed; `--strict` stops at the first one. It must run inside the cluster (Kafka
advertises `kafka:9092`). On the demo cluster use the aggregator image from GHCR instead of
`bnpb/aggregator:dev`.

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
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/checksum?source=BMKG"
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

**Analytics dashboard.** Open `$GW/analytics` in a browser and sign in as `analytics-viewer`
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
  go run ./services/aggregator/cmd/verify-analytics --base=$GW --bucket=week
```

### Problem 2: replicated aggregator and leader election

The aggregator runs as one Deployment with three replicas, spread over nodes with
`topologySpreadConstraints` (`maxSkew: 1`, `ScheduleAnyway`). Rolling updates use
`maxUnavailable: 0` and `maxSurge: 1`; a stopping pod runs a 5 s `preStop`, then a graceful
shutdown of up to 10 s. Pods on a lost node are rescheduled after 30 s (`not-ready` and
`unreachable` tolerations).

| Work | Done by | Coordination |
| --- | --- | --- |
| HTTP (reads, Audit API, analytics, ops) | every replica | none, state lives in MongoDB |
| Kafka consumers (`hazard-upserts`, `field-reports`) | every replica | Kafka consumer group |
| Polling BMKG | one replica | Lease `poller-bmkg` |
| Polling PVMBG | one replica | Lease `poller-pvmbg` |

**Leader election and fencing** (`pkg/lease`, also used by coastal-ingester):

- One `coordination.k8s.io/Lease` per source through client-go `leaderelection`:
  `LeaseDuration` 15 s, `RenewDeadline` 10 s, `RetryPeriod` 2 s, `ReleaseOnCancel` true
  (`LEASE_DURATION_SECONDS`, `LEASE_RENEW_DEADLINE_SECONDS`, `LEASE_RETRY_PERIOD_SECONDS`).
  Expiry is measured with each replica's monotonic clock from the last observed change, so a
  clock skew of +45 s or −30 s does not decide who leads.
- `spec.leaseTransitions` is the epoch (fencing token). Before every poll cycle the leader calls
  `Confirm`, which reads the Lease from the API server and stops if the holder or epoch changed.
- MongoDB keeps a `fences` document per source with the highest epoch that wrote and the poll
  cursor. `CommitPoll` stores a cycle in one transaction (`readConcern: snapshot`,
  `writeConcern: majority`) that rejects an older epoch with `ErrStaleEpoch`. A new leader raises
  the fence before its first poll, so a frozen old leader that wakes up cannot write.
  Rejections are counted in `poller_fenced_writes_rejected_total`.
- The cursor only moves after a whole cycle is stored; the next cycle re-reads 2 minutes before
  it (`cursorOverlap`). Deterministic `hazard_id` and versioned upserts make the re-read harmless.
- After each term a replica sits out one lease period before it runs for leader again, so a
  replica that keeps failing (for example, one that cannot reach storage) cannot keep winning
  the lease it just released.

Each replica has a CPU limit of 500m and memory 256Mi/512Mi; at most 64 requests run at once
(`AGG_MAX_CONCURRENCY`), the rest get 429 instead of queueing until they time out.

```bash
NS=sistem-informasi-kebencanaan-bnpb
kubectl -n $NS get leases                                   # holder and epoch per source
kubectl -n $NS delete pod <holder of poller-bmkg>           # failover: a new holder within ~20 s, epoch + 1
# Freeze: on the VM host, VBoxManage controlvm <vm> pause; wait 30 s; VBoxManage controlvm <vm> resume
#   QEMU node (node-d): send "stop" then "cont" to its monitor (tcp 127.0.0.1:4444 on laptop 1)
#   the woken pod logs "lease no longer ours" and sends nothing; Grafana shows the epoch change
# Clock skew and node kills: Tester Agent, e.g. kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>
k6 run loadtest/m2/p2-sustained.js & loadtest/m2/p2-chaos.sh   # 10,000 VUs while scaling 3-5-2 and a rolling update
k6 run -e VUS=400 loadtest/m2/p2-throughput.js              # once with 1 and once with 3 aggregator replicas
```

k6 needs the client credentials as `-e MEDIA_ID=.. -e MEDIA_SECRET=..` (and `FIELD_`, `INTERNAL_`)
plus `-e BASE_URL=http://<node>`; run it from a machine outside the cluster. HPA (`components/hpa`,
aggregator 3–5 and client-api 2–4 at 70% CPU) is turned off during the manual 3-5-2 scaling test
so it does not fight the `kubectl scale` commands.

### Problem 3: schema changes, dead letters and field reports

**Declared schema policy.** The aggregator accepts a change automatically when it
does not touch a field the mapper requires: a new field, a missing optional field, or
fields in a different order. A change to a required field (renamed, different type, or
missing) is not guessed. The record is parked with reason `schema_incompatible` until
Ops adapt the mapping rules (`rename`, `values`, `defaults` in the aggregator ConfigMap).
Required fields are `report_id`, `volcano_id`, `alert_level`, `reported_at` (PVMBG) and
`event_id`, `magnitude`, `occurred_at` (BMKG).

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
| `POST /internal/ops/retract?source=SATELIT` | Retract every live HazardEvent of a source (one version above what is stored); fallback for clearing a replaced feed session |

**Scenario:**

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/pvmbg-mock 8082:8082 &
PVMBG=http://localhost:8082
ADMIN="X-PVMBG-Admin-Token: $PVMBG_ADMIN_TOKEN"
OPS=$(login "$CLIENT_OPS_ID" "$CLIENT_OPS_SECRET" | field access_token)
MEDIA=$(login "$CLIENT_MEDIA_ID" "$CLIENT_MEDIA_SECRET" | field access_token)

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
bash scripts/deploy.sh demo           # new ConfigMap hash restarts the aggregator
curl -s -X POST -H "Authorization: Bearer $OPS" "$GW/internal/ops/replay?source=PVMBG"
curl -s -H "Authorization: Bearer $AUDIT" "$GW/internal/audit/unprocessed?source=PVMBG"
```

After the replay only the four broken responses from step 2 are still listed. Their
bytes can never be read, so they stay parked as evidence; the data they should have
carried was fetched again by the next poll.

**Field reports.** Tim Lapangan sends reports to `POST /field-reports` (served by
`field-report-api`). The `Idempotency-Key` header is required: the client creates one key per
report and reuses it on every retry. The key becomes the `report_id`.

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

### Problem 4: wildfire stream

`wildfire-processor` reads the Feed Satelit SSE stream, groups hotspots into fires
(Definisi Kebakaran: confidence not low, ≤ 1.0 km and ≤ 360 min apart, chained, at least
2 detections) and sends every change to `bnpb.hazard-upserts.v1`. The aggregator is the
only writer of HazardEvents and applies a message only when its `version` is newer.

| Decision | Choice |
|---|---|
| Source of truth | The feed itself. It replays from any offset, so the processor keeps its state in memory. After a restart it loads the newest snapshot and replays only the rest of the feed; without a usable snapshot it replays from offset 0. Nothing is sent until it reaches the offset that was latest at startup. |
| Snapshot | Every 60 s and on shutdown, right after a batch reached Kafka: the whole engine state, gob + gzip, in the compacted topic `bnpb.wildfire-snapshots.v1` (one key, Kafka keeps the newest). Kafka is not tied to the processor's node, so a rescheduled pod finds it. A snapshot from another feed session or another config is ignored. Measured: 168k events → 5 MB, saved in 1.1 s, restored in 0.9 s; a full replay of 432k events took 94 s. |
| Fire ID | `fire-` + hash of the detection that arrived first (lowest feed offset). Late detections arrive later, so they never rename a fire; a replay gives the same IDs. On a merge the older ID stays and the other is retracted. |
| Version | `epoch × 10⁹ + feed offset + 1` of the event that last changed the fire. A replay produces the same versions, so the aggregator ignores what it already has. |
| Epoch | Unix seconds when the processor first saw the feed session, kept in the snapshot so a restart in the same session reuses it. A later session always gets a larger epoch, so its versions beat everything stored from earlier sessions. A new epoch is never below the last snapshot's, so a node clock that was moved back cannot produce older versions. `VERSION_EPOCH` can only raise it. |
| Vanished fire | Retracted (`op: retract`), not deleted: the document stays with `retracted: true` and is hidden from the default listings. |
| Lateness | Watermark = median of the newest detection time per satellite − 360 min, so one satellite whose clock runs ahead cannot push the others into the late list. The heartbeat clock is not used: heartbeats only come when the feed is idle and lag by hours (median 3.7 h, up to 16 h in the practice recordings). A detection older than the watermark is not applied but listed at `/internal/wildfire/late` and counted. Updates and retracts always apply. |
| Final | A fire is final (`attributes.final`) once the watermark passes its last detection + 360 min: no new detection can join it any more. |
| Province | Nearest centroid among 38 provinces, computed from a CC BY 4.0 GeoJSON (see `internal/fire/province.go`). |
| Availability | 1 replica, `Recreate`, rescheduled 30 s after a node failure: the feed accepts one connection per group. |
| Feed session change | The exam (`POST /v1/exam/start`) is expected to replace the practice session. Noticed when the profile changes, when `latest_offset` goes back twice in a row (one odd answer is ignored), or when the feed answers 400 for our resume offset and is now behind it. The processor then retracts every fire of the old session, starts an empty engine with a newer epoch at offset 0, and saves a snapshot at once. If the session changed while the processor was down, the snapshot it finds belongs to the old session: the fires that snapshot left live are retracted on start. Fires born after that snapshot can be cleared by Internal Ops with `POST /internal/ops/retract?source=SATELIT`. |

Internal endpoints (audit token or Internal Ops JWT):

| Endpoint | Purpose |
|---|---|
| `GET /internal/wildfire/stats` | Offsets, lag, watermark, fires by state, single / late / low-confidence / duplicate counts, rebuild time |
| `GET /internal/wildfire/fires` | Every current fire and a digest of them |
| `GET /internal/wildfire/late?limit=&after_offset=` | Late detections |

Metrics on `:8086/metrics`: `wildfire_lag_events`, `wildfire_watermark_lag_seconds`,
`wildfire_feed_latest_offset`, `wildfire_processed_offset`, `wildfire_detections{state}`,
`wildfire_fires{state}`, `wildfire_feed_reconnects_total{reason}`, `wildfire_offset_gaps`,
`wildfire_rebuild_seconds`, `wildfire_restored_offset`, `wildfire_snapshot_offset`,
`wildfire_snapshot_bytes`, `wildfire_emit_delay_seconds`, `wildfire_epoch`,
`wildfire_feed_session_changes_total{reason}`. End-to-end latency (feed arrival until
stored) is the aggregator's `hazard_store_latency_seconds{source="SATELIT"}`.

```bash
# Run the engine over a recorded feed and compare with an O(n^2) recomputation
go run ./services/wildfire-processor/cmd/wildfire-check -in events.jsonl -out fires.json -snapshot
# On the cluster: kill the processor, let the feed run ahead, compare with stored events
scripts/verify-wildfire.sh
```

**Writing HazardEvents from other services (`hazard-upserts`).** Polled sources (BMKG, PVMBG)
are stored directly; every other producer (wildfire-processor, coastal-ingester, dataset loader)
sends messages to `bnpb.hazard-upserts.v1` (6 partitions, 7-day retention), which all aggregator
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
- `hazard_store_latency_seconds{source}` measures `produced_at` → stored.

### Problem 5: unreliable coastal sensor

Sensor Pantai is touched by exactly one component, `coastal-ingester`. Everything else,
client-api included, reads coastal data from storage, so a broken sensor cannot slow down client
reads or the other sources.

- **One client.** coastal-ingester runs two replicas; only the holder of Lease
  `ingest-sensor-pantai` polls the sensor (same `pkg/lease` as Problem 2). It polls
  `GET /v1/observations` every second, 200 observations per page, and reads up to 20 pages in a
  row while pages come back full (backlog).
- **Resilience layers** (failsafe-go, outermost first): bulkhead → retry → circuit breaker →
  timeout. Retries never run while the breaker is open or the bulkhead is full, and only for
  transport errors, 5xx and 429.

  | Mechanism | Value | Variable |
  |---|---|---|
  | Timeout | 2 s per attempt (normal answer ≈ 0.13 s) | `SENSOR_REQUEST_TIMEOUT_MS=2000` |
  | Retry | 2 retries, exponential backoff 200 ms–2 s, 30% jitter | `SENSOR_MAX_RETRIES=2` |
  | Circuit breaker | opens after 5 failures in a row, stays open 10 s, 1 half-open trial | `SENSOR_BREAKER_FAILURES=5`, `SENSOR_BREAKER_OPEN_MS=10000` |
  | Bulkhead | at most 2 requests to the sensor at once | `SENSOR_MAX_CONCURRENCY=2` |
  | Stale detection | `generated_at` stuck for 15 s, or a caught-up page more than 900 s (sensor time) behind `generated_at` | `SENSOR_STUCK_AFTER_MS=15000`, `SENSOR_STALE_AFTER_SECONDS=900` |

- **Freshness by the sensor's own clock.** The sensor's virtual clock runs 60× faster than real
  time, so freshness is judged with `generated_at`, never with the node clock. A stale answer
  counts as a failure for the breaker, so a sensor that "looks healthy" is treated like a dead
  one. When `generated_at` goes back the sensor restarted: the cursor is reset and duplicates are
  dropped by `observation_id`.
- **Contract validation.** Every observation is checked for required fields, types, known station
  (`GET /v1/stations`), sane ranges (|`sea_level_anomaly_m`| ≤ 20, 0 ≤ `wave_height_m` ≤ 30) and
  `quality` (`ok`, `suspect`, `bad`). Violations are not dropped: the original observation goes
  to `bnpb.sensor-rejected.v1` with a reason (`missing_field`, `wrong_type`, `unknown_station`,
  `out_of_range`, `unknown_quality`), and never becomes a HazardEvent.
- **Critical path.** In every page, `critical=true` observations are sent first to their own
  topic `bnpb.critical-alerts.v1` with `acks=all` and the header `X-Ingested-At`, so they never
  wait behind ordinary data. A critical observation that fails validation is still sent, marked
  `verified=false`, and also set aside. `field-receiver` reads the topic in its own group, commits
  only after processing, drops duplicates by `observation_id` and records the latency.
- **COASTAL events.** Valid observations become one `COASTAL` HazardEvent per station through
  `hazard-upserts`, versioned by `observed_at` in ms. Severity (group rule): AWAS if critical,
  |anomaly| ≥ 1.0 m or wave ≥ 4 m; SIAGA ≥ 0.5 m or ≥ 2.5 m; WASPADA ≥ 0.3 m or ≥ 1.5 m;
  otherwise NORMAL.
- **Fast answers.** client-api marks a COASTAL event not updated for 30 s with `"stale": true`
  and `"as_of"` (`COASTAL_STALE_AFTER_SECONDS`), so clients get the last reading at once.
- **Load shedding per role.** Media may hold at most 40 concurrent `GET /hazards` requests
  (`CLIENT_API_MEDIA_MAX_CONCURRENCY`); above that Media gets 429 with `Retry-After: 1` at once,
  while Tim Lapangan and Internal Ops keep being served. The global limit is 200 per instance.

**Reliability target.** SLI: share of critical alerts received by field-receiver within 5 s of
being read by the ingester. SLO: 99% per hour within 5 s, and 100% within 60 s. Error budget:
1% per hour may arrive later than 5 s.

```bash
NS=sistem-informasi-kebencanaan-bnpb
kubectl -n $NS exec deploy/field-receiver -- wget -qO- http://127.0.0.1:8089/alerts   # alerts received and their latency
kubectl -n $NS exec deploy/coastal-ingester -- wget -qO- http://127.0.0.1:8088/status # breaker, last success, failures
kubectl -n $NS scale deploy/sensor-pantai --replicas=0      # breaker opens, client-api answers stale at once
kubectl -n $NS scale deploy/sensor-pantai --replicas=1      # recovers without intervention
k6 run loadtest/m2/p5-shedding.js                           # Media gets 429, Tim Lapangan stays at 200
```

`/alerts` lists what the running field-receiver pod has received since it started; counts across
restarts come from Prometheus (`critical_alert_received_total`).

### Observability

| Tool | Where | What |
|---|---|---|
| Grafana | `$GW/grafana/` | Dashboards "BNPB Overview", "BNPB Storage (P1)", "BNPB Reliability (P2 & P5)", "BNPB Streams (P3, field reports, P4)", provisioned from `deploy/k8s/base/observability/*.json` |
| Prometheus | `$GW/prometheus/` | Scrapes every pod with the `prometheus.io/scrape` annotation (job `bnpb-pods`) |
| Jaeger | `$GW/jaeger/` | Traces over OTLP; one request or poll cycle can be followed across HTTP calls and Kafka messages |

Main metrics per problem:

| Problem | Metrics |
|---|---|
| P1 | `store_unit_up`, `store_docs`, `store_ops_total`, `store_partial_reads_total` |
| P2 | `leader_is_leader{lock,pod}`, `leader_epoch{lock}`, `http_client_requests_total{app="aggregator",upstream}`, `poller_fenced_writes_rejected_total{source}` |
| P3 | `aggregator_dlq_events_total{source,outcome}`, `aggregator_dlq_messages{source,status}` (use `max`), `field_reports_received_total{result}`, `field_report_kafka_ack_seconds`, `field_reports_stored_total{result}` |
| P4 | `wildfire_lag_events`, `wildfire_watermark_lag_seconds`, `wildfire_offset_gaps`, `hazard_store_latency_seconds{source="SATELIT"}` |
| P5 | `sensor_breaker_state`, `sensor_polls_total{outcome}`, `sensor_rejected_total{reason}`, `sensor_critical_published_total`, `critical_alert_received_total`, `critical_alert_duplicates_total`, `critical_alert_latency_seconds`, `client_api_shed_total{scope}` |

## Milestone 1 Test Scenarios

These run on the Docker Compose stack from [Docker Compose (Milestone 1 services)](#docker-compose-milestone-1-services).

<details>
<summary>Milestone 1 scenarios (Docker Compose)</summary>

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

### M1 Problem 1: schema change at runtime

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

### M1 Problem 2: slow source, load, and outage

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

### M1 Problem 3: authentication and trust

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

### M1 Problem 4: independent services and flexible storage

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

### M1 Problem 5: fan-out through Kafka

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

</details>

## Configuration

### Ports

Inside the cluster every service is reached through its Service name and the port below; from
outside, only Traefik (port 80) is exposed. With Docker Compose, `*_HOST_PORT` is where a service
appears on your machine; to fix a port clash, change only `*_HOST_PORT`.

| Service | Port | Compose host port (variable) |
| --- | --- | --- |
| client-api | 8080 | 8080 (`CLIENT_API_HOST_PORT`) |
| bmkg-mock | 8081 | 8081 (`BMKG_HOST_PORT`) |
| pvmbg-mock | 8082 | 8082 (`PVMBG_HOST_PORT`) |
| aggregator | 8083 | not published |
| auth-service | 8084 | 8084 (`AUTH_HOST_PORT`) |
| consumer-dashboard | 8085 | 8085 (`CONSUMER_DASHBOARD_HOST_PORT`) |
| field-report-api | 8085 | Kubernetes only |
| consumer-notifier | 8086 | 8086 (`CONSUMER_NOTIFIER_HOST_PORT`) |
| wildfire-processor | 8086 | Kubernetes only |
| consumer-regional | 8087 | 8087 (`CONSUMER_REGIONAL_HOST_PORT`) |
| coastal-ingester | 8088 | Kubernetes only |
| field-receiver | 8089 | Kubernetes only |
| sensor-pantai | 8080 | Kubernetes only |
| kafka | 9092 (9093 controller) | not published |
| mongos, shard members, config servers | 27017 | not published |
| grafana, jaeger, prometheus | 3000, 16686, 9090 | Kubernetes only, through Traefik |
| kafka-ui | 8080 | 8090 (`KAFKA_UI_PORT`), Compose only |

### Environment Variables

Secrets and per-environment values live in `.env`, with example values in
[`.env.example`](.env.example); `scripts/deploy.sh` turns them into the Secret `bnpb-secrets`.
Other settings live in each service's ConfigMap under `deploy/k8s/base/<service>/kustomization.yaml`,
so they can change without rebuilding an image. `.env` and `infra/keys/` are git-ignored.

**Cluster and secrets (Milestone 2)**

| Variable | Where | Description |
| --- | --- | --- |
| `MONGO_SLOT_NODES` | `.env` | Node for Mongo slots a b c d, in that order |
| `MONGO_MONITOR_USERNAME`, `MONGO_MONITOR_PASSWORD` | `.env` | User the aggregator's storage monitor connects with |
| `PANTAI_AUTH_TOKEN` | `.env` → Secret `pantai-token` | Shared token of Sensor Pantai and coastal-ingester |
| `SATELLITE_TOKEN` | `.env` | Feed Satelit token |
| `AUDIT_TOKENS` | `.env` | Audit API tokens, comma separated |
| `CLIENT_ANALYTICS_ID`, `CLIENT_ANALYTICS_SECRET` | `.env` | `analytics-viewer` client |
| `GRAFANA_ADMIN_PASSWORD` | `.env` | Grafana admin password |
| `KAFKA_TOPICS`, `KAFKA_REPLICATION_FACTOR` | Kafka ConfigMap | Topics as `name:partitions:retention_ms`, created with RF 3 |

**Aggregator (Milestone 2 additions)**

| Variable | Default | Description |
| --- | --- | --- |
| `LEASE_DURATION_SECONDS`, `LEASE_RENEW_DEADLINE_SECONDS`, `LEASE_RETRY_PERIOD_SECONDS` | `15`, `10`, `2` | Leader lease timing |
| `KAFKA_UPSERTS_TOPIC`, `KAFKA_UPSERTS_GROUP` | `bnpb.hazard-upserts.v1`, `bnpb.aggregator.hazard-upserts` | Upserts from other producers |
| `STORE_PARTIAL_MODE` | `literal` | When reads are marked `partial` (see Problem 1) |
| `AUDIT_INCLUDE_RETRACTED` | `false` | Whether retracted events count in the Audit API and analytics |
| `STORE_EXPERIMENTS` | `off` | Copy writes into the shard-key experiment collections |

**coastal-ingester**

| Variable | Value | Description |
| --- | --- | --- |
| `SENSOR_BASE_URL` | `http://sensor-pantai:8080` | Sensor Pantai address |
| `SENSOR_POLL_INTERVAL_MS`, `SENSOR_PAGE_LIMIT` | `1000`, `200` | Poll interval and page size |
| `SENSOR_REQUEST_TIMEOUT_MS`, `SENSOR_MAX_RETRIES` | `2000`, `2` | Timeout per attempt, retries with jitter |
| `SENSOR_BREAKER_FAILURES`, `SENSOR_BREAKER_OPEN_MS` | `5`, `10000` | Circuit breaker |
| `SENSOR_MAX_CONCURRENCY` | `2` | Bulkhead |
| `SENSOR_STUCK_AFTER_MS`, `SENSOR_STALE_AFTER_SECONDS` | `15000`, `900` | Stale detection (real time, sensor time) |
| `KAFKA_CRITICAL_TOPIC`, `KAFKA_UPSERTS_TOPIC`, `KAFKA_REJECTED_TOPIC` | `bnpb.critical-alerts.v1`, `bnpb.hazard-upserts.v1`, `bnpb.sensor-rejected.v1` | Output topics |

**client-api (Milestone 2 additions)**

| Variable | Value | Description |
| --- | --- | --- |
| `CLIENT_API_MEDIA_MAX_CONCURRENCY` | `40` | Concurrent Media requests on `GET /hazards`; above it Media gets 429 |
| `COASTAL_STALE_AFTER_SECONDS` | `30` | COASTAL events older than this get `stale: true` and `as_of` |

**field-report-api, field-receiver, wildfire-processor**

| Variable | Value | Description |
| --- | --- | --- |
| `FIELD_REPORTS_TOPIC`, `PRODUCE_TIMEOUT_MS` | `bnpb.field-reports.v1`, `3000` | Field report topic; 503 when Kafka does not ack in time |
| `KAFKA_CRITICAL_TOPIC` (field-receiver) | `bnpb.critical-alerts.v1` | Critical alerts topic |
| `SATELLITE_BASE_URL` | assistants' feed | Feed Satelit address |
| `ALLOWED_LATENESS_MINUTES` | `360` | Watermark lateness |
| `FLUSH_INTERVAL_MS`, `FLUSH_EVENTS` | `500`, `500` | Micro-batch to Kafka |
| `FEED_IDLE_TIMEOUT_SECONDS` | `45` | Reconnect when the stream is silent |
| `SNAPSHOT_TOPIC`, `SNAPSHOT_EVERY_SECONDS` | `bnpb.wildfire-snapshots.v1`, `60` | State snapshots |
| `VERSION_EPOCH` | `0` | Can only raise the version epoch |

<details>
<summary>Milestone 1 variables</summary>

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
| `BMKG_EVENT_INTERVAL_SECONDS` | `10` | How often a new event appears (`0`: quiet mode) |

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
| `BMKG_BASE_URL`, `PVMBG_BASE_URL` | | Source addresses |
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
| `AGGREGATOR_BASE_URL` | | Aggregator address |
| `AUTH_JWKS_URL` | | Where the public key is fetched from |
| `STALE_THRESHOLD_SECONDS` | `30` | When PVMBG data counts as stale (`stale_since`) |
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

</details>

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
  flag to the configured values and generates records with new IDs.

Sensor Pantai and Feed Satelit are provided by the assistants; see their contracts in the
assignment spec.

### Auth service

| Endpoint | Body | Returns |
| --- | --- | --- |
| `POST /login` | `{"username": "...", "password": "..."}` | `access_token`, `refresh_token` |
| `POST /refresh` | `{"refresh_token": "..."}` | A new pair. The old refresh token is revoked. |
| `GET /.well-known/jwks.json` | | Public key for verifying tokens |

### Client API

`GET /hazards[?type=SEISMIC|VOLCANIC|WILDFIRE|COASTAL][&since=<RFC 3339>][&fields=<a,b>][&limit=<n>][&cursor=<c>]`
with `Authorization: Bearer <access_token>`. Without `type` it returns the newest events of every
type across all shards. Results are sorted newest first, 100 per page by default (`limit` up to
500). When more results exist, the response carries an `X-Next-Cursor` header; pass its value as
`cursor` to get the next page. When part of the storage is unreachable the response carries
`X-Partial: true` and `X-Unavailable-Units`.

| Client | Credentials | Scope | Fields returned |
| --- | --- | --- | --- |
| Media / press | `CLIENT_MEDIA_*` | `media` | Summary only: `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at` |
| Field team | `CLIENT_FIELD_*` | `field` | All fields |
| BNPB internal ops | `CLIENT_OPS_*` | `internal` | All fields |
| Analytics viewer | `CLIENT_ANALYTICS_*` | `analytics` | None from client-api (403); analytics dashboard only |

- A `media` token that asks for a raw field (`source_ref_id`, `latitude`, `longitude`,
  `attributes`) through `fields` is refused with 403. For other scopes `fields` has no effect.
- While PVMBG cannot be reached, volcanic events carry `stale_since`: the last time the
  aggregator polled PVMBG successfully.
- COASTAL events not updated for 30 s carry `"stale": true` and `"as_of"`.
- Overload: Media gets 429 with `Retry-After: 1` above 40 concurrent requests; every client gets
  429 above 200 concurrent requests per instance. Requests are never queued.

### Field Report API

| Endpoint | Credential | Returns |
| --- | --- | --- |
| `POST /field-reports` with `Idempotency-Key` | `field` or `internal` JWT | `202` after Kafka `acks=all`, `503` if Kafka does not answer in 3 s |
| `GET /field-reports/{report_id}` | `field` or `internal` JWT | `pending` or `processed` |

### Aggregator (through the ingress)

| Endpoint | Credential |
| --- | --- |
| `GET /internal/audit/events`, `/checksum`, `/location`, `/unprocessed` | audit token |
| `GET /internal/ops/unprocessed[/{message_id}]`, `POST /internal/ops/replay`, `POST /internal/ops/retract` | audit token or `internal` JWT |
| `GET /analytics` (page), `GET /analytics/data` | `analytics` or `internal` JWT |

### Wildfire, coastal and field-receiver

| Service | Endpoint | Returns |
| --- | --- | --- |
| wildfire-processor | `GET /internal/wildfire/stats`, `/fires`, `/late` | Stream state, current fires, late detections (audit token or `internal` JWT) |
| coastal-ingester | `GET /status` (inside the cluster) | Breaker state, last success, failure counts |
| field-receiver | `GET /alerts` (inside the cluster) | Critical alerts received by this pod, with latency |

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
├── deploy/k8s/
│   ├── base/                     # One Kustomize folder per service, plus kafka, mongodb,
│   │                             # sensor-pantai, observability (Prometheus, Grafana dashboards, Jaeger) and ingress
│   ├── components/hpa/           # HorizontalPodAutoscaler for aggregator and client-api
│   ├── overlays/dev/             # k3d, local images, smaller MongoDB
│   ├── overlays/demo/            # VM cluster, GHCR images, HPA on
│   └── jobs/loader.yaml          # Earthquake catalog loader Job
│
├── infra/
│   ├── k3s/                      # install-server.sh and install-agent.sh
│   ├── broker/, store/           # Kafka topic and MongoDB init for Docker Compose
│   ├── keys/                     # JWT signing key and Mongo keyfile (generated, git-ignored)
│   └── docker-compose.yml        # Milestone 1 services on one machine
│
├── pkg/
│   ├── correlation/              # Correlation ID helpers
│   ├── envx/                     # Environment variable helpers
│   ├── fieldreport/              # Field report message type
│   ├── geo/                      # Distance and province lookup
│   ├── hazard/                   # HazardEvent type, hazard_id generator, upsert messages
│   ├── httpx/                    # HTTP server and client with request and latency logging
│   ├── jwtauth/                  # JWT verification with clock leeway
│   ├── kafkax/                   # Kafka producer and consumer loop, duplicate filter
│   ├── lease/                    # Kubernetes Lease leader election with fencing epoch
│   ├── logging/                  # JSON logger
│   ├── satfeed/                  # Feed Satelit SSE reader
│   └── telemetry/                # Prometheus metrics and OpenTelemetry tracing
│
├── services/
│   ├── bmkg-mock/                # Simulated BMKG
│   ├── pvmbg-mock/               # Simulated PVMBG (schema versions, outage, corruption)
│   ├── aggregator/               # Polling, mapping, sharded store, Audit API, analytics, DLQ, upserts consumer
│   │   └── cmd/                  # loader (catalog) and verify-analytics
│   ├── auth-service/             # Login, refresh, JWKS
│   ├── client-api/               # Client API: scopes, stale marking, load shedding
│   ├── field-report-api/         # POST /field-reports for Tim Lapangan
│   ├── wildfire-processor/       # Feed Satelit stream to wildfire HazardEvents
│   │   └── cmd/wildfire-check/   # Brute-force comparison over a recorded feed
│   ├── coastal-ingester/         # The only Sensor Pantai client
│   ├── field-receiver/           # Critical alert receiver for Tim Lapangan
│   ├── consumer-dashboard/       # Kafka consumer: dashboard summary
│   ├── consumer-notifier/        # Kafka consumer: SIAGA and AWAS notifications
│   └── consumer-regional/        # Kafka consumer: regional feed
│
├── scripts/
│   ├── deploy.sh                 # One-command deploy (dev or demo)
│   ├── label-nodes.sh            # Mongo slot labels
│   ├── gen-keys.sh, gen-mongo-keyfile.sh, gen-mongo-manifests.sh
│   ├── dev-mongo-sharded.sh      # Small sharded cluster in Docker for development
│   ├── p1-distribution.sh        # P1 shard-key comparison
│   ├── verify-p2.sh              # M1 Problem 2 checks (latency, load, outage)
│   ├── verify-field-reports.sh   # P3 field report delivery check
│   ├── verify-wildfire.sh        # P4 check on the cluster
│   ├── verify-criteria.sh        # M1 Problem 3 checks
│   └── health.sh                 # /health of every Compose service
│
├── loadtest/
│   ├── m2/                       # k6: p2-sustained, p2-throughput, p2-chaos.sh, p5-shedding
│   └── loadtest.js               # Milestone 1 k6 script and results
├── evidence/                     # Reports written by the verify scripts
├── .github/workflows/images.yml  # Builds and pushes every image to GHCR
├── .env.example                  # Every configuration variable with an example value
├── go.mod
└── README.md
</pre>

## Contributors

<div align="center">

| Name | NIM | Milestone 2 responsibilities |
| :--- | :---: | :--- |
| Mayla Yaffa Ludmilla | 13523050 | P3: PVMBG schema and corruption modes, dead letter queue and replay, field-report-api. P4: Feed Satelit reader, wildfire-processor, snapshot and watermark. Grafana for P3 and P4 |
| Noumisyifa Nabila Nareswari | 13523058 | P1: MongoDB sharded cluster, versioned store and fencing `CommitPoll`, health monitor and partial reads, `hazard-upserts` consumer, Audit API, catalog loader and shard-key experiments, analytics dashboard and `analytics-viewer`, Grafana Storage |
| Muhammad Edo Raduputu Aprima | 13523096 | Platform: Kubernetes migration, Traefik ingress, one-command deploy, k3s setup, Kafka 3 brokers. P2: leader election and aggregator readiness/shutdown. P5: coastal-ingester, critical alert path, field-receiver, client-api load shedding. Prometheus, Grafana, Jaeger, OpenTelemetry, HPA, image workflow, load test scripts |

</div>
