<div align="center">

<img src="docs/assets/seath-logo.png" alt="Seath logo" width="160">

# Seath

**Record live NATS streams into PostgreSQL — and explore them from your browser.**

*Named after Seath the Scaleless, the archivist dragon: it watches the streams and preserves them.*

[![CI](https://github.com/amirerfantim/seath/actions/workflows/ci.yml/badge.svg)](https://github.com/amirerfantim/seath/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
![Go 1.25+](https://img.shields.io/badge/go-1.25%2B-00ADD8.svg)

</div>

---

## 🤔 What is Seath?

Imagine your backend services are constantly chatting over **NATS** (a messaging
system), sending each other tiny binary messages (**Protocol Buffers**). Normally
those messages fly by and disappear. **Seath quietly listens in, decodes them,
and saves them into a PostgreSQL database** — so you can look at them later, query
them with SQL, and export them.

You drive the whole thing from a clean web console. No code, no rebuilds.

```
   NATS topic  ──▶  Seath  ──▶  PostgreSQL table  ──▶  your SQL / CSV / charts
  (live proto)     (decode +      (one tidy table
                    sample)        per recording)
```

---

## ✨ What can it do?

| | Feature | In plain words |
|---|---|---|
| 🎬 | **Recordings** | Pick a topic, hit start. Seath decodes each message and writes typed rows into its own table. Run many at once. |
| 🎚️ | **Live sampling** | Combine a traffic percentage with any number of field filters: exact integer, boolean, string, or enum values; inclusive integer modulo ranges (`user_id % 100` from `45` to `65`); and float comparisons (`score > 0.8`). Every filter must match, and the rule can change **while recording**, without a restart. |
| 🧹 | **Retention & indexes** | Every recording auto-deletes rows older than N hours (mandatory, 1..`MAX_RETENTION_HOURS`, default 24); add indexes on the columns you filter by. |
| 🔎 | **Tap** | Just want to know if *anything* is flowing on a subject? Tap it and watch — even without knowing its schema. |
| 🗄️ | **Table browser + SQL** | Browse rows, run bounded **read-only** SQL, export a page/query or stream the whole table as CSV. |
| 🧩 | **Dynamic proto registry** | Upload versioned protobuf descriptor bundles, activate them without rebuilding Seath, and map any NATS topic with a searchable picker. Modules, versions, descriptors, and mappings all live in Postgres, so each Seath instance can own a different catalogue. |
| 🌐 | **NATS environments** | Switch between DB-configured clusters with a click. Credentials never leave the server. |
| 🔐 | **Auth & audit** | Cookie login, `admin` / `viewer` roles, and a log of who changed what. Viewers browse and export; they get the SQL console only with a separate read-only login (`QUERY_PG_DSN`). |
| ❤️ | **Health & backpressure** | `/healthz` + `/readyz` probes, and a visible "dropped" counter if it ever can't keep up. |

---

## 🗺️ How it works

![Seath data flow: NATS topic → subscribe → sample → decode → batch writer → PostgreSQL, with the web console controlling recordings and reading back via SQL](docs/diagrams/overview.svg)

<details><summary>Diagram source (Mermaid)</summary>

```mermaid
flowchart LR
    NATS["NATS topic<br/>(proto messages)"]
    subgraph seath["Seath"]
        SUB["subscribe"]
        SAMPLE["sample<br/>(percentage AND field filters)"]
        DECODE["decode proto<br/>to table row"]
        BATCH["batch writer"]
    end
    PG[("PostgreSQL<br/>one table per recording")]
    UI["Web console<br/>+ read-only SQL"]

    NATS --> SUB --> SAMPLE --> DECODE --> BATCH --> PG
    UI -->|"create / start / stop"| seath
    PG -->|"browse / query / export"| UI
```

</details>

Three things run side by side:

- **Recordings** — the main job: subscribe → sample → decode → batch-insert.
- **Taps** — throwaway probes that just count traffic and preview raw payloads.
- **The console** — the web UI + REST API you use to control everything.

> 🧠 Seath remembers what it was doing. Recording definitions, counters, and
> "should this be running?" all live in Postgres, so a restart picks up exactly
> where it left off.

---

## 🚀 Quick start

### Try the demo (Docker Compose) 🧪

The compose file starts Seath, PostgreSQL and a NATS server on one private
network. It is a demo: the passwords in it are examples.

```bash
docker compose up -d --build       # set SEATH_PORT if port 8080 is taken
docker compose exec seath seath user add admin --role admin --password demo-password
```

Open <http://localhost:8080> and sign in as `admin`. The steps below do the
same work as the console, but through the API, so you can copy them. They need
`protoc` and `curl`.

```bash
B=http://localhost:8080
curl -s -c jar -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"demo-password"}' $B/api/login

# 1. Tell Seath where NATS is.
curl -s -b jar -H 'Content-Type: application/json' \
  -d '{"name":"demo","url":"nats://nats:4222"}' $B/api/presets

# 2. Load a protobuf schema and activate it.
protoc -I examples --include_imports --descriptor_set_out=event.protoset examples/event.proto
curl -s -b jar -F module_path=examples/event.proto -F version=v1.0.0 \
  -F descriptor_set=@event.protoset $B/api/proto/modules
curl -s -b jar -X PUT $B/api/proto/modules/1/activate

# 3. Map a topic to the message type.
curl -s -b jar -H 'Content-Type: application/json' \
  -d '{"topic":"demo.events","proto_name":"example.events.Event"}' $B/api/topic-mappings

# 4. Create a recording, then start it with the "id" from the answer.
curl -s -b jar -H 'Content-Type: application/json' \
  -d '{"topic":"demo.events","table":"demo_events","sample_mode":"rate","sample_rate":100,"nats_env":"demo","retention_hours":24}' \
  $B/api/recordings
curl -s -b jar -X POST $B/api/recordings/<id>/start

# 5. Publish a message, then read the row back.
printf 'event_id: 7\nvalue: "hello"\nenabled: true\n' \
  | protoc -I examples --encode=example.events.Event examples/event.proto \
  | docker compose run --rm -T nats-box nats pub -s nats://nats:4222 demo.events --force-stdin
curl -s -b jar "$B/api/recordings/<id>/rows?limit=5"
```

Stop the demo and delete its data with `docker compose down -v`.

### Run it locally

```bash
# build the single static binary
go build -o seath ./cmd/main.go

# create a login, then start the console
PG_DSN="postgres://user:pass@host:5432/db?sslmode=disable" ./seath user add admin --role admin
PG_DSN="postgres://user:pass@host:5432/db?sslmode=disable" ./seath serve
# open http://localhost:8080
```

On first boot, open **Registry** and import at least one protobuf
`FileDescriptorSet`. Generate the bundle in the proto repository or its CI:

```bash
protoc -I ./pb --include_imports \
  --descriptor_set_out=events-v1.0.0.protoset \
  $(find ./pb -name '*.proto')
```

Stage the bundle with its repository path and version, then activate it. Staging
does not affect live decoding. Activation atomically replaces the active version
for new recordings; existing recordings keep the immutable module IDs and proto
type captured when they were created. Proto source, Git credentials, and
`protoc` are never needed inside the Seath container.

### Run it with Docker 🐳

```bash
docker build -t seath:latest .
docker run -p 8080:8080 -e PG_DSN="postgres://user:pass@host:5432/db?sslmode=disable" seath:latest
```

---

## 🧑‍💻 Commands

```bash
seath serve                      # 🖥️  run the web console (default :8080, or --addr / HTTP_ADDR)
seath start                      # 🎬  record one topic headlessly from config
seath schema --list              # 📜  list every known topic
seath schema --topic <topic>     # 🔧  print the proto → column mapping for a topic
seath user add <name> [--role admin|viewer]   # 👤 add/update a login
seath user list                  # 👥  list users
seath user rm <name>             # 🗑️  remove a user
```

---

## ⚙️ Configuration

Seath reads `/etc/seath/config.yaml` (override the path with `CONFIG_PATH`). See
the example: [`configs/config.yaml`](configs/config.yaml).

Environment variables override the file — handy in containers:

| Variable | What it sets |
|---|---|
| `PG_DSN` | PostgreSQL connection string **(the only thing `serve` needs)** |
| `NATS_ADDRESS` | NATS URL (for the headless `start` command) |
| `TOPIC` / `TABLE` / `SAMPLE_RATE` | single-recording settings for `start` |
| `HTTP_ADDR` | console listen address (default `:8080`) |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `MAX_RETENTION_HOURS` | ceiling on a recording's retention window (default `24`, minimum `1`) |
| `QUERY_TIMEOUT_SECONDS` | timeout for read-only SQL queries and full-table CSV exports (default `90`, minimum `1`) |
| `QUERY_MAX_ROWS` | maximum rows returned by interactive SQL (default `15000`, minimum `1`; full-table CSV export remains uncapped) |
| `QUERY_PG_DSN` | separate read-only login for the SQL console. Without it, only admins can run SQL. See [deployment](docs/deployment.md#sql-console-and-secrets) |
| `SEATH_SECRET_KEY` | base64 32-byte key (`openssl rand -base64 32`) that encrypts NATS environment passwords and tokens in the database |
| `RETENTION_HOURS` | retention window for the headless `start` command (defaults to the ceiling) |

---

## 🏗️ Project layout

```
cmd/                 🚪 CLI entrypoint (serve / start / schema / user)
internal/
  recorder/          🎬 per-recording engine: subscribe, sample, decode, batch-write
  manager/           🧠 owns recorders, taps, presets, retention; persistence + loops
  httpapi/           🌐 REST API + embedded web UI
  pg/                🗄️ PostgreSQL helpers (DDL, batch insert, stats, queries)
  schema/            🔧 proto → column introspection & value extraction
  prototypes/        🧩 DB-backed descriptor loading + dynamic messages
  natspreset/        🌍 NATS environment presets
  auth/              🔐 cookie auth, roles, audit
  config/            ⚙️ configuration loading
configs/             📄 example configuration
```

> 🔭 Want the full tour — every endpoint, every file and the data flow? Read
> **[`docs/architecture.md`](docs/architecture.md)**. What is planned next is in
> **[`docs/roadmap.md`](docs/roadmap.md)**.

---

## 🧪 Development

You need Go 1.25 or newer (see `go.mod`). CI runs these checks
([`ci.yml`](.github/workflows/ci.yml)):

```bash
test -z "$(gofmt -l cmd internal)"      # formatting
go vet ./...
go test -race ./...
python3 -m unittest discover -s tools   # bootstrap helpers
go build -trimpath -buildvcs=false -o /tmp/seath ./cmd
docker build -t seath:test .
```

---

## ☸️ Deployment

Seath runs as a single container. Mount a non-secret configuration file at
`/etc/seath/config.yaml` and supply `PG_DSN` from your secret manager. See
[deployment and bootstrap](docs/deployment.md) for configuration precedence,
instance manifests, CI settings, and release packaging.

> ⚠️ **Run exactly one replica.** Seath is a single writer — two replicas would
> record the same topics twice. Use `replicas: 1` and a `Recreate` rollout
> strategy in Kubernetes.

---

## 📜 Licensing & attribution

Seath is licensed under the [MIT License](LICENSE).

Seath embeds the **Inter** and **JetBrains Mono** fonts, both under the SIL Open
Font License 1.1. Their copyright notices and licence texts ship beside the font
files in [`internal/httpapi/web/fonts/`](internal/httpapi/web/fonts/) and are
summarised in [`NOTICE`](NOTICE).

---

<div align="center">

Made with 🐉 by the Seath contributors

</div>
