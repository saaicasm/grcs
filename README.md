# grcs — logging/event pipeline demo

A single-endpoint demo of a decoupled logging pipeline. One HTTP endpoint accepts
events and fans them out to an operational store and an analytics store over two
independent paths:

```
POST /events ──┬── ScyllaDB        (operational store: point reads/writes)
   (Go API)    │
               └── Redpanda topic ──► consumer ──► ClickHouse (analytics store)
```

- **ScyllaDB** — operational DB, wide-column model, fast key-based access.
- **Redpanda** — Kafka-compatible log that decouples the write path from analytics.
- **ClickHouse** — columnar analytics DB for aggregations over the event stream.
- **Docker Compose** — orchestrates all three services.

**The point:** the API write path (Scylla + Redpanda) is decoupled from the
analytics path (ClickHouse). If ClickHouse is slow or down, the API keeps
accepting events — they buffer on the Redpanda log and drain when the consumer
catches up.

## Layout

```
grcs/
├── docker-compose.yml      # Scylla + Redpanda + ClickHouse
├── go.mod / go.sum
├── init/
│   ├── scylla.cql          # keyspace + events table
│   ├── clickhouse.sql      # analytics table
│   └── clickhouse-users.xml # opens the default user to the host (demo-only)
├── internal/
│   └── event/event.go      # shared Event struct (api + consumer)
└── cmd/
    ├── api/main.go         # POST /events → Scylla + Redpanda
    └── consumer/main.go    # Redpanda → ClickHouse
```

## Prerequisites

- Docker + Docker Compose
- Go 1.24+

## Run it

### 1. Start the services

```bash
docker compose up -d
```

Wait until all three report `(healthy)` — Scylla takes ~30s:

```bash
docker compose ps
```

### 2. Create the topic and apply the schemas

Redpanda ships with auto-topic-creation disabled, so create the `events` topic
explicitly, then apply the two schemas (the init scripts are mounted into the
containers but not run automatically). Do this once the services are healthy:

```bash
# Redpanda topic the api produces to / the consumer reads from
docker compose exec -T redpanda rpk topic create events

# Scylla keyspace + table, ClickHouse database + table
docker compose exec -T scylla     cqlsh                          < init/scylla.cql
docker compose exec -T clickhouse clickhouse-client --multiquery < init/clickhouse.sql
```

Verify:

```bash
docker compose exec -T redpanda   rpk topic list
docker compose exec -T scylla     cqlsh -e "DESCRIBE TABLE grcs.events;"
docker compose exec -T clickhouse clickhouse-client -q "SHOW CREATE TABLE grcs.events;"
```

### 3. Run the API and the consumer

In two separate terminals:

```bash
# terminal A — HTTP API (writes Scylla + produces to Redpanda)
go run ./cmd/api
# expect: api listening on :8080 ...
```

```bash
# terminal B — consumer (Redpanda → ClickHouse)
go run ./cmd/consumer
# expect: consumer started (topic=events ...)
```

### 4. Send events

```bash
# a valid event → 202 Accepted
curl -s -XPOST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","type":"page_view","source":"web","payload":{"path":"/home"}}'
echo

# missing user_id → 400 Bad Request
curl -s -XPOST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"click"}'
echo

# a few more to trigger a batch flush in the consumer
for i in $(seq 1 5); do
  curl -s -XPOST localhost:8080/events \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"u1\",\"type\":\"click\",\"source\":\"web\",\"payload\":{\"n\":$i}}" >/dev/null
done
```

The first call returns `{"id":"...","status":"accepted"}`. Watch terminal B —
within ~1s you should see `flushed N events to clickhouse`.

### 5. Verify both stores

```bash
# operational path — Scylla, newest-first for a user
docker compose exec -T scylla cqlsh -e \
  "SELECT user_id, timestamp, type, source, payload FROM grcs.events WHERE user_id='u1';"

# analytics path — ClickHouse, aggregated by type
docker compose exec -T clickhouse clickhouse-client -q \
  "SELECT type, count() AS n FROM grcs.events GROUP BY type ORDER BY n DESC;"
```

The same events appear in both stores via their two independent paths.

### Teardown

```bash
docker compose down       # stop, keep data
docker compose down -v    # stop and wipe the volumes
```

## API

### `POST /events`

Request body (`user_id` and `type` are required; the rest are optional):

```json
{
  "id":        "optional; server generates a UUID if omitted",
  "user_id":   "u1",
  "type":      "page_view",
  "source":    "web",
  "timestamp": "optional RFC3339; defaults to receipt time (UTC)",
  "payload":   { "any": "json object" }
}
```

Responses:

| Status | Meaning |
|--------|---------|
| `202 Accepted` | Written to Scylla and produced to Redpanda |
| `400 Bad Request` | Malformed JSON or missing required field |
| `503 Service Unavailable` | Scylla or Redpanda write failed |

### `GET /healthz`

Returns `200 OK` for liveness checks.

## Configuration

Both binaries are configured via environment variables (defaults target the
compose stack on `localhost`):

| Variable | Default | Used by |
|----------|---------|---------|
| `API_ADDR` | `:8080` | api |
| `SCYLLA_HOSTS` | `127.0.0.1:9042` | api |
| `SCYLLA_KEYSPACE` | `grcs` | api |
| `REDPANDA_BROKER` | `127.0.0.1:9092` | api, consumer |
| `EVENTS_TOPIC` | `events` | api, consumer |
| `CONSUMER_GROUP` | `grcs-clickhouse` | consumer |
| `CLICKHOUSE_ADDR` | `127.0.0.1:9000` | consumer |
| `CLICKHOUSE_DB` | `grcs` | consumer |
| `CLICKHOUSE_USER` | `default` | consumer |
| `CLICKHOUSE_PASSWORD` | `` (empty) | consumer |
| `BATCH_SIZE` | `1000` | consumer |
| `FLUSH_MS` | `1000` | consumer |

## Design notes

**Why two stores for the same data.** Each is modeled for the query it serves:

| | ScyllaDB | ClickHouse |
|---|---|---|
| Partition / sort | by `user_id`, time DESC | by `type`, time |
| Optimized for | "recent events for a user" (point read) | "counts by type over time" (aggregation) |
| Role | operational source of truth | analytical rollups |

**Write ordering in the API.** Scylla is written synchronously and must succeed
before a `2xx` — it is the durable operational record. The Redpanda produce uses
`RequiredAcks=all`, so a `202` means the event is on the log, not just buffered
client-side. If Scylla fails, nothing is acknowledged and nothing is produced.

**Batching in the consumer.** ClickHouse is built for large, infrequent inserts,
not row-per-request. The consumer accumulates records and flushes when the batch
fills (`BATCH_SIZE`) or a timer fires (`FLUSH_MS`), whichever comes first — so
low traffic still drains within a second and bursts coalesce into efficient
inserts.

**Delivery semantics.** The consumer uses a consumer group with manual offset
commits and only commits **after** a successful ClickHouse insert (insert first,
commit second). This is **at-least-once**: a crash between insert and commit
replays the batch on restart, which can produce duplicate rows in ClickHouse. The
clean fix is a `ReplacingMergeTree` keyed on `id` (or otherwise idempotent
inserts); it's left out here to keep the demo focused.

**Failure isolation.** Because analytics writes go through the Redpanda log, a
slow or down ClickHouse does not block the API — events accumulate on the topic
and the consumer drains them once it recovers.

## Troubleshooting

- **API can't connect to Scylla at startup** — Scylla isn't healthy yet, or the
  keyspace wasn't applied. Confirm `docker compose ps` shows `(healthy)` and that
  step 2 ran.
- **Topic errors on produce** — Redpanda auto-creates topics by default; if
  disabled, create it manually:
  `docker compose exec redpanda rpk topic create events`.
- **No rows in ClickHouse** — check the consumer log for `flushed N events`. With
  default settings, flushes happen at most once per second or every 1000 events.
- **Consumer fails with ClickHouse `code: 516` (authentication failed)** — the
  server image restricts the `default` user to localhost, so host connections
  through Docker's port-forward are rejected. `init/clickhouse-users.xml` (mounted
  by compose) re-opens it. If you see this, the container predates that mount:
  recreate it with `docker compose up -d clickhouse`.
