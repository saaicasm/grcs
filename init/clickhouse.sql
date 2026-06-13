-- ClickHouse schema for the grcs analytics store.
--
-- Apply after the container is healthy:
--   docker compose exec -T clickhouse clickhouse-client --multiquery < init/clickhouse.sql
--
-- This is the analytics path: the consumer reads events off Redpanda and
-- batch-inserts them here. It is decoupled from the API write path, so if this
-- store is slow or down the API keeps accepting events.

CREATE DATABASE IF NOT EXISTS grcs;

-- events: columnar table tuned for aggregations over the event stream.
--
-- Data modeling note: ClickHouse is analytics-driven. We use MergeTree, the
-- primary sorting key (type, timestamp), and monthly partitioning so the common
-- analytical queries — counts/rollups by event type over a time range — scan
-- only the relevant partitions and granules instead of the whole table.
--
-- Field names/types mirror internal/event.Event and the Scylla schema, so the
-- same struct round-trips through Scylla, Redpanda, and ClickHouse.
CREATE TABLE IF NOT EXISTS grcs.events (
  id         String,                       -- Event.ID
  user_id    String,                       -- Event.UserID
  type       LowCardinality(String),       -- Event.Type   (few distinct values)
  source     LowCardinality(String),       -- Event.Source (few distinct values)
  timestamp  DateTime64(3, 'UTC'),         -- Event.Timestamp (millisecond precision)
  payload    String                        -- Event.Payload, stored as a JSON string
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)            -- monthly partitions: cheap time-range pruning + drops
ORDER BY (type, timestamp, id);             -- sorting key: rollups by type over time stay sequential

-- Example analytical queries this layout serves efficiently:
--
--   -- event volume per type over the last day
--   SELECT type, count() AS n
--   FROM grcs.events
--   WHERE timestamp >= now() - INTERVAL 1 DAY
--   GROUP BY type
--   ORDER BY n DESC;
--
--   -- hourly event rate by source
--   SELECT toStartOfHour(timestamp) AS hour, source, count() AS n
--   FROM grcs.events
--   GROUP BY hour, source
--   ORDER BY hour;
