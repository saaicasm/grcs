-- ScyllaDB schema for the grcs operational store.
--
-- Apply after the container is healthy:
--   docker compose exec -T scylla cqlsh < init/scylla.cql
--
-- This is the API write path: every accepted POST /events is written here for
-- fast key-based access, independent of the analytics path (ClickHouse).

CREATE KEYSPACE IF NOT EXISTS grcs
  WITH replication = {
    'class': 'SimpleStrategy',   -- single-node demo; use NetworkTopologyStrategy in prod
    'replication_factor': 1
  };

-- events: one row per event, partitioned by user_id.
--
-- Data modeling note: Scylla/Cassandra is query-driven.
-- We partition by user_id and cluster by (timestamp DESC, id) so the natural
-- query — "give me the most recent events for a user" — is a single-partition
-- read served in timestamp order, no filtering or secondary index needed.
--
-- Field names/types mirror internal/event.Event so the same struct round-trips
-- through Scylla, Redpanda, and ClickHouse.
CREATE TABLE IF NOT EXISTS grcs.events (
  user_id    text,        -- partition key  (Event.UserID)
  timestamp  timestamp,   -- clustering key (Event.Timestamp)
  id         text,        -- clustering key, tiebreaker (Event.ID)
  type       text,        -- Event.Type
  source     text,        -- Event.Source
  payload    text,        -- Event.Payload, stored as a JSON string
  PRIMARY KEY ((user_id), timestamp, id)
) WITH CLUSTERING ORDER BY (timestamp DESC, id ASC);
