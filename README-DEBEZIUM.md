## Debezium Installation steps
Corporate Compliance:

1. No table scans
2. Near-real-time
3. Preserves order
4. Immutable audit trail
5. Scales with Kafka
6. Works perfectly with Flink SQL
------------------------------

## Prerequisites (Important)
1. Cassandra 4.x (recommended) or 3.11+
2. CDC enabled on Cassandra
3. Kafka + Kafka Connect running
4. Debezium Cassandra connector plugin installed on Kafka Connect

## Step 1 — Enable CDC in Cassandra (On-Prem)
cdc_enabled: true
cdc_total_space_in_mb: 4096
cdc_free_space_check_interval_ms: 250
## Restart Cassandra nodes.
## Enable CDC per Table:
    ALTER TABLE sales.events WITH cdc = true;

## Step 2 — Kafka Topics (Create First)
kafka-topics.sh --create \
  --topic onprem.cdc.sales.events \
  --bootstrap-server kafka:9092 \
  --partitions 6 \
  --replication-factor 3
-- DLQ dedletter que topic
kafka-topics.sh --create \
  --topic onprem.cdc.sales.events.dlq \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 3

## Step 3 — Debezium Cassandra Connector Configuration
    - cassandra-onprem-connector.json
>>
{
  "name": "cassandra-onprem-sales-connector",
  "config": {
    "connector.class": "io.debezium.connector.cassandra.CassandraConnector",

    "tasks.max": "1",

    "cassandra.contact.points": "10.10.0.5,10.10.0.6",
    "cassandra.port": "9042",
    "cassandra.local.datacenter": "DC_ONPREM",

    "cassandra.keyspace": "sales",
    "cassandra.table.include.list": "sales.events",

    "cdc.dir": "/var/lib/cassandra/cdc",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",

    "topic.prefix": "onprem.cdc",

    "snapshot.mode": "initial",

    "tombstones.on.delete": "false",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "onprem.cdc.sales.events.dlq",
    "errors.deadletterqueue.context.headers.enable": "true",

    "heartbeat.interval.ms": "30000",

    "provide.transaction.metadata": "false"
  }
}

-------------------------------

## Step 4 — Deploy Connector
>>bash:
curl -X POST \
  -H "Content-Type: application/json" \
  http://kafka-connect:8083/connectors \
  --data @cassandra-onprem-connector.json

>> verify
  curl http://kafka-connect:8083/connectors/cassandra-onprem-sales-connector/status

## Step 5 — Kafka Message Format (What Flink Receives)
# op = c/u/d
# after contains the row image
# source.dc guarantees data-center traceability
{
  "op": "u",
  "ts_ms": 1730001234567,
  "after": {
    "id": "123",
    "amount": 100.5,
    "event_ts": "2025-01-01T10:00:00"
  },
  "source": {
    "keyspace": "sales",
    "table": "events",
    "dc": "DC_ONPREM"
  }
}

## Step 6 — Flink SQL Source (Zeppelin)
CREATE TABLE cass_onprem_cdc (
  op STRING,
  ts_ms BIGINT,
  after ROW<
    id STRING,
    amount DOUBLE,
    event_ts TIMESTAMP(3)
  >,
  source ROW<
    keyspace STRING,
    table_name STRING,
    dc STRING
  >,
  WATERMARK FOR after.event_ts AS after.event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'onprem.cdc.sales.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
-------------------------
## Step 7 — Extract Canonical Record for Hashing
# ATTN
# Deletes can be handled separately if needed.
CREATE VIEW onprem_events_flat AS
SELECT
  after.id AS id,
  after.amount AS amount,
  after.event_ts AS event_ts,
  source.dc AS source_dc
FROM cass_onprem_cdc
WHERE op IN ('c','u');

----------------------------
## Summarize:
## Security & Production ready
-----------------------------------------
| Control        | Recommendation       |
| -------------- | -------------------- |
| Cassandra user | Read-only            |
| Kafka ACLs     | Topic-scoped         |
| CDC dir        | Local SSD            |
| Connector      | One per keyspace     |
| Offset storage | Kafka internal       |
| Monitoring     | Lag + commitlog size |
-----------------------------------------

============================================================
## Next Steps (Optional Enhancements)
✅ Cloud Cassandra Debezium config
✅ Schema Registry compatibility checks
✅ Delete reconciliation strategy
✅ Daily CDC completeness validation
✅ Auto-repair pipelines
============================================================