## Debezium Installation steps
# ATTN Can be used for Cassandra Version 4+
Corporate Compliance:

1. No table scans
2. Real-time
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
# 1.1  Save debezium/cassandra-cloud-connector.json and POST to Kafka connect
>> Notes
topic.prefix produces topic cloud.cdc.sales.events. If you want the topic to be exactly table.update, set topic.prefix to table.update and cassandra.table.include.list accordingly (Debezium will append table name — most connectors do this).

snapshot.mode = initial runs a snapshot on first deploy; change to never if not desired.
# 1.2 Kafka Connect REST commands
http://kafka-connect:8083 connect URL.
# Create connector:
>> bash:
curl -X POST -H "Content-Type: application/json" \
  --data @cassandra-cloud-connector.json \
  http://kafka-connect:8083/connectors

# Check connector status
  curl http://kafka-connect:8083/connectors/cassandra-cloud-sales-connector/status | jq .
# List connectors
  curl http://kafka-connect:8083/connectors | jq .  
# Delete connector
  curl -X DELETE http://kafka-connect:8083/connectors/cassandra-cloud-sales-connector
# Add/Update connector config (PUT)
  curl -X PUT -H "Content-Type: application/json" \
  --data @cassandra-cloud-connector.json \
  http://kafka-connect:8083/connectors/cassandra-cloud-sales-connector/config


---------------------------------------------------

## Step 2 — Kafka Topics (Create First)
kafka-topics.sh --create \
  --topic onprem.cdc.sales.events \
  --bootstrap-server kafka:9092 \
  --partitions 6 \
  --replication-factor 3

# -- DLQ dedletter que topic
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
----------------------------------------------------
## Another Option 
## 3) Zeppelin notebook JSON (importable)
  Save as zeppelin_cdc_hash_dq.json and Import into Zeppelin (Notebook → Import note → upload).
  This notebook uses the Flink SQL interpreter (%flink.ssql) which many Zeppelin + Flink setups use — adjust to %flink.sql if your gateway uses that.
## Steps (paragraphs) to Deploy in Zeppelin:
  Notes
    -The notebook assumes an onprem_hash_events topic exists and is produced by your on-prem Flink pipeline.
    - Time window tolerance (±10 minutes) can be adjusted to match replication/ingestion delays.
    -If Flink SQL interpreter uses %flink.sql instead of %flink.ssql, change the paragraph  prefixes.
## Deployment:
  - Run cdc-hash-dq-reconile.json in Zeppelin
It will create paragraphs:
  1. Create CDC source table from Debezium topic cloud.cdc.sales.events
  2. Flatten after image and compute canonical row_hash using SHA2
  3. Apply DQ rules (required fields, negative values, timestamp freshness)
  4. Publish hash audit topic cloud_hash_events
  5. Consume onprem_hash_events topic (from onprem pipeline)
  6. Reconciliation view: windowed full outer join with tolerance for time skew
  7. Persist reconciliation mismatches to dq_reconciliation_audit topic
  8. Counters: valid / invalid per minute to metrics topics
---------------------------------------------------
2. Schema Registry integration & Avro converter (recommended)
  To use Avro events and schema tracking (strongly recommended):
  a) Example Avro schema for the CDC after object (register this in Schema Registry)
      sales.events-value.avsc:
      >>
      {
        "type": "record",
        "name": "SalesEvent",
        "namespace": "com.company.sales",
        "fields": [
          {"name":"id", "type":["null","string"], "default": null},
          {"name":"amount", "type":["null","double"], "default": null},
          {"name":"event_ts", "type":["null", {"type":"long", "logicalType":"timestamp-millis"}], "default": null}
        ]
      }
# Note: Register it using Confluent Schema Registry API or curl + avro client.
----------------------------
b) Kafka Connect converter config (use AvroConverter)
# Set in Debezium connector 
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "https://schema-registry.example.com",
  "value.converter.basic.auth.credentials.source": "USER_INFO",
  "value.converter.basic.auth.user.info": "SR_API_KEY:SR_API_SECRET",
# or Kafka Connect worker config:
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "https://schema-registry.example.com",
  "key.converter.basic.auth.credentials.source": "USER_INFO",
  "key.converter.basic.auth.user.info": "SR_API_KEY:SR_API_SECRET"
# Note:  
 - When using AvroConverter, Debezium will emit Avro messages with subjects like cloud.cdc. sales.events-value. Flink SQL tables should set format='avro' (or use a proper format factory) and leverage schema registry integration if your Flink distribution supports it.
---------------------------
4) Schema Registry integration & Avro converter (recommended)
  To use Avro events and schema tracking (strongly recommended):
  a) Example Avro schema for the CDC after object (register this in Schema Registry)
    sales.events-value.avsc
>>
{
  "type": "record",
  "name": "SalesEvent",
  "namespace": "com.company.sales",
  "fields": [
    {"name":"id", "type":["null","string"], "default": null},
    {"name":"amount", "type":["null","double"], "default": null},
    {"name":"event_ts", "type":["null", {"type":"long", "logicalType":"timestamp-millis"}], "default": null}
  ]
}
<< >>
Note: 
  Register it using Confluent Schema Registry API or curl + avro client.
  b) Kafka Connect converter config (use AvroConverter)
  - Set in Debezium connector or Kafka Connect worker config:
  -----------------
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "https://schema-registry.example.com",
  "value.converter.basic.auth.credentials.source": "USER_INFO",
  "value.converter.basic.auth.user.info": "SR_API_KEY:SR_API_SECRET",

  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "https://schema-registry.example.com",
  "key.converter.basic.auth.credentials.source": "USER_INFO",
  "key.converter.basic.auth.user.info": "SR_API_KEY:SR_API_SECRET"
--------------------
## When using AvroConverter, Debezium will emit Avro messages with subjects like cloud.cdc.sales.events-value. Flink SQL tables should set format='avro' (or use a proper format factory) and leverage schema registry integration if your Flink distribution supports it.
  Flink SQL sample using Avro (if supported):
>>
CREATE TABLE cloud_cdc_raw (
  op STRING,
  ts_ms BIGINT,
  `after` ROW<id STRING, amount DOUBLE, event_ts TIMESTAMP(3)>,
  source ROW<keyspace STRING, table_name STRING, dc STRING>
) WITH (
  'connector' = 'kafka',
  'topic' = 'cloud.cdc.sales.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'avro-confluent', -- Flink distribution dependent
  'value.avro-confluent.schema-registry.url' = 'https://schema-registry.example.com'
);
## If your Flink distribution doesn’t support Confluent Avro format natively, use JSON in CDC and validate schema with the schema_validator.py pre-ingest service, or add a UDF/format.
----------------
5) Flink SQL: Hashing + DQ pipeline (concise summary & snippets)
Use the notebook above. Key patterns shown again:
## NOTE:
## Canonical hash (must be identical in onprem & cloud)
  SHA2(CONCAT_WS('|', COALESCE(id,'NULL'), COALESCE(CAST(amount AS STRING),'NULL'), CAST(event_ts AS STRING)), 256) AS row_hash

# DQ rules example (table-agnostic, tunable)
CASE
  WHEN id IS NULL THEN 'FAIL_ID_NULL'
  WHEN amount IS NULL THEN 'FAIL_AMOUNT_NULL'
  WHEN amount < 0 THEN 'FAIL_NEG_AMOUNT'
  WHEN event_ts < (CURRENT_TIMESTAMP - INTERVAL '7' DAY) THEN 'WARN_OLD_EVENT'
  ELSE 'PASS'
END AS dq_status
## Publish hash to audit topic SQL:
INSERT INTO cloud_hash_topic SELECT id, row_hash, source_dc, event_ts FROM cloud_dq WHERE dq_status = 'PASS';

# Reconciliation full outer join with event-time tolerance
(see Zeppelin notebook paragraph 6)
  Counters
- Use tumbling windows & write to Kafka metrics topics (see notebook paragraph 8).

-----------------------------
## Security, production & operational notes
 1. Don't publish PII in audit topics; if necessary, hash or mask fields before emitting.
 2. Network & credentials: Connectors should run in the same network region/VNet as 
 3. Cassandra nodes (or use secure peering). Use TLS & authentication for Kafka & Schema Registry.
 4. Connector scaling: Debezium Cassandra connector is usually single-task; scale by sharding keyspaces/tables or multiple connectors per node.
 5. Offsets & restart: Kafka Connect stores offsets — test restart/snapshot behavior in staging.
 6. Dead-letter queue: Configure errors.deadletterqueue.topic.name to capture malformed events.
 7. Monitoring: Monitor connector metrics, Kafka consumer lag, Flink job latency, and reconciliation mismatch counts. Send metrics to Datadog or Prometheus.
 8. Idempotency: Use idempotent writes in downstream sinks or deduplicate by PK/hash if Flink processing can replay.
---------------------------

Next:
A) Produce a ready-to-post cassandra-onprem-connector.json (on-prem variant).
B) Generate the Zeppelin JSON notebook file as a downloadable artifact (I can package it into a ZIP).
C) Create a sample rules.yaml and run rules_to_sql.py output to produce the dq_results SQL for Zeppelin.
D) Generate a Docker Compose snippet for Kafka Connect + Debezium + Schema Registry + Kafka for local testing.
E) Create a Confluent Avro curl example to register the schema automatically.
--------------------------------------------------
## Local Deployment:
## Docker Compose stack that includes:
1. Kafka
2. Zookeeper
(or KRaft-only mode if you prefer — I include both options, KRaft recommended)
3. Schema Registry
4. Kafka Connect
5. Debezium Cassandra Connector plugin preinstalled
6. Connect worker with AVRO + JSON converters
7. Optional UI (AKHQ)
8. Docker Compose: Kafka (KRaft) + Schema Registry + Kafka Connect + Debezium Cassandra Connector
# Folder Structure:
project/
  docker-compose-dq.yml
  connect-jars/
      debezium-connector-cassandra-2.5.0/
          (all Debezium Cassandra JARs)
  connect-config/
      cassandra-cloud-connector.jsonHI
      cassandra-onprem-connector.json
----------------
>> Run:
  docker-compose -f docker-compose-dq.yml up -d


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