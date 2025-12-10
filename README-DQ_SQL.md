Architecture (Zeppelin + Flink SQL)
Kafka (raw topic)
      ↓
Zeppelin Notebook
    + Flink SQL Gateway
      ↓
SQL DQ Job:
  - Schema validation
  - Null checks
  - Threshold rules
  - Row_hash = SHA2(JSON)
      ↓
Outputs
  1) Kafka valid topic
  2) Kafka invalid topic
  3) Cassandra (onPrem)
  4) Cassandra (Azure)
---------------------------------------------------------------------------------
| Capability                                      | Status                      |
| ----------------------------------------------- | --------------------------- |
| Table-agnostic hashing                          | ✅ SHA2 hashing via SQL     |
| DQ rule engine                                  | ✅ SQL CASE-based rules     |
| Schema validation                               | ✅ SQL IS NULL / type errors|
| On-Prem Cassandra                               | ✅ supported                |
| Azure Cassandra                                 | ✅ supported                |
| Dual-pipeline (2 Kafka → 2 Flink → 2 Cassandra) | fully supported             |
| Zero Java code                                  | 100% SQL-based              |
---------------------------------------------------------------------------------
1. Create Zeppelin notebook JSON 
2. Reusable DQ rules YAML → SQL rule generator (Python based rules_to_sql.py)
  (will convert YAML into Flink SQL case logic - generated DQ-results view SQL)
3. Cassandra drift reconciliation as SQL view for Flink SQL = will compare rows:
  OnPRem and Cloud Cassandra tables, will show mismatches.
  >> dq/drift_rec.sql

## As Result DQ will cover:
 1. Fully-featured DQ rule engine
 2. Extensible YAML rules
 3. Automatic SQL view generation
 4. Zeppelin REST auto-update
 5. External lookups
 6. Cross-field checks
 7. Null-handling semantics
 8. Ready for large-scale DQ governance across all Flink jobs

================================================================================
# STEPS:
## Zeppelin SQL Notebook
# Content:
1. Kafka source
2. Row hash
3. DQ rule engine (USe py rule_to_sql.py to generate rules:)
4. Valid topic
5. Invalid topic
6. Cassandra On-Prem sink
7. Cassandra Azure sink

## Deployment Steps:
  1. Deploy the Zeppelin notebook (zeppelin_rowhash_dq.json) via Zeppelin UI → Import.
  2. Put rules.yaml and rules_to_sql.py on a machine accessible by Zeppelin (or run locally).
  3. Run python rules_to_sql.py rules.yaml > dq_results.sql, copy/paste the CREATE VIEW dq_results SQL into the Zeppelin paragraph 3 (replacing the placeholder).
  4. Alternatively 2nd option: we can run the generator inside Zeppelin using %sh python /path/rules_to_sql.py /path/rules.yaml and copy the output into a %flink.ssql paragraph.
  >> Limitations:
  3.2. This generator emits CASE statements using Flink SQL functions (REGEXP_LIKE / NOW() etc. older versions may use REGEXP).
  3.3. More complex rules: 
        - cross-field comparisons
        - external lookups
        - schema registry checks
      require either UDFs or pre-/post-processing outside SQL — the generator covers the common rule types requested.
  4. Ensure Kafka topics exist: raw_topic, validated_topic, invalid_topic.
  5. Ensure Flink has the Cassandra connector available and the cassandra_onprem / cassandra_azure table connectors are correctly configured (host, auth).
  6. Run notebook paragraphs in Zeppelin to get: (source → enrich → dq_results → sinks).
  7. Monitor cassandra_reconciliation view for drift; optionally write to an alerting topic.
    Can query:
  >> SELECT * FROM cassandra_reconciliation WHERE reconciliation_status <> 'OK' LIMIT 100;
## For testing:
8. Extend the rules_to_sql.py generator to support more rule types (cross-field, external lookup, configurable null-handling), or to directly push the generated SQL into Zeppelin via the REST API. 
  >> Use: rules_to_sql_extended.py

9. Create a Zeppelin %sh paragraph that runs the generator and auto-inserts the generated SQL into a paragraph (requires Zeppelin REST API tokens).
>> Zeppelin %sh paragraph
  A paragraph that runs the generator inside Zeppelin, 
    - fetches YAML
    - generates SQL
    - auto-updates another paragraph.
10. Add Schema Validation - runtime message validator PY
  - Integration Steps:
    1. Import and call validate_dict in dq_service.py pipeline before rules: invalid messages get routed to DLQ/invalid topic.
    2. For Azure Schema Registry, need to use the Azure SDK (azure-schemaregistry + azure-identity) in production; 
    it is for Confluent REST.
11. Add Schema-evolutuon / Compatibility Check 
 ## ATTN: 
  script is conservative 
    - Full Avro compatibility semantics need to plug in a compatibility library like 
    -- Apache Avro 1.11 Java compatibility tools 
    -- Confluent's compatibility checks
  Need to use:
   --latest-prev to compare latest two versions quickly.
11. 1. 
  Integration schema checks into DQ flow:
  11.1.1 Runtime schema validation (per-event)
    - Add schema_validator.SchemaValidator into Python DQ microservice (dq_service.py) before rule engine:
    - Call ok, reason = sv.validate_dict(record, subject=subject_name)
    - If not ok: mark record as invalid and route to DLQ
    - If ok: proceed to other rules
    This ensures only schema-conforming messages reach Flink SQL / the dq_results view.
  >> inside existing dq_service.py loop:
  >> py:
    from schema_validator import SchemaValidator
    sv = SchemaValidator(schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL"))

    subject = "<topic>-value"
    ok, reason = sv.validate_dict(record, subject=subject)
    if not ok:
        record["_dq_reasons"] = [{"id": "schema_validation", "severity": "fail", "message": reason}]
        producer.send(INVALID_TOPIC, record)
        continue
      # else: forward to validated topic or to Flink
--------------------------------------------------------------
## Schema-evolution checks (pre-deploy / CI / governance)
  Use schema_evolution_check.py in CI pipeline to automatically check compatibility when pushing new schemas.

  ## GitLab/GitHub CI step:
    On PR that updates schemas 
    - run python:
     schema_evolution_check.py --registry $REG --subject $SUB --version-old $OLD --version-new $NEW or --latest-prev

  ATTN: 
    Fail the build if breaking changes found (unless explicitly approved).
--------------------------------------------------------------
###  Zeppelin integration & rules_to_sql
  In rules.yaml:
    - allow a schema_registry rule that instructs the generator to:
    - (a) place a placeholder condition in the SQL (because SQL can't call registry at runtime), and
    - (b) the generator or deploy pipeline will ensure the runtime validator microservice is deployed
  >> Add to rule next entry:
    - id: schema_check
      type: schema_registry
      subject: "customers-value"
      severity: fail
      message: "message does not conform to schema in registry"


===================================================
:: SQL ::
---------------------------------------------------
INSERT INTO keyspace.table (pk, col1, col2) VALUES (pk_val, val1, val2) IF NOT EXISTS;

INSERT INTO keyspace.table (pk, col1) VALUES (pk_val, col_val) USING TTL 86400; -- TTL in seconds
INSERT INTO keyspace.table (pk, col1) VALUES (pk_val, col_val) USING TIMESTAMP 1620000000000000; -- timestamp in microseconds
-- combine:
INSERT INTO keyspace.table (pk, col1) VALUES (pk_val, col_val) USING TTL 3600 AND TIMESTAMP 1620000000000000;
-----------------------
## Keyspace creation (dev and production examples)
-- Simple/local development (not for multi-DC production)
CREATE KEYSPACE IF NOT EXISTS testks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

-- Production — replace datacenter names and RFs appropriately
CREATE KEYSPACE IF NOT EXISTS dmitriks
WITH replication = {
'class': 'SimpleStrategy',
'DC1': 3,
'DC2': 2
}
AND durable_writes = true;

## Notes:
1. Use SimpleStrategy only for development/single-DC. 
2. Use NetworkTopologyStrategy for production.
3. Replication factor (RF) determines availability and consistency behavior.

CREATE TABLE IF NOT EXISTS dmitriks.users (
user_id uuid,
name text,
email text,
signup_ts timestamp,
last_seen timestamp,
tags set<text>,
PRIMARY KEY (user_id)
) WITH comment = 'Basic user table'
AND gc_grace_seconds = 864000; -- 10 days

## Notes:
 -- This is a single-partition primary key; reads by user_id are cheap.
 -- Store sets/lists/maps for small-size usage.

## 2) Time-series / event table (partitioning + clustering)
-- Model: partition by day (or tenant), cluster by event time desc
CREATE TABLE IF NOT EXISTS dmitriks.events_by_day (
tenant_id text,
day date, -- day bucket
event_time timeuuid, -- clustering column for ordering
event_id uuid,
event_type text,
payload text,
PRIMARY KEY ((tenant_id, day), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit':'DAYS'}
AND default_time_to_live = 0;
-----------------
-- Example insert using toTimeStamp/now:
INSERT INTO dmitriks.events_by_day (tenant_id, day, event_time, event_id, event_type, payload)
VALUES ('tenantA', toDate(now()), now(), uuid(), 'click', '{...}');

## Design notes:
  -- Partition key is (tenant_id, day) to bound partition size. Adjust bucketing strategy depending on volume.
  -- Use timeuuid clustering for ordering and uniqueness.
  -- Consider compaction strategy TimeWindowCompactionStrategy for time series.
## 3) Counters / metrics table
-- Counters must use UPDATE to increment; schema must only contain counter columns (except primary key).

CREATE TABLE IF NOT EXISTS dmitriks.page_view_counters (
  page_id text,
  date date,
  views counter,
  PRIMARY KEY ((page_id), date)
);
## 4) Collections and UDT
-- Create a UDT for address
CREATE TYPE IF NOT EXISTS dmitriks.address (
street text,
city text,
zip text
);

## -- Table using UDT and map/list/set
CREATE TABLE IF NOT EXISTS dmitriks.customers (
customer_id uuid,
name text,
address frozen<address>,
phone_numbers list<text>,
attributes map<text, text>,
favorite_tags set<text>,
PRIMARY KEY (customer_id)
);

## Insert example:
INSERT INTO dmitriks.customers (customer_id, name, address, phone_numbers, attributes, favorite_tags)
VALUES (uuid(), 'Acme Corp', {street: '1 Main', city: 'Town', zip: '12345'}, ['+1-800-1111'], {'tier':'gold'}, {'fast','reliable'});

## Notes:
-- Use frozen<> for UDTs or collections when storing as a value (prevents partial updates).
Avoid very large collections in a row.
## 5) Secondary index and materialized view examples (use with caution)
-- Secondary index (good for low-cardinality or small partitions)
CREATE INDEX IF NOT EXISTS users_by_email_idx ON dmitriks.users (email);

-- Materialized view example (deprecated/with caution — consider denormalization or application-side writes)
CREATE MATERIALIZED VIEW IF NOT EXISTS dmitriks.users_by_email AS
SELECT user_id, email, name FROM dmitriks.users
WHERE email IS NOT NULL AND user_id IS NOT NULL
PRIMARY KEY (email, user_id);

## Notes:
-- Secondary indexes and MVs have performance and maintenance trade-offs. Prefer designing tables for query patterns or maintain denormalized tables via application or lightweight batches.
## 6) Table options: compaction, compression, caching, TTL
-- Example with Leveled Compaction (good for small writes & random reads)
CREATE TABLE IF NOT EXISTS dmitriks.products (
product_id uuid PRIMARY KEY,
name text,
category text,
price decimal
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'};

-- Set default TTL on a table (applies to all writes without explicit TTL)
CREATE TABLE IF NOT EXISTS dmitriks.volatile_logs (
id uuid PRIMARY KEY,
msg text,
created timestamp
) WITH default_time_to_live = 86400; -- 1 day
-----------------------------------------------
## -- Batch multiple statements (use sparingly — logged batch across multiple partitions is anti-pattern):
BEGIN BATCH
INSERT INTO testks.table (pk, c1) VALUES (pk1, v1);
INSERT INTO testks.table (pk, c1) VALUES (pk2, v2);
APPLY BATCH;

============================================================
## 1. SQL Zeppilin to Run:
    %flink.ssql
    -- SQL goes here
## 2. Register Kafka source table in Zeppelin    

%flink.ssql

CREATE TABLE raw_events (
    id STRING,
    ts TIMESTAMP(3),
    value DOUBLE,
    event_json STRING,
    WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
## 3. Add row-hash using Flink SQL (SHA-256)
%flink.ssql

CREATE VIEW enriched_events AS
SELECT
    *,
    SHA2(event_json, 256) AS row_hash
FROM raw_events;

## 4. Create a re-usable SQL Rule Engine
    Integrate Rules:
        - Required field check
        - Threshold check
        - Timestamp freshness
        - Schema check (via IS NULL detection)
        - Row-level record hashing (done above)

## 5. Split valid vs invalid into Kafka topics
>>    Valid rows → Kafka valid topic
%flink.ssql

CREATE TABLE kafka_valid (
    id STRING,
    ts TIMESTAMP(3),
    value DOUBLE,
    row_hash STRING,
    event_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'validated_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO kafka_valid
SELECT id, ts, value, row_hash, event_json
FROM dq_results
WHERE dq_status = 'VALID';
--------------------------------

## Invalid rows → Kafka invalid topic
%flink.ssql

CREATE TABLE kafka_invalid (
    id STRING,
    dq_status STRING,
    dq_reason STRING,
    row_hash STRING,
    event_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'invalid_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO kafka_invalid
SELECT id, dq_status, dq_reason, row_hash, event_json
FROM dq_results
WHERE dq_status <> 'VALID';

----------------------------------

## 6. Sink to Cassandra (On-Prem + Azure)
    Define 2 Cassandra sink tables:
>> On-Prem Cassandra:
%flink.ssql

CREATE TABLE cassandra_onprem (
    id STRING PRIMARY KEY,
    ts STRING,
    value DOUBLE,
    row_hash STRING,
    event_json STRING
) WITH (
  'connector' = 'cassandra',
  'host' = 'cassandra-onprem',
  'keyspace' = 'dq',
  'table' = 'dq_events'
);

INSERT INTO cassandra_onprem
SELECT id, CAST(ts AS STRING), value, row_hash, event_json
FROM dq_results
WHERE dq_status = 'VALID';
-------------------------------

## Azure Cassandra (Cosmos DB Cassandra API)
%flink.ssql

CREATE TABLE cassandra_azure (
    id STRING PRIMARY KEY,
    ts STRING,
    value DOUBLE,
    row_hash STRING,
    event_json STRING
) WITH (
  'connector' = 'cassandra',
  'host' = 'cassandra-azure',
  'username'='...',
  'password'='...',
  'keyspace' = 'dq',
  'table' = 'dq_events'
);

INSERT INTO cassandra_azure
SELECT id, CAST(ts AS STRING), value, row_hash, event_json
FROM dq_results
WHERE dq_status = 'VALID';
-------------------------------

## 7. Full Zeppelin SQL Notebook (copy/paste ready)
