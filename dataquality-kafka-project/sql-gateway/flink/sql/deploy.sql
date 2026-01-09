-- Set Flink job name (appears in Flink UI). In Zeppelin use %flink.sql and run this
-- before any statements that submit the streaming job (e.g. INSERT INTO ...).
SET 'pipeline.name' = 'dq-enrichment-job';

CREATE TABLE topic1_source (
  pk STRING,
  hash STRING,
  payload STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-topic1',
  'scan.startup.mode' = 'earliest-offset',
  'json.ignore-parse-errors' = 'true',
  'format' = 'raw'
);

CREATE TABLE topic2_hash (
  pk STRING,
  cloud_hash STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-topic2',
  'scan.startup.mode' = 'earliest-offset',
  'json.ignore-parse-errors' = 'true',
  'format' = 'raw'
);

CREATE TABLE dq_results (
  pk STRING,
  payload STRING,
  ts TIMESTAMP(3),
  hash_match BOOLEAN,
  PRIMARY KEY (pk) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dq-results',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-results-group', -- Ensure group.id is set  
  'json.ignore-parse-errors' = 'true', 
  'key.format' = 'raw',
  'value.format' = 'raw'
  );  
  --  'value.format' = 'json'


CREATE VIEW dq_results_view AS
SELECT
  pk,
  source_hash,
  cloud_hash,

  CASE
    WHEN source_hash IS NULL THEN 'INVALID'
    WHEN cloud_hash IS NULL THEN 'INVALID'
    WHEN source_hash <> cloud_hash THEN 'INVALID'
  ELSE 'VALID'
  END AS dq_status,

  CASE
    WHEN source_hash IS NULL THEN 'SOURCE_HASH_MISSING'
    WHEN cloud_hash IS NULL THEN 'CLOUD_HASH_MISSING'
    WHEN source_hash <> cloud_hash THEN 'HASH_MISMATCH'
  ELSE 'OK'
  END AS dq_reason,

  -- Severity aggregation
  CASE
    WHEN source_hash IS NULL THEN 2
    WHEN cloud_hash IS NULL THEN 2
    WHEN source_hash <> cloud_hash THEN 1
  ELSE 0
  END AS dq_error_total

FROM enriched_events;

-----

-- Add processing-time wrappers so we can do a time-bounded interval join.
CREATE VIEW topic1_source_proc AS
SELECT
  pk,
  hash,
  payload,
  ts,
  PROCTIME() AS proc_time
FROM topic1_source;

CREATE VIEW topic2_hash_proc AS
SELECT
  pk,
  cloud_hash,
  PROCTIME() AS proc_time
FROM topic2_hash;

-- Replace the original join with a LEFT interval join that waits up to 10 seconds
-- for topic2 to produce/advance its state (joins t2 where t2.proc_time is within
-- [t1.proc_time, t1.proc_time + INTERVAL '10' SECOND]).
CREATE VIEW enriched_events_view AS
SELECT DISTINCT
  t1.pk,
  t1.hash AS source_hash,
  t2.cloud_hash,
  t1.payload,
  t1.ts
FROM topic1_source_proc t1
LEFT JOIN topic2_hash_proc t2
  ON t1.pk = t2.pk
  AND t2.proc_time BETWEEN t1.proc_time AND t1.proc_time + INTERVAL '10' SECOND;
  AND t2.event_time BETWEEN t1.event_time - INTERVAL '5' SECOND AND t1.event_time + INTERVAL '10' SECOND;
-- This view enriches events from topic1_source with cloud_hash from topic2_hash 
-- based on matching primary keys (pk). 
-- Added payload to the view for further processing in data quality checks.


CREATE VIEW dq_check_view AS
SELECT
    ee.pk,
    ee.payload,
    ee.ts,
    CASE 
        WHEN ee.cloud_hash IS NULL THEN FALSE
        WHEN ee.source_hash = ee.cloud_hash THEN TRUE
        ELSE FALSE
    END AS hash_match
FROM enriched_events ee;
-- This view performs data quality checks by comparing source_hash and cloud_hash.
-- It outputs a boolean indicating whether the hashes match for each event.
-- If cloud_hash is NULL, it indicates missing data, resulting in a FALSE match.
-- The resulting view includes pk, payload, timestamp, and the hash match result.

CREATE VIEW dq_metrics AS
SELECT
  dq_status,
  COUNT(*) AS total
FROM dq_results_view
GROUP BY dq_status;
-- To query the metrics, we can use:
-- SELECT * FROM dq_metrics;

INSERT INTO dq_results
SELECT
    pk,
    payload,
    ts,
    hash_match
FROM dq_check_view;
-- This insertion writes the results of the data quality checks into the dq_results table.
-- The dq_results table captures the primary key, payload, timestamp, and
-- whether the hash comparison was successful for each event.
-- This SQL script sets up the necessary tables, views, and data quality checks
-- for processing and validating data from two Kafka topics.
-- This SQL script sets up the necessary tables, views, and data quality checks
-- for processing and validating data from two Kafka topics.
-- It reads from topic1_source and topic2_hash, enriches the data,
-- performs hash comparisons, and writes the results to dq_results.

-- Stable wrapper layer (never empty)

CREATE VIEW dq_results_final AS
SELECT *
FROM dq_results;

-- Option 2 produced by python
-- Data Quality Rules Implementation
-- DLQ + Metrics Sink
CREATE TABLE dq_dlq (
  pk STRING,
  dq_reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq_dlq',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-dlq-group', 
  'format' = 'json'
);

INSERT INTO dq_dlq
SELECT pk, dq_reason
FROM dq_results
WHERE dq_status = 'INVALID';


------Reconcile Data Quality Rules Examples------
CREATE TABLE cassandra_reconcile (
  pk STRING,
  payload STRING,
  ts TIMESTAMP(3),
  dq_status STRING,
  dq_reason STRING,
  dq_error_total INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cassandra-reconcile',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO cassandra_reconcile
SELECT
  v.pk,
  r.payload,
  r.ts,
  v.dq_status,
  v.dq_reason,
  v.dq_error_total
FROM dq_results_view v
LEFT JOIN dq_results r
  ON v.pk = r.pk
WHERE v.dq_status = 'INVALID';
-- Additional Data Quality Rules can be implemented similarly
-- by creating views or tables that encapsulate specific checks.
-- Below are some example rules that can be added as needed.  
  

-------------------------------------------------
-- Example Rule 1: Check for null values in critical columns
-- CREATE VIEW dq_null_check AS 
-- SELECT *,
--        CASE      
--            WHEN critical_column IS NULL THEN 'FAIL'
--            ELSE 'PASS'
--        END AS null_check_result
-- FROM source_table;
-- Example Rule 2: Check for value ranges
-- CREATE VIEW dq_value_range_check AS
-- SELECT *,
--        CASE
--            WHEN numeric_column < 0 OR numeric_column > 100 THEN 'FAIL'
--            ELSE 'PASS'
--        END AS value_range_check_result
-- FROM source_table;
-- Example Rule 3: Check for duplicate records
-- CREATE VIEW dq_duplicate_check AS
-- SELECT pk,
--        COUNT(*) AS record_count,
--        CASE
--            WHEN COUNT(*) > 1 THEN 'FAIL'
--            ELSE 'PASS'
--        END AS duplicate_check_result
-- FROM source_table
-- GROUP BY pk;
