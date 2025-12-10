## Flink SQL + Zeppelin + Cassandra DC (Cloud/onPrem)
>> One Flink job → one Kafka topic
✅ One Flink job → exactly one Cassandra data center
✅ No cross-DC writes
✅ Full Data Quality (DQ) + hashing
✅ Drift validation without dual connections

                       ┌─────────────┐
                       │ Kafka       │
                       └─────┬───────┘
                             │
             ┌───────────────┴───────────────┐
             │                               │
     topic_onprem_sales               topic_cloud_sales
             │                               │
     ┌───────▼────────┐              ┌───────▼────────┐
     │ Flink SQL Job  │              │ Flink SQL Job  │
     │  (Zeppelin)   │              │  (Zeppelin)   │
     └───────┬────────┘              └───────┬────────┘
             │                               │
   Cassandra On-Prem                  Cassandra Azure
     DC = DC_ONPREM                     DC = DC_AZURE
-----------------------------------------------------
| Requirement            | Flink SQL                |
| ---------------------- | ------------------------ |
| One DC per job         |  Guaranteed by connector |
| Governance             |  CI-validated            |
| Generic rules          |  SQL + hashing           |
| Zeppelin compatibility |  Native                  |
| Reduced risk           |  No code path errors     |
-----------------------------------------------------

## Flink SQL Guarantees “One DC Only”
 -Flink SQL itself cannot dynamically switch Cassandra DCs.
    - The DC is fixed in the Cassandra table connector options.
    - The enforcement happens at DDL time, not runtime.
-------------------------------------
## On-Prem Flink SQL Job
CREATE TABLE cassandra_onprem_sink (
  id STRING,
  event_ts TIMESTAMP(3),
  value DOUBLE,
  record_hash STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cassandra',
  'keyspace' = 'sales',
  'table' = 'events',
  'hosts' = '10.10.0.5,10.10.0.6',
  'datacenter' = 'DC_ONPREM'
);
-------------------------------------
## Azure Flink SQL Job
CREATE TABLE cassandra_cloud_sink (
  id STRING,
  event_ts TIMESTAMP(3),
  value DOUBLE,
  record_hash STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cassandra',
  'keyspace' = 'sales',
  'table' = 'events',
  'hosts' = 'cassandra-cloud.internal',
  'datacenter' = 'DC_AZURE'
);

## 1.2 Metrics Tables (Kafka Sinks)
>> Valid Records Counter Sink
CREATE TABLE dq_valid_metrics (
  job_name STRING,
  metric_ts TIMESTAMP(3),
  valid_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq_metrics_valid',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

## 1.3 Invalid Records Counter Sink
CREATE TABLE dq_invalid_metrics (
  job_name STRING,
  metric_ts TIMESTAMP(3),
  invalid_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq_metrics_invalid',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
------------------------------------
## 1.3 Generate Counters (Windowed 1 min)
## counters have:
    - Exactly-once
    - Windowed
    - Restart-safe
    - Datadog-enabled

## Valid Rows Counter
INSERT INTO dq_valid_metrics
SELECT
  'flink_onprem_job' AS job_name,
  TUMBLE_END(event_ts, INTERVAL '1' MINUTE) AS metric_ts,
  COUNT(*) AS valid_count
FROM dq_rules
WHERE dq_status = 'PASS'
GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE);

## Invalid Rows Counter
INSERT INTO dq_invalid_metrics
SELECT
  'flink_onprem_job' AS job_name,
  TUMBLE_END(event_ts, INTERVAL '1' MINUTE) AS metric_ts,
  COUNT(*) AS invalid_count
FROM dq_rules
WHERE dq_status <> 'PASS'
GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE);

-------------------------------------
## Kafka Sources (Isolated Topics)
CREATE TABLE kafka_onprem_source (
  id STRING,
  event_ts TIMESTAMP(3),
  value DOUBLE,
  raw_json STRING,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic_onprem_sales',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
-------------------------------------
CREATE TABLE kafka_cloud_source (
  id STRING,
  event_ts TIMESTAMP(3),
  value DOUBLE,
  raw_json STRING,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic_cloud_sales',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
## 1.4 Cassandra Metrics Sink (Optional) 
CREATE TABLE dq_metrics_cassandra (
  job_name STRING,
  metric_ts TIMESTAMP(3),
  metric_type STRING,
  metric_value BIGINT,
  PRIMARY KEY (job_name, metric_ts, metric_type) NOT ENFORCED
) WITH (
  'connector' = 'cassandra',
  'keyspace' = 'metrics',
  'table' = 'dq_counts',
  'hosts' = '10.10.0.5',
  'datacenter' = 'DC_ONPREM'
);

====================================
## 2. Zeppelin Notebook JSON:
>> Flink_DQ_OnPrem.json
>> Cloud Version change: 
    - Kafka topic
    - Cassandra sink hosts + datacenter
    - job_name string
>>     

=====================================
## Datadog
    1. Kafka metrics topics →
    2. Datadog Agent →
    3. Dashboards:
----------------------------------------------------
| Metric                | Meaning                  |
| --------------------- | ------------------------ |
| dq.valid.count        | Clean records per minute |
| dq.invalid.count      | Bad records per minute   |
| dq.invalid / dq.total | DQ breach ratio          |
| Zero valid rows       | Alert (sync stalled)     |
----------------------------------------------------

=====================================================
Target:
1. Flink SQL only	
2. One Cassandra DC per job	(DDL pinned)
3. Valid row counter
4. Invalid row counter
5. DLQ for bad data	
6. Zeppelin JSON-ready
7. Cloud & On-Prem matching

## Next Steps:


-------------------------------------
## 3. Data Quality Rules in Flink SQL
# 3.1 Canonical Record Hash (Generic for Any Table)
# Works for any table (hash definition is shared across jobs)
CREATE VIEW dq_enriched AS
SELECT
  id,
  event_ts,
  value,
  SHA256(
    CONCAT_WS('|',
      COALESCE(id, 'NULL'),
      COALESCE(CAST(value AS STRING), 'NULL'),
      CAST(event_ts AS STRING)
    )
  ) AS record_hash,
  raw_json
FROM kafka_onprem_source;
-------------------------------------

## 3.2 Rule Evaluation (Generic, Table-Agnostic)
CREATE VIEW dq_rules AS
SELECT
  *,
  CASE
    WHEN id IS NULL THEN 'FAIL_ID_NULL'
    WHEN value < 0 THEN 'FAIL_NEG_VALUE'
    WHEN event_ts IS NULL THEN 'FAIL_TS_NULL'
    ELSE 'PASS'
  END AS dq_status
FROM dq_enriched;


-----------------------------------------
## 3.3 Only Valid Records Go to Cassandra
INSERT INTO cassandra_onprem_sink
SELECT
  id,
  event_ts,
  value,
  record_hash
FROM dq_rules
WHERE dq_status = 'PASS';

------------------------------------------
## 4 Validate Cassandra DC at Runtime (SQL-Level Guard)
## >> Use deployment-time validation
>> Flink SQL cannot inspect Cassandra metadata.
# Validate Cassandra DC in CI/CD before executing SQL
    - Block the job if:
    - hosts ≠ expected subnet
    - datacenter option ≠ expected DC name
>> CI rule:
    grep -q "datacenter = 'DC_ONPREM'" flink_onprem.sql || exit 1    
    ( stronger than runtime checks in SQL )

## 5 Drift Detection (SQL, Safe)
>> Separate Flink SQL job (read-only)
--
CREATE VIEW unified_hashes AS
SELECT record_hash, 'ONPREM' AS source_dc FROM cassandra_onprem_hashes
UNION ALL
SELECT record_hash, 'CLOUD' AS source_dc FROM cassandra_cloud_hashes;

--
SELECT
  record_hash,
  COUNT(DISTINCT source_dc) AS dc_count
FROM unified_hashes
GROUP BY record_hash
HAVING dc_count > 1;
-------------------------------------------

## 6 Zeppelin Notebook Structure 
Each notebook:
    Runs one environment only
    Is config-locked
    Is reviewable & auditable
---------------------    
 01_sources.sql
 02_dq_rules.sql
 03_valid_inserts.sql
 04_invalid_dlq.sql
 05_drift_report.sql

## Steps
1. >> run:
    python3 tools/rules_to_sql.py rules.yaml > dq_results.sql
— or push directly into Zeppelin with --push-to-zeppelin.

2. >> Add ci/validate_flink_sql.sh into your CI pipeline before deploying Zeppelin SQL notebooks.

3. >>Wire Kafka topics and update hosts/datacenter options in the notebooks.
-----------------
Deploy Datadog agent or consume the metrics topics to feed dashboards.
- Generate Zeppelin JSON notebooks (importable)
- Provide CI/CD validation scripts
- Produce final SVG architecture diagram
- Convert your DQ YAML → Flink SQL automatically

==================================================================
## >> Prevent Common Mistakes (Anti-Patterns):
1. One Flink job writing to both Cassandra DCs
2. Cassandra multi-DC routing without explicit DC pinning
3. Shared secrets/config across jobs
4. Runtime discovery of Cassandra DC without validation
5. Drift detection using live dual-writes
## >> Following Corporate Standards and Best practices:
1. One Flink job per Cassandra DC
2. Kafka as the audit & comparison backbone
3. Hash-based reconciliation via Kafka + Flink SQL
4. Optional read-only audit service for controlled, short-lived comparisons
>> All that gives 
    - security isolation
    - blast-radius control (limiting potential damage from security bridge)
    - compliance approval.

## Anti-Pattern Explanation
1. One Flink job connecting to both DCs - Breaks DC isolation, audit risk
2. Cross-DC Cassandra drivers -	Violates Cassandra topology assumptions
3. Direct DB-to-DB diff jobs - Slow, expensive, unsafe
4. Periodic full table scans - Non-scalable, disruptive

==================================================================
## Audit for Cassandra ===========================================
==================================================================
INSERT into users_audit values('INSERTED', now(), 'dmitry', 100001, 'geservdrZp', 'ren-iac-admin', 'dmitry', 'Dmitry','Kolpakov', 'dmitry.kolpakov@ge.com', 1, now(), now(), now(), 'DK' );
INSERT into users_audit values('UPDATED', now(), 'dmitry', 100001, 'geservdrZp', 'ren-iac-admin', 'dmitry', 'Dmitry','Kolpakov', 'dmitry.kolpakov@ge.com', 2, now(), now(), now(), 'DK' );
INSERT into users values( 100002, 'tenantID', 'ren-iac-admin', 'dmitry3', 'Dmitry','Kolpakov', 'dmitry.kolpakov@ge.com', 1, now(), now(), now());
INSERT into users_calendar values( 100001, 'dmitry', now(), now());

DROP TABLE IF EXISTS users_audit CASCADE;
CREATE TABLE IF NOT EXISTS users_audit (
    operation    VARCHAR(8)   NOT NULL,
    stamp        timestamp NOT NULL,
    userid       text      NOT NULL,
  id integer PRIMARY KEY,
  tenant_id VARCHAR(50),
  oidc_name   VARCHAR(50),
  username  VARCHAR(50),
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email   VARCHAR(120),
  status  int,
  auth_date  VARCHAR(150),
  creation_date  timestamp,
  updation_date  timestamp,
  updated_by VARCHAR(50)
);

-- create Trigger
CREATE OR REPLACE FUNCTION process_users_audit() RETURNS TRIGGER AS $users_audit$
    BEGIN
        -- Create a row in users_audit to reflect the operation performed on users,
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO users_audit SELECT 'DELETED', now(), user, OLD.*;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO users_audit SELECT 'UPDATED', now(), user, NEW.*;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO users_audit SELECT 'INSERTED', now(), user, NEW.*;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$users_audit$ LANGUAGE plpgsql;

CREATE TRIGGER users_audit
AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION process_users_audit();
