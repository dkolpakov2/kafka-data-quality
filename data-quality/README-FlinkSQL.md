###  Kafka → Flink SQL

- Data Quality rules
- OnPrem vs Azure Cassandra
- Counters, reconciliation, drift detection

## TODO:
- Remove Flink interpreter from Zeppelin
- Deploy Flink SQL Gateway
- Generate SQL from your DQ rules
- Submit SQL via REST
- Use Zeppelin only to view results

## Target Architecture
Kafka
  ↓
Flink Cluster (JobManager + TaskManagers)
  ↑
Flink SQL Gateway (REST API)
  ↑
Zeppelin / curl / Python (submit SQL)

## Design Principles
  # Global Requirements
    Rule logic (when)
    Metric logic
    Severity
      Added rule severity aggregation (dq_error_total)
        Supported severities:
          ERROR
          WARN
          INFO
    Hash comparison logic
  # Table Specific Requirements
    Kafka topic
    Cassandra table
    PK column
    Hash fields
    DC / env tags 

## Approved Design:
Metrics are table-aware
Metrics are topic-aware
Tags are first-class (not baked into names)
One Flink job supports many tables
YAML-driven (no SQL rewrites)
Datadog-native observability
===============================================================================
## 1. RUN:
	docker-compose up -d

## 2. Verify:
	curl http://localhost:8083/info

	If Failed: then debug:
		docker-compose ps
		
## 3. Create a SQL Session (REQUIRED)
	curl -X POST http://localhost:8083/v1/sessions

## 4. Submit Flink SQL (Example)
	curl -X POST http://localhost:8083/v1/sessions/abc-123/statements \
	  -H "Content-Type: application/json" \
	  -d '{
		"statement": "SELECT 1"
	  }'

## 5. run python to create DB VIEWs by rules.yaml
  cd tools
  python rules_to_sql.py rules.yaml > dq_generated.sql
## 6. Then execute dq_generated.sql in:
  - Flink SQL Gateway
  - Zeppelin %flink.ssql
  - CI validation
  - Version control
## 7 It will Generates (Guaranteed)
  For each table:
    dq_results_<table>
    dq_metrics_<table>  

## 8. Kafka Source Table (Flink SQL)

CREATE TABLE kafka_source (
  id STRING,
  amount DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq-topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

----------- ## Send to Flink  

curl -X POST http://localhost:8083/v1/sessions/abc-123/statements \
  -H "Content-Type: application/json" \
  -d '{"statement": "CREATE TABLE kafka_source (...)" }'
  
## 9. Data Quality Counters (VALID / INVALID)
🔹 Create DQ View

CREATE VIEW dq_classified AS
SELECT
  *,
  CASE
    WHEN amount IS NULL OR amount < 0 THEN 'INVALID'
    ELSE 'VALID'
  END AS dq_status
FROM kafka_source;

🔹 Aggregate Counters

SELECT
  dq_status,
  COUNT(*) AS cnt
FROM dq_classified
GROUP BY dq_status;

## 10. Cassandra Sink (OnPrem / Cloud)

  CREATE TABLE cassandra_sink (
  id STRING,
  amount DOUBLE,
  ts TIMESTAMP(3),
  dq_status STRING,
  PRIMARY KEY (id)
) WITH (
  'connector' = 'cassandra',
  'hosts' = 'cassandra-onprem',
  'keyspace' = 'dq',
  'table' = 'results'
);

-- Insert into Cassandra
INSERT INTO cassandra_sink
SELECT id, amount, ts, dq_status
FROM dq_classified;

## 11. Zeppelin Integration (Optional & Safe)
-- Use REST via %sh or %http
>> Zeppelin paragraph:
%sh
curl -X POST http://sql-gateway:8083/v1/sessions \
  -H "Content-Type: application/json"

## 12. Two Pipelines (OnPrem vs Cloud)
--------------------------------------------------
| Pipeline | Kafka   | Flink SQL | Cassandra     |
| -------- | ------- | --------- | ------------- |
| OnPrem   | topic_1 | Job A     | Cassandra DC1 |
| Cloud    | topic_2 | Job B     | Cassandra DC2 |
--------------------------------------------------

## 14. Results:
Each SQL job:
	- Has its own Kafka topic
	- Has its own Cassandra sink
	- Runs on the same Flink cluster (or separate if needed)

######------------------------------------------##########
Comparison Zeppelin -> SQL Gateway
----------------------------------------------------------
| Zeppelin Flink Interpreter | SQL Gateway               |
| --Minuses----------------- | --Pluses----------------- |
| x launches local Flink     | + remote cluster          |
| x fragile in Docker        | + designed for containers |
| x unmaintained             | + active                  |
| x poor errors              | + REST diagnostics        |
----------------------------------------------------------	

## 7.1.  Datadog Mapping:
  dq.cassandra.delete_count
  dq.cassandra.missing_cloud
  dq.cassandra.hash_mismatch
  dq_error_total
  dq_warn_total
  dq_info_total
 # 7.2 DD Tags (AUTO)
    env:prod
    dc:onprem
    table:customer_onprem
    topic:onprem.events
 # 7.3. Datadog dashboards
    Filter by dc:azure
    Compare table:*
    Alert on hash_mismatch > 0

===========================================================

## Optional Optional (Next-Level Enhancements)
Severity-based metrics (dq_error_total, dq_warn_total)
Error rate (%) SQL
Per-PK sampling for audits
SLO SQL (availability & correctness)
Automatic Datadog alert templates
Multi-table YAML support


## 11. NEXT STEPS

 1. Generate full DQ SQL pipelines
 2. Convert YAML DQ rules → Flink SQL
 3. Add hash-based record reconciliation
 4. Add Cassandra drift detection SQL
 5. Add valid/invalid counters persisted to Cassandra
 6. Generate Zeppelin notebooks that call SQL Gateway
 7. Add Datadog metrics from Flink	

# V2 
  
  Generate Zeppelin notebook JSON
  Add rule versioning + audit history
  Add auto-DLQ routing SQL
  Error-rate % metrics
  Schema compatibility checks
  Unit tests for generator
  Zeppelin notebook JSON export

----------------- ------------------------------------- 
 