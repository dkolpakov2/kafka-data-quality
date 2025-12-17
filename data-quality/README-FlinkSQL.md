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
  
## 5. Kafka Source Table (Flink SQL)

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
  
## 6. Data Quality Counters (VALID / INVALID)
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

## 7. Cassandra Sink (OnPrem / Cloud)

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

## 8. Zeppelin Integration (Optional & Safe)
-- Use REST via %sh or %http
>> Zeppelin paragraph:
%sh
curl -X POST http://sql-gateway:8083/v1/sessions \
  -H "Content-Type: application/json"

##9. Two Pipelines (OnPrem vs Cloud)
--------------------------------------------------
| Pipeline | Kafka   | Flink SQL | Cassandra     |
| -------- | ------- | --------- | ------------- |
| OnPrem   | topic_1 | Job A     | Cassandra DC1 |
| Cloud    | topic_2 | Job B     | Cassandra DC2 |
--------------------------------------------------

## 10. Results:
Each SQL job:
	- Has its own Kafka topic
	- Has its own Cassandra sink
	- Runs on the same Flink cluster (or separate if needed)

######------------------------------------------##########
Comparison Zeppelin -> SQL Gateway
----------------------------------------------------------
| Zeppelin Flink Interpreter | SQL Gateway               |
| -------------------------- | ------------------------- |
| ❌ launches local Flink    | + remote cluster          |
| ❌ fragile in Docker       | + designed for containers |
| ❌ unmaintained            | + active                  |
| ❌ poor errors             | + REST diagnostics        |
----------------------------------------------------------	
## 11. NEXT STEPS

 1. Generate full DQ SQL pipelines
 2. Convert YAML DQ rules → Flink SQL
 3. Add hash-based record reconciliation
 4. Add Cassandra drift detection SQL
 5. Add valid/invalid counters persisted to Cassandra
 6. Generate Zeppelin notebooks that call SQL Gateway
 7. Add Datadog metrics from Flink	
 
------------------------------------------------------ 
 