
## Updated Architecture Logical Design
# High-level flow
Kafka Topic-1 (source events with hash)
   |
   v
Flink SQL Job: DQ Hash Check
   |
   |-- extract PK from partitionColumns
   |-- join with Topic-2 (Cassandra-derived hash)
   |-- compare hashes
   |-- maintain counters
   |
   +--> DataDog metrics

### Advantages for this Architecture ###

 No synchronous DB calls in Flink SQL
 Fully streaming, event-driven
 Scales independently
 Exactly-once hash comparison
 Clean DQ counters
 Cloud / OnPrem separation 
 Isolated plugins
 Time-windowed reconciliation
 Late-arrival handling
 Record-level audit sink
 Deterministic Hash Guaratees	 
 DataDog-friendly
   
## Detailed flow

1. Topic-1 message
Contains:
	Primary Key (PK)
	Hash computed on On-Prem side
	Metadata (partitionColumns)

	
2. Topic-2 message
Created by a separate pipeline:
	Extract PK
	Query Cassandra Cloud
	Serialize row
	Compute hash
	Publish {PK, cloud_hash} to Kafka Topic-2
	
2.2 Create DLQ Topic
	kafka-topics.sh --create \
	  --topic dlq-cassandra-cloud \
	  --bootstrap-server kafka:9092 \
	  --partitions 3 \
	  --replication-factor 1
	
	
3. ATTN: No Direct Access Flink SQL to Cassandra DB
	Cassandra access will be outside Flink SQL 
		Kafka Connect
		Microservice 
		Flink DataStream job
4. Kafka Message Contracts...
	Topic-1 (On-Prem source)	
	{
	  "event_id": "evt-001",
	  "hash_onprem": "9a3f8c...",
	  "partitionColumns": [
		{ "type": "PK", "value": "1234567" }
	  ],
	  "event_ts": "2025-01-16T10:00:00Z"
	}
----
	Topic-2 (Cloud Cassandra hash)
	{
	  "pk": "1234567",
	  "hash_cloud": "9a3f8c...",
	  "cloud_ts": "2025-01-16T10:00:02Z"
	}
==========
5. Flink SQL Job – Hash Validation Logic

Zeppelin:
5.1 Prerequizites:
	Topic-1 source
	
CREATE TABLE topic1_onprem (
  event_id STRING,
  hash_onprem STRING,
  partitionColumns ARRAY<ROW<type STRING, value STRING>>,
  event_ts TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);


		Topic 2 Cloud source
	
CREATE TABLE topic2_cloud (
  pk STRING,
  hash_cloud STRING,
  cloud_ts TIMESTAMP(3),
  WATERMARK FOR cloud_ts AS cloud_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

5.2 Extract PK from Topic-1 and Save it in VIEW:
	
CREATE VIEW topic1_with_pk AS
SELECT
  event_id,
  hash_onprem,
  pc.value AS pk,
  event_ts
FROM topic1_onprem,
LATERAL TABLE(UNNEST(partitionColumns)) AS T(pc)
WHERE pc.type = 'PK';


5.3. Hash Comparison - Core DQ Logic

CREATE VIEW dq_hash_check AS
SELECT
  t1.event_id,
  t1.pk,
  t1.hash_onprem,
  t2.hash_cloud,
  CASE
    WHEN t2.hash_cloud IS NULL THEN 'MISSING_CLOUD'
    WHEN t1.hash_onprem = t2.hash_cloud THEN 'MATCH'
    ELSE 'MISMATCH'
  END AS dq_status
FROM topic1_with_pk t1
LEFT JOIN topic2_cloud t2
ON t1.pk = t2.pk;

6. Counters:
	Valid
	Invalid
	Missing
	
6.1 Aggregate Counters by Streaming	

SELECT
  dq_status,
  COUNT(*) AS cnt
FROM dq_hash_check
GROUP BY dq_status;

6.1.1 Explanation Statuses:
	MATCH → valid
	MISMATCH → invalid
	MISSING_CLOUD → delayed / missing data
	
7. Sending Metrics to DataDog 

7.1 Kafka → DataDog Agent
	Flink SQL writes counters to Kafka
	DataDog Agent consumes & emits metrics	
	
7.1.2 Kafka Sink for metrics

CREATE TABLE dq_metrics_kafka (
  dq_status STRING,
  cnt BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq-metrics',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

7.1.3 INSERT for DQ 

INSERT INTO dq_metrics_kafka
SELECT dq_status, COUNT(*) AS cnt
FROM dq_hash_check
GROUP BY dq_status;

7.1.4. DataDog Agent:
kafka_consumer:
  topics:
    - dq-metrics
	
7.2. Other Option: Flink Metrics → DataDog
	Use Flink metric reporters
	Map dq_status counters to custom metrics
	Requires Flink config (no SQL)
	
-----------------------------------------------------------
8. Cassandra Cloud Hash Producer 
ATTN: this is separate component Outside Flink SQL
8.0 Architecture: Kafka Connect Cassandra → Topic-2 config
Cassandra Cloud
   ↓
Kafka Connect (Cassandra Source Connector)
   ↓
Topic-2 (pk, hash_cloud)
   ↓
Flink SQL (hash comparison)

8.1 Decoupling is Important for scalability.
	Based on:
		Python
		Java
		Kafka Connect

   Data:			
	pk = extract_pk(msg)
	row = cassandra.get(pk)
	hash_cloud = sha256(json.dumps(row))
	produce(topic2, { "pk": pk, "hash_cloud": hash_cloud })

8.2 Deterministic Hash Guarantees as follows
  Field ordering
  Null-safe
  Repeatable
  Cross-DC consistency
  DLQ compatible

8.2 Prerequisites
Kafka Connect cluster must have Required JARs
	DataStax Cassandra Source Connector or
	Apache Cassandra Kafka Connector
	Custom SMT JAR for hashing (recommended)

Folder structure:
 /kafka/connect/plugins/
 ├── cassandra-source/
 ├── custom-hash-smt.jar

8.3 Create Kafka Topic 
	kafka-topics.sh --create \
  --topic topic2-cloud-hash \
  --bootstrap-server kafka:9092 \
  --partitions 6 \
  --replication-factor 1

	
8.4 Create Cassandra Source connector
Connector will do next steps:
------------------------------------------------
| Step | Action                                |
| ---- | ------------------------------------- |
| 1    | Poll Cassandra Cloud table            |
| 2    | Read row by PK                        |
| 3    | Extract PK as Kafka message key       |
| 4    | Compute SHA-256 hash from row columns |
| 5    | Emit `{ pk, hash_cloud }`             |
| 6    | Publish to Topic2                     |
------------------------------------------------

cassandra-cloud-source.json

{
  "name": "cassandra-cloud-hash-source",
  "config": {
    "connector.class": "com.datastax.oss.kafka.source.CassandraSourceConnector",
    "tasks.max": "4",

    "contact.points": "cassandra-cloud",
    "load.balancing.local.dc": "dc1",
    "port": "9042",

    "keyspace": "cloud_keyspace",
    "table": "business_table",

    "topic.prefix": "raw-cloud-",

    "poll.interval.ms": "5000",
    "timestamp.column.name": "last_updated",
    "mode": "timestamp",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "transforms": "ExtractPK,ComputeHash,RouteTopic",

    "transforms.ExtractPK.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.ExtractPK.fields": "pk",

    "transforms.ComputeHash.type": "com.company.kafka.connect.smt.HashRowSMT",
    "transforms.ComputeHash.hash.fields": "col1,col2,col3",
    "transforms.ComputeHash.hash.algorithm": "SHA-256",
    "transforms.ComputeHash.output.field": "hash_cloud",

    "transforms.RouteTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.RouteTopic.regex": "raw-cloud-.*",
    "transforms.RouteTopic.replacement": "topic2-cloud-hash",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-cassandra-cloud",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}


8.4 .4. Deploy Cassandra connector

	curl -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @cassandra-cloud-source.json

Verify
	curl http://connect:8083/connectors/cassandra-cloud-hash-source/status


8.5 Custom Hash 
	Based on:
		Python
		Java
		Kafka Connect
	SMT Coverage includes
		Serialize selected columns
		Normalize field order
		Compute hash
		Inject into value

9. Failure & Edge-Case Handling for Cassandra:

errors.tolerance=all							Connector keeps running on bad records
errors.deadletterqueue.topic.name				Sends failed records to DLQ
errors.deadletterqueue.context.headers.enable	Adds error metadata (stack trace, stage)
errors.log.enable								Logs errors
errors.log.include.messages						Logs full record payload


10. Check for DLQ:
	When Failed stage it will follow =  Cassandra → SMT → Kafka
	
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic dlq-cassandra-cloud \
  --from-beginning

11. Maintain Kafka DLQ = add (wil lgive time to retry before commit to DLQ):
"errors.retry.timeout": "600000",
"errors.retry.delay.max.ms": "30000"

12. Failure Scenarios

Action-Type Scenarios (CREATE / UPDATE / DELETE)
Action	Expected Behavior
CREATE	Compare hashes
UPDATE	Compare hashes
DELETE	No hash comparison
UPSERT	Compare hashes
UNKNOWN	Route to DLQ

Flink Logic as Follows:
CASE
  WHEN action = 'DELETE' THEN 'SKIPPED_DELETE'
  WHEN hash_cloud IS NULL THEN 'MISSING_CLOUD'
  WHEN hash_onprem = hash_cloud THEN 'MATCH'
  ELSE 'MISMATCH'
END


12.1 Message-Level Failures (Kafka Topic-1 / Topic-2)
1. Message-Level Failures (Kafka Topic-1 / Topic-2)
1.1 DELETE Events
  Expected Behavior:
    Do NOT hash
    Do NOT compare
    Track as valid delete
 # Flink SQL: 
  CASE
    WHEN action = 'DELETE' THEN 'DELETE_EVENT'
  Add Metrics
    dq_delete_count  

1.2 Missing PK
  Handling
    Fail fast
    Route to DLQ
    Increment dq_missing_pk

1.3 Duplicate Messages when Kafka will replay / retry
  Handling
    Deduplicate in Flink SQL using PK + timestamp
    Metric: dq_duplicates

1.4 Invalid Hash Format
  Handling
    Regex check in Flink SQL
    Metric: dq_hash_format_error

2. Cassandra Cloud Failure Scenarios (Topic-2 Producer)    
2.1 Row Not Found in Cassandra
  Scenario
    PK exists on-prem
    Missing in cloud
  Handling
    Produce { pk, hash_cloud = null }
    Flink SQL → MISSING_CLOUD
    Metric: dq_missing_cloud

2.2 Cassandra Timeout / Unavailable
  Handling
    Kafka Connect retries (limited number ~ 3)
    DLQ if exhausted
    Metric: dq_cassandra_timeout

2.3 Partial Row (Null Columns)
  Handling
    Hash normalization converts null → empty
    Still comparable
    Metric: dq_partial_row

3. Hash Comparison Failures (Core DQ)
3.1 Hash Mismatch => Primary DQ Signal
  Scenario
    hash_onprem != hash_cloud
  Handling
    dq_status = MISMATCH
    Metric: dq_hash_mismatch

3.2 Hash Match 
  Handling
    dq_status = MATCH
    Metric: dq_hash_match

3.3 Late Cloud Data
  Scenario
    Topic-1 arrives before Topic-2
  Handling
    LEFT JOIN with grace window
    Reconcile when Topic-2 arrives
    Metric: dq_late_arrival

--------
Kafka Connect SMT Failure Scenarios
5.1 Missing Hash Fields 

Handling
  SMT throws exception
  Kafka Connect → DLQ
  Metric: dq_smt_missing_fields

5.2 Unsupported Hash Algorithm 
Handling
  Config validation fails
  Connector won’t start

6. Schema / Contract Drift
6.1 New Column Added in Cassandra
Handling
  Ignored unless added to hash.fields
  Metric: dq_schema_drift_detected

6.2 Column Removed 
Handling
  Hash changes
  Flag as MISMATCH
  Metric: dq_column_removed

7. Operational Failures
7.1 Kafka Lag

Handling
  Monitor consumer lag
  Metric: dq_kafka_lag

7.2 Flink Backpressure 
  Handling
    Datadog Flink metrics
    Metric: dq_backpressure

### Summary of DataDog Metrics    
Metric	
dq_hash_match	
dq_hash_mismatch	
dq_missing_cloud	
dq_delete_count	
dq_duplicates	
dq_invalid_hash	
dq_late_arrival	
dq_smt_errors	
dq_dlq_records	
===============================================================

777. Steps:
Generate Kafka Connect Cassandra → Topic-2 config
Generate Python Cassandra hash producer
Generate full Flink SQL Gateway submission scripts
Generate Zeppelin JDBC notebooks
Add time-windowed reconciliation
Add late-arrival handling
Add record-level audit sink	


Generate a ready-to-run Kafka Connect Docker image
Generate the Hash SMT Maven project
Add Schema Registry support
Add exactly-once semantics

For Failure scenarios:
  Time-windowed reconciliation logic
  Delete tombstone propagation
  Versioned hash strategy
  Audit table in Cassandra
  Auto-remediation workflows
  SLA / SLO definitions
================================================================

Java SMT:
public class HashRowSMT<R extends ConnectRecord<R>> implements Transformation<R> {

  @Override
  public R apply(R record) {
    Map<String, Object> value = (Map<String, Object>) record.value();
    String canonical = value.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(e -> e.getValue().toString())
        .collect(Collectors.joining("|"));

    String hash = DigestUtils.sha256Hex(canonical);
    value.put("hash_cloud", hash);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        value,
        record.timestamp()
    );
  }
}

==========================================================