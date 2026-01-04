###### . Data Flow Summary
Producer sends events → topic1
Cassandra holds reference truth
Flink SQL joins using selector
Data quality rules evaluated
Results:
    Kafka topic_dq_results
    DataDog metrics
Zeppelin queries Flink in real time

https://zeppelin.apache.org/docs/0.11.2/interpreter/flink.html

## Run Docker Zeppelin + Flink: 
---------------
## Download Flink 1.15 or afterwards (Only Scala 2.12 is supported)

docker run -u $(id -u) -p 8080:8080 --rm -v /mnt/disk1/flink-sql-cookbook-on-zeppelin:/notebook -v /mnt/disk1/flink-1.12.2:/opt/flink -e FLINK_HOME=/opt/flink  -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.11.2

## Version-specific notes for Flink
Flink 1.15 is scala free and has changed its binary distribution, the following extra steps is required. 
* Move FLINKHOME/opt/flink-table-planner2.12-1.15.0.jar to FLINKHOME/lib 
* Move FLINKHOME/lib/flink-table-planner-loader-1.15.0.jar to FLINKHOME/opt 
* Download flink-table-api-scala-bridge2.12-1.15.0.jar and flink-table-api-scala2.12-1.15.0.jar to FLINKHOME/lib


## Flink 1.16 introduces new ClientResourceManager for sql client, you need to:
	move FLINK_HOME/opt/flink-sql-client-1.16.0.jar to FLINK_HOME/lib

### RUN: from parent folder: dataquality-kafka-project:
docker build --no-cache -f zeppelin/Dockerfile-zeppelin-flink -t zeppelin:1.17.1-flink .

 docker-compose build --no-cache zeppelin
	
 docker-compose up -d zeppelin

docker run -u  -p 8080:8080 --rm -v /mnt/disk1/flink-sql-cookbook-on-zeppelin:/notebook -v /mnt/disk1/flink-1.12.2:/opt/flink -e FLINK_HOME=/opt/flink  -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.11.2

## Clean Docker Cache:
 Add the --force or -f flag to bypass the confirmation prompt 
	docker system prune -a --volumes -f
  command removes dangling (unreferenced) build cache data
	docker builder prune 
  Stopped Containers: Use to remove all stopped containers.
	docker container prune 
  Unused Images: The command removes dangling images
	docker image prune 
	
  Unused Volumes: The command removes unused volumes. 	
	docker volume prune 
	
 TO check space for Images, Containers, local Volumes, Build Cache:
	docker system df
-------------------------
docker build --no-cache -f Dockerfile-zeppelin-flink -t zeppelin:0.11.2 .	

-------------------------------------
## 3. Data Quality Rules (Example)
| Rule         | Description                 |
| ------------ | --------------------------- |
| PK not null  | `selector IS NOT NULL`      |
| Amount match | `amount == expected_amount` |
| Status match | `status == expected_status` |
| Freshness    | event_time within SLA       |
| Completeness | all required fields present |

## 4. Flink SQL (SSQL)
# 4.1 Source Table – Kafka topic1
CREATE TABLE source_events (
  selector STRING,
  event_time TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

# 4.2 Cassandra Reference Table
CREATE TABLE cassandra_ref (
  selector STRING,
  expected_amount DECIMAL(10,2),
  expected_status STRING,
  PRIMARY KEY (selector) NOT ENFORCED
) WITH (
  'connector' = 'cassandra',
  'keyspace' = 'dq',
  'table' = 'reference_data',
  'hosts' = 'cassandra'
);

# 4.3 Kafka topic2 (Optional CDC Stream)
CREATE TABLE cassandra_events (
  selector STRING,
  expected_amount DECIMAL(10,2),
  expected_status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

# 4.4 Data Quality Join & Validation
CREATE VIEW dq_validation AS
SELECT
  s.selector,
  s.amount,
  r.expected_amount,
  s.status,
  r.expected_status,
  s.event_time,

  CASE
    WHEN s.selector IS NULL THEN 'FAIL_PK_NULL'
    WHEN s.amount != r.expected_amount THEN 'FAIL_AMOUNT_MISMATCH'
    WHEN s.status != r.expected_status THEN 'FAIL_STATUS_MISMATCH'
    ELSE 'PASS'
  END AS dq_status

FROM source_events s
LEFT JOIN cassandra_ref r
ON s.selector = r.selector;

# 4.5 Sink – Kafka topic_dq_results
CREATE TABLE dq_results (
  selector STRING,
  dq_status STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic_dq_results',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
-----
INSERT INTO dq_results
SELECT selector, dq_status, event_time
FROM dq_validation;

## 5. DataDog Metrics Sink (via Flink)
Metric Examples
    dq.records.total
    dq.records.pass
    dq.records.fail

Flink SQL → DataDog via Side Output (Java/Scala job)
If SQL-only is required, use Kafka → DataDog Agent instead.

## 6. Zeppelin Configuration
Interpreter
    Flink SQL Interpreter
    Point to:
        FLINK_MASTER=flink-jobmanager
Notebook
    %flink.ssql
    SHOW TABLES;
    ------------
    %flink.ssql
    SELECT dq_status, COUNT(*) 
    FROM dq_validation
    GROUP BY dq_status;    


### #############  ERROR  ########################################
# Zeppelin tries to:
Locate a Flink installation
Find a Flink application JAR (flink-sql-client / flink-table-planner)
Launch it as a subprocess
Because no Flink JARs are present or configured, Zeppelin passes null  NPE.
This is not your SQL, and not Kafka / Cassandra related.



## 9. Extensions (Next Steps)
    Add schema registry
    Use Kafka Connect CDC from Cassandra
    Add Great Expectations-style rules
    Persist DQ failures to Iceberg / S3
    2Create Add alerting in DataDog