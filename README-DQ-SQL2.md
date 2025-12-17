## Kafka ->Flink SQL + Zeppelin + Cassandra DC (Cloud/onPrem)
>> One Flink job → one Kafka topic
- One Flink job → exactly one Cassandra data center
- No cross-DC writes
- Full Data Quality (DQ) + hashing
- Drift validation without dual connections

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
     │  (Zeppelin)    │              │  (Zeppelin)    │
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
## Result:
## 1 Two independent Cassandra datacenters
  - DC_ONPREM
  - DC_AZURE
# 2 Zeppelin notebooks you already imported
# 3 Flink SQL job execution via Zeppelin
  docker-compose up -d --scale taskmanager=4
# 4 Kafka source → Flink SQL → Cassandra sinks
# 5 DLQ topics
------------------------
A Makefile wrapping the entire deploy:
>> 
  make up
  make seed-kafka
  make run-dq
-------------------------
## Usage notes
  1. Start the local stack:
    # > (from repo root where your docker-compose.yml lives).
      docker-compose up -d --build 
    # push image
      docker image push [OPTIONS] NAME[:TAG]
      docker image tag confluentinc/cp-zookeeper:7.5.0 zookeeper:7.5.0
      docker image tag obsidiandynamics/kafdrop kafdrop:latest
      docker image tag python:3.10-slim kafka-seeds:latest
	  docker image tag cassandra:4.1  cassandra_onprem:4.1
	  docker image tag apache/zeppelin:0.10.1 zeppelin:0.10.1

## What the Error Means => WSL is already installed
# The service cannot be started because it is disabled
  -- Means one (or more) of these is disabled:
  Windows Subsystem for Linux
  Virtual Machine Platform
  Hyper-V
  BIOS virtualization
  WSL service stopped      
  ## FIX: Open PowerShell as Administrator and run:
  1.
  >> dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
  >> dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
  for Docker Desktop:
  >> dism.exe /online /enable-feature /featurename:Hyper-V /all /norestart
  2. Restart Windows (MANDATORY)
  3. Verify by powerShell:
		>> sc query LxssManager
    # Output:
      STATE  : 4  RUNNING
	# If Stopped then run:
		>> sc start LxssManager	
  4. Check BIOS Virtualization (VERY COMMON ISSUE)
    Enable in BIOS:
      Intel: Intel VT-x
      AMD: SVM Mode
    4.1. check 
      >> systeminfo | findstr /i "Virtualization"
   5. Set WSL2 as Default
      >> wsl --set-default-version 2
      >> wsl --status
      >> wsl --list --online
  ---------------------------------------------
  
============================== Flink inside the Zeppelin container  ==============================  
### ATTN: Zeppelin MUST install Flink inside the Zeppelin container.
		This will avoid all Docker volume below issues.
==================================================================================================		
		
	## WARN error when run in Zeppelin 

    # Shot Down docker:
      docker-compose down -v
      update docker-compose.yaml add Flink 
	  zeppelin:
		build:
			context: .
			dockerfile: Dockerfile-zeppelin-flink
		container_name: zeppelin
		ports:
			- "8080:8080"
		  environment:
			ZEPPELIN_JAVA_OPTS: >
			  -DFLINK_HOME=/opt/flink-1.17.1
			  -Dzeppelin.flink.useLocalFlink=true
		  depends_on:
			- jobmanager		
	# next: docker-compose down -v
	 docker-compose build --no-cache zeppelin
	 docker-compose up -d
	 

      # Now Zeppelin will find /opt/flink/bin/sql-client.sh and the Flink interpreter will WORK.
    # NOT WORKING: in Zeppelin Interpreter possibly add:  
      FLINK_CONF_DIR = /opt/flink/conf
    # Then neeVerify Flink Interpreter Files
		Inside Zeppelin container:
			docker exec -it zeppelin ls /opt/zeppelin/interpreter/flink
		You should see JARs.
		!!! If empty → interpreter not installed.
	# OR Other way:
		docker exec -it zeppelin ls $FLINK_HOME/lib | grep flink-dist	

		echo $FLINK_HOME
		ls $FLINK_HOME/lib | grep flink-dist
   # Add the Flink jar to kafka-data-quality/data-quality/flink-1.17.1 folder to mount inside Zeppilin :
      - Pool Flink to local to be in Zeppilin access
        wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
        tar -xzf flink-1.17.1-bin-scala_2.12.tgz

      - new folder will be created:
        /kafka-data-quality/data-quality/flink-1.17.1
    # Fix Step 2 — Install Zeppelin Flink Interpreter Plugin
      - add Dockerfile to docker-compose:
    >> yaml
      zeppelin:
        build:
          context: .
          dockerfile: Dockerfile-zeppelin-flink
    # Fix Step 3 — Enable Interpreter Group in Zeppelin UI
        Open Zeppelin UI → Interpreter → search “Flink”
          We should now see:
            flink: %flink, %flink.bsql, %flink.ssql, %flink.pyflink, %flink.ipyflink
   # Restart Docker:
		docker-compose ps   ## will show all running containers
		docker-compose down
		docker-compose build zeppelin

        docker-compose up -d    ## --build
		docker-compose restart zeppelin

    make build-jar (requires Maven and the Java project present).
  3. Submit jobs:
    make submit-onprem or make submit-cloud (ensure FLINK_REST env points to JobManager).
  4. CI validation:
    make validate-ci (runs simple checks on Zeppelin SQL notebooks).
  5. Datadog:
    Use monitoring/datadog_dq_dashboard.json & monitoring/datadog_monitors.json to import into Datadog.

=============================================================

Metrics topics for Datadog
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
>>  Flink_DQ_CLoud.json
    (Minimal Changes)
      Only update:
        Kafka topic
        Cassandra sink hosts + datacenter
        job_name string
    Everything else is reusable !!!

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
  docker-compose up -d --scale taskmanager=4

2. One Cassandra DC per job	(DDL pinned)
3. Valid row counter
4. Invalid row counter
5. DLQ for bad data	
6. Zeppelin JSON-ready
7. Cloud & On-Prem matching

## Next Steps:
  - Import the Zeppelin notebooks (Zeppelin UI → Import note → upload JSON).
  - Place rules.yaml alongside tools/rules_to_sql.py and run:
  - python3 tools/rules_to_sql.py rules.yaml > dq_results.sql
    — or push directly into Zeppelin with --push-to-zeppelin.
  - Add ci/validate_flink_sql.sh into your CI pipeline before deploying Zeppelin SQL notebooks.
  - Wire Kafka topics and update hosts/datacenter options in the notebooks.
  - Deploy Datadog agent or consume the metrics topics to feed dashboards.
  Optional next steps:
  - Wire the generator to fetch rules.yaml from your Git repo automatically.
  - Convert datadog_dq_dashboard.json into an importable Datadog dashboard via API calls.
  - Add unit tests for rules_to_sql.py.

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

## FIX: FLINK_HOME is not specified    
## Set in Zeppelin Interpreter Settings:
## Flink Plugins in Interpreter UI: %flink.ssql, %flink.pyflink, %flink.bsql
  1. Navigate to the Interpreter menu in the Zeppelin UI.
  2. Find the flink interpreter and click edit.
  3. Add a new property or modify the existing one with FLINK_HOME as the name and the path to your Flink directory (e.g., /opt/flink or C:\flink_home) as the value.
  4. Save the settings and restart the interpreter.
## Set in Environment Variables (for all interpreters/system-wide):
  1. Edit the zeppelin-env.sh file located in the $ZEPPELIN_HOME/conf directory.
  2. Add the line export FLINK_HOME=/path/to/your/flink/home.  /opt/flink
  3. Restart the entire Zeppelin server for the changes to take effect    
  
##  Inside Zeppelin container:
	docker exec -it zeppelin ls /opt/zeppelin/interpreter/flink

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
## Create Kafka Topics:
 3.1 Get shell into Kafka container:
    docker exec -it kafka bash

 3.1.2. Delete Topic to clean before:
	kafka-topics \
		--bootstrap-server kafka:9092 \
		--delete \
		--topic topic_onprem_sales
  3.1.3. Create topic:
	  kafka-topics \
	  --bootstrap-server kafka:9092 \
	  --create \
	  --topic topic_onprem_sales \
	  --partitions 1 \
	  --replication-factor 1
  #### If Retention in loop 1 sec:
	kafka-configs \
	  --bootstrap-server kafka:9092 \
	  --entity-type topics \
	  --entity-name topic_onprem_sales \
	  --alter \
	  --add-config retention.ms=1000  
	## Then remove retention:
	kafka-configs \
	  --bootstrap-server kafka:9092 \
	  --entity-type topics \
	  --entity-name topic_onprem_sales \
	  --alter \
	  --delete-config retention.ms
	## Verify Topic is Empty:
	kafka-console-consumer \
	  --bootstrap-server kafka:9092 \
	  --topic topic_onprem_sales \
	  --from-beginning
	## Consumer offsets are NOT reset automatically.
		## To reset consumer offsets:
		kafka-consumer-groups \
		  --bootstrap-server kafka:9092 \
		  --group flink-dq-onprem \
		  --reset-offsets \
		  --to-earliest \
		  --execute \
		  --topic topic_onprem_sales
------------ 
 3.2 Produce to a topic:
    kafka-console-producer --broker-list kafka:9092 --topic topic_onprem_sales

 3.3 Create messages:
    {"id":"1","event_ts":"2025-01-01T00:00:00Z","value":100}
    {"id":"2","event_ts":"2025-01-01T00:00:01Z","value":-50}
## Option 2 — Create topic first (if needed)
    Inside Kafka container:
    >> kafka-topics --create --topic topic_onprem_sales \
      --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
## Option 3 — Produce messages from host (Docker Desktop)
    docker exec -it kafka \
    kafka-console-producer --broker-list kafka:9092 --topic topic_onprem_sales
## Option 4 — Produce messages with Python (very useful for testing)
  >> pip install kafka-python
  >> run: python producer.py
  ------------------- producer.py -----------------------
    from kafka import KafkaProducer
    import json
    import time

    p = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    messages = [
        {"id": "10", "event_ts": "2025-01-01T00:00:00Z", "value": 100},
        {"id": "11", "event_ts": "2025-01-01T00:00:05Z", "value": -20},
    ]

    for m in messages:
        print("Sending:", m)
        p.send("topic_onprem_sales", m)

    p.flush()
-------------------
Option 5 — Post messages via REST Proxy (if you want HTTP)
  If you add the Confluent REST Proxy to your docker-compose:    
>>
  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.5.0
    depends_on:
      - kafka
    ports:
      - "8082:8082"
    environment:
      KAFKA_REST_BOOTSTRAP_SERVERS: "kafka:9092"
      KAFKA_REST_LISTENERS: "http://0.0.0.0:8082"
--
>> Then you can POST messages like this:
  curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
  --data '{"records":[{"value":{"id":"3","value":15}}]}' \
  http://localhost:8082/topics/topic_onprem_sales
-------
+ Step To Add Kafka Seeder:
  docker-compose up -d kafka-seeder
  ## Monitor
  docker logs -f kafka-seeder

------------------
## I >> Check Kafka Messages after checking kafka-seeder =>10 messages:
10. 
10.1. Option 1: Use kafka-console-consumer:
    Enter into Kafka container
    docker container ls --filter "status=running"
  >> docker exec -it kafka bash
  >> docker exec -it <KAFKA-container-id> BASH
  >> kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic topic_onprem_sales \
      --from-beginning
  >> 
10.2 Option 2: Option B — Verify topic exists
  >> bash inside container:
  >> kafka-topics --bootstrap-server kafka:9092 --list

10.3 Option 3: Inspect consumer groups (Flink)
  >> bash:
  kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --list
----------
# II >> # Check Flink group offsets:
  kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group flink-dq-onprem \
  --describe
# Check Flink Job Status
# Open Flink UI
http://localhost:8081
 -- Verify:
  Job is RUNNING
  No FAILED tasks
  Kafka source offsets advancing
-----------------------------------
## III >> Check Cassandra Entries
- Enter Cassandra container 
docker exec -it cassandra-onprem cqlsh
  >> List keyspaces:
    DESCRIBE KEYSPACES;
  >> Use your keyspace:
    USE dq_keyspace;
  >> List tables:
    DESCRIBE TABLES;
  >> Query data:
    SELECT * FROM sales_events LIMIT 10;
  >> Count records:
    SELECT COUNT(*) FROM sales_events;
# 
--------------------------------------
## IV >> Validate Data Quality Counters
# If you created counters in Flink SQL:
# Check via Flink UI
    Task → Metrics -> Look for:
        - dq_valid_count
        - dq_invalid_count
  >> REST:
  curl http://localhost:8081/jobs/<jobid>/metrics

---------------------------------------
## V >> Compare On-Prem vs Cloud Cassandra
# Repeat same steps for cloud Cassandra container:
docker exec -it cassandra-cloud cqlsh
USE dq_keyspace;
SELECT COUNT(*) FROM sales_events;

---------------------------------------
## VI >> Optional (Highly Recommended) — Add Kafka UI
# Add to docker-compose
kafdrop:
  image: obsidiandynamics/kafdrop
  depends_on:
    - kafka
  ports:
    - "9000:9000"
  environment:
    KAFKA_BROKERCONNECT: kafka:9092
## GO to UI and Browse for Topics/Messages/Offsets/Schemas:
  http://localhost:9000

## Zeppelin: http://localhost:8080/#/notebook/2MD56Q4ZV
>> http://localhost:8080/#/notebook/2MD56Q4ZV


#
# --------------------------------------------------------------------
## NEXT:
 - Add hash-based reconciliation queries between Cassandra DCs
 - Add DQ dashboards in Datadog
 - Add automatic data drift detection
 - Add Zeppelin SQL notebooks to validate counts visually

====================================================================
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
====================================================================
## NEXT STEPS MONTORING
## DATA DOG Service
====================================================================

----------------
Deploy Datadog agent or consume the metrics topics to feed dashboards.
- Generate Zeppelin JSON notebooks (importable)
- Provide CI/CD validation scripts
- Produce final SVG architecture diagram
- Convert your DQ YAML → Flink SQL automatically

4cd ... Start all APPS in docker:
    >>    docker-compose up -d --build

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





### OUT OF SCOPE

## remove cached errored file
  git rm --cached data-quality/flink-1.17.1/lib/flink-dist-1.17.1.jar
  echo "*.jar" >> .gitignore
  git add .gitignore
  git commit -m "Ignore JAR artifacts"
  ## Remove jar form ALL history
  pip install git-filter-repo
  git filter-repo --path flink-dist-1.17.1.jar --invert-paths
  git push origin master --force
#
#
#
#
#
#
#
===================================================================
Out of scope for sometime, issues exist (sloweness )
===================================================================
## Architecture
Cassandra - DEBEZIUM -> Kafka- Flink SQL
-------------------------------------------------------------------
1. No direct DB-to-DB connection
2. Immutable audit trail
3. Real-time or near-real-time
4. Works with Zeppelin SQL
-------------------
## This Is the BEST Solution
----------------------------
| Requirement        | Met |
| ------------------ | --- |
| No direct DB-to-DB | Y   |
| Real-time          | Y   |
| Zeppelin SQL       | Y   |
| Immutable audit    | Y   |
| Scales             | Y   |
| Security           | Y   |
| Compliance-ready   | Y   |
----------------------------
## ATTN:
## Anti-patterns to Avoid
>> Hashing inside Cassandra triggers
>> Mixing hash logic between DCs
>> Ignoring event time skew
>> Cross-DC database drivers
>> Full table scans

## TODO
Generate Debezium Cassandra connector config
Provide a Zeppelin notebook JSON for this exact flow
Add CI validation rules
Add auto-remediation logic

------------------------------
## Step 1 — Capture Cassandra Updates → Kafka
   Option A (BEST): CDC via Debezium + Cassandra CommitLog
    Recommended for real-time audits
##  Components
  - Debezium Cassandra Connector
  - Kafka Connect cluster (on-prem)
>> What Happens = 
  Pushes to Kafka topic:
    onprem.cdc.sales.events
  Reads Cassandra CommitLog
  Emits INSERT / UPDATE events
  No polling
  Minimal load
  Example Debezium Event:
  >> {
  "key": { "id": "123" },
  "value": {
    "after": {
      "id": "123",
      "amount": 100,
      "event_ts": "2025-01-01T10:00:00"
    },
    "op": "u",
    "ts_ms": 1730000000000
  }
}
---------------------------------
## Step 2 — Hash the Record (Kafka → Flink SQL)
  Flink SQL (Zeppelin) — On-Prem Side
>> SQL
CREATE TABLE cass_onprem_cdc (
  id STRING,
  amount DOUBLE,
  event_ts TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'onprem.cdc.sales.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
--------------------
# Canonical Hash View
  - Must be identical on both sides
>> SQL
CREATE VIEW onprem_hashed AS
SELECT
  id,
  SHA256(
    CONCAT_WS('|',
      COALESCE(id,'NULL'),
      COALESCE(CAST(amount AS STRING),'NULL'),
      CAST(event_ts AS STRING)
    )
  ) AS row_hash,
  event_ts
FROM cass_onprem_cdc;
------------------------
# Publish Hash to Kafka Audit Topic
-- This topic becomes your immutable audit stream
>>SQL
CREATE TABLE onprem_hash_topic (
  id STRING,
  row_hash STRING,
  source_dc STRING,
  event_ts TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'onprem_hash_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO onprem_hash_topic
SELECT
  id,
  row_hash,
  'ONPREM' AS source_dc,
  event_ts
FROM onprem_hashed;
----------------------------------
## Step 3 — Cloud Side (Same Pattern)

## Step 4 — Compare Two Kafka Topics in Flink SQL
CREATE TABLE onprem_hash (
  id STRING,
  row_hash STRING,
  event_ts TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'onprem_hash_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE cloud_hash (
  id STRING,
  row_hash STRING,
  event_ts TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'cloud_hash_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
-------------------------
## 5 Windowed Join (Handling Timing Differences)
  - Tolerates replication delay
  - Works in Zeppelin SQL
>> SQL
CREATE VIEW reconciliation AS
SELECT
  COALESCE(o.id, c.id) AS id,
  o.row_hash AS onprem_hash,
  c.row_hash AS cloud_hash,
  CASE
    WHEN o.row_hash IS NULL THEN 'MISSING_ONPREM'
    WHEN c.row_hash IS NULL THEN 'MISSING_CLOUD'
    WHEN o.row_hash <> c.row_hash THEN 'DRIFT'
    ELSE 'MATCH'
  END AS status
FROM onprem_hash o
FULL OUTER JOIN cloud_hash c
ON o.id = c.id
AND o.event_ts BETWEEN c.event_ts - INTERVAL '1' MINUTE
                   AND c.event_ts + INTERVAL '1' MINUTE;

## 6 Step 5 — Persist Audit Results
>> SQL
CREATE TABLE dq_reconciliation_audit (
  id STRING,
  onprem_hash STRING,
  cloud_hash STRING,
  status STRING,
  audit_ts TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq_reconciliation_audit',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO dq_reconciliation_audit
SELECT
  id,
  onprem_hash,
  cloud_hash,
  status,
  CURRENT_TIMESTAMP
FROM reconciliation
WHERE status <> 'MATCH';

---------------------------------






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
========== DONE ==================================================
==================================================================