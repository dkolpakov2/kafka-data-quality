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
 Clean Docker Cache:
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
-------------------------
## Usage notes
  1. Start the local stack:
    # > (from repo root where your docker-compose.yml lives).
      docker-compose up -d --build 
	  docker build --no-cache -f Dockerfile.flink -t flink-sql-gateway:1.17.1-kafka .
	  docker-compose build --no-cache zeppelin
    # push image
		docker image push flink-sql-gateway:1.17.1-kafka
      docker image push [OPTIONS] NAME[:TAG]
      docker image tag confluentinc/cp-zookeeper:7.5.0 zookeeper:7.5.0
      docker image tag obsidiandynamics/kafdrop kafdrop:latest
      docker image tag python:3.10-slim kafka-seeds:latest
	  docker image tag cassandra:4.1  cassandra_onprem:4.1
	  docker image tag apache/zeppelin:0.11.1 zeppelin:0.11.1

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


===========
## Run unit tests (fast)
From repo root: PowerShell (Windows):
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
pip install pytest pytest-cov
pytest -v
## Validate Rules YAML fix identetion
python -c "import yaml; yaml.safe_load(open('data-quality/tools/rules_v2.yaml'))"

pytest -v --cov=producer --cov=consumer --cov-report=xml
# coverage.xml will be produced (CI artifact)

# Run a single test:
pytest -q tests/test_validator_schema.py::test_schema_validation_pass

# 2) Linting / Static checks
pip install ruff
ruff check .

# 3) Schema validation (Avro/JSON)
CI step runs:

pip install fastavro jsonschema
python scripts/validate_schemas.py

4) Build / Java tests (Flink project)
If you need to build the Flink Java job (see Flink-DQ-Kafka):
# Ensure JDK and Maven installed
cd Flink-DQ-Kafka
mvn test 
# run Java unit tests for Flink module
mvn package         # build jar
# or use `make build-jar` if you have a Makefile in your environment (README references it)

5) Integration / End-to-end tests (Docker Compose)
Start the stack (root or data-quality folder, per README):

# from repo root (uses docker-compose.yml)
docker-compose up -d --build
# or if following Flink/Zeppelin setup: docker-compose -f data-quality/docker-compose.yaml up -d

- Seed Kafka (two options):
Using the included seeder container:
  docker-compose up -d kafka-seeder
  docker logs -f kafka-seeder

- Running seeder locally (use .venv or Python env):
Verify:

cd data-quality/kafka-seeder
python -m pip install -r requirements.txt
KAFKA_BROKER=kafka:9092 python seeder.py
# Or set KAFKA_BROKER=localhost:9092 if testing against local host

Check service logs (e.g., validator/pipeline): docker logs -f data_quality_service
Inspect Kafka topic messages:

docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic topic_onprem_sales --from-beginning --max-messages 10

- Inspect Cassandra via docker exec -it cassandra cqlsh and DESCRIBE KEYSPACES.
6) CI-local (simulate GitLab CI steps)
You can run the sequence locally:

## Unit tests + coverage:
pytest -v --cov=producer --cov=consumer --cov-report=xml
## Schema validation:
python scripts/validate_schemas.py
===============================================================================

# Validate Placeholders Are Replaced:
grep -n "{t1}\|{t2}\|{pk}" dq_generated.sql
  # should be empty
grep -n "FALSE" dq_generated.sql
  # should be empty



kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic onprem.customer.events --partitions 1 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic cloud.customer.hash --partitions 1 --replication-factor 1






================================================
## Next Steps:
  - Import the Zeppelin notebooks (Zeppelin UI → Import note → upload JSON).
  - Place rules.yaml alongside tools/rules_to_sql.py and run:
  - cd tools 
  - python rules_to_sql.py rules.yaml > dq_results.sql
    — or push directly into Zeppelin with --push-to-zeppelin.
  - Add ci/validate_flink_sql.sh into your CI pipeline before deploying Zeppelin 
  rules to integrate in rules.yaml:
  - delete_event   # FIRST
  - missing_pk
  - missing_cloud
  - hash_match
  - hash_mismatch  # LAST

  SQL notebooks.
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

# ------------------------------------------
# Use idempotent writes to support replays.
# ------------------------------------------
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
    cd tools;
    python rules_to_sql.py rules.yaml > dq_results.sql
— or push directly into Zeppelin with --push-to-zeppelin.

2. >> Add ci/validate_flink_sql.sh into your CI pipeline before deploying Zeppelin SQL notebooks.

3. Import dq_generated2.sql into Flink SQLwill  UI


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


# ------------------------------------------------------------------ #

## C. Periodic Reconciliation (Flink Batch Job)
  Run every 30 minutes / hourly:

Compare:
| Check                      | Method                                | Reason                      |
| -------------------------- | ------------------------------------- | --------------------------- |
| **Row count**              | CQL `COUNT(*)` or Spark via Flink SQL | Quick surface mismatch      |
| **Hash per partition key** | Murmur3 hashing                       | Detect drift                |
| **Full row consistency**   | Flink Batch job joining both tables   | Identify partial corruption |
| **Statistical comparison** | Mean, stddev, quantiles               | Detect data drift           |
| **Schema changes**         | Schema registry diff                  | Detect mismatches           |
----------------------------------------------------------------------------------------------------

## Vaidate  Drift Detection
CREATE VIEW unified_hashes AS
SELECT record_hash, 'ONPREM' AS source_dc FROM cassandra_onprem_hashes
UNION ALL
SELECT record_hash, 'CLOUD' AS source_dc FROM cassandra_cloud_hashes;
-- ##
SELECT
  record_hash,
  COUNT(DISTINCT source_dc) AS dc_count
FROM unified_hashes
GROUP BY record_hash
HAVING dc_count > 1;

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
## NEXT STEPS MONITORING
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


=================================================================
## Fix Error When Run Zeppelin 
flink.ssql
SELECT 1;

ERROR:
java.lang.IndexOutOfBoundsException: Index: 0, Size: 0
	at java.util.ArrayList.rangeCheck(ArrayList.java:659)
	at java.util.ArrayList.get(ArrayList.java:435)
	at org.apache.zeppelin.interpreter.launcher.FlinkInterpreterLauncher.chooseFlinkAppJar(FlinkInterpreterLauncher.java:148)
	at org.apache.zeppelin.interpreter.launcher.FlinkInterpreterLauncher.buildEnvFromProperties(FlinkInterpreterLauncher.java:84)
	at org.apache.zeppelin.interpreter.launcher.StandardInterpreterLauncher.launchDirectly(StandardInterpreterLauncher.java:77)
	at org.apache.zeppelin.interpreter.launcher.InterpreterLauncher.launch(InterpreterLauncher.java:110)
	at org.apache.zeppelin.interpreter.InterpreterSetting.createInterpreterProcess(InterpreterSetting.java:856)
	at org.apache.zeppelin.interpreter.ManagedInterpreterGroup.getOrCreateInterpreterProcess(ManagedInterpreterGroup.java:66)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.getOrCreateInterpreterProcess(RemoteInterpreter.java:104)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.internal_create(RemoteInterpreter.java:154)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.open(RemoteInterpreter.java:126)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.getFormType(RemoteInterpreter.java:271)
	at org.apache.zeppelin.notebook.Paragraph.jobRun(Paragraph.java:438)
	at org.apache.zeppelin.notebook.Paragraph.jobRun(Paragraph.java:69)
	at org.apache.zeppelin.scheduler.Job.run(Job.java:172)
	at org.apache.zeppelin.scheduler.AbstractScheduler.runJob(AbstractScheduler.java:132)
	at org.apache.zeppelin.scheduler.RemoteScheduler$JobRunner.run(RemoteScheduler.java:182)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
-----------------------
1.   Index: 0, Size: 0 
means:
   - Zeppelin tried to pick the first JAR from FLINK_HOME/lib or the configured flink.app.jar list,
   - But there are no JARs available, so the ArrayList is empty → crash.
Common causes:
   - You mounted Flink into Zeppelin, but $FLINK_HOME/lib is empty in the Zeppelin container.
   - You never set the flink.app.jar property in Zeppelin interpreter settings.
   - You are running Zeppelin and Flink in separate containers, and Zeppelin has no access to Flink binaries.

## Option 1: Use Flink SQL Gateway Instead
  - Recommended for modern setups.
    Zeppelin connects to remote Flink SQL Gateway, no JAR needed.
Steps:
  - Start a Flink cluster.
  - Start Flink SQL Gateway in Flink_jobmanager cluster:
      $FLINK_HOME/bin/sql-gateway.sh start-foreground
      Check: 
        curl http://localhost:8081
  - Configure Zeppelin %flink.ssql interpreter:
      1. Execution mode: Remote
      2. Remote host: sql-gateway host (or container name)
      3. Remote port: 8083    

## Option 2: Provide a Flink Distribution in Zeppelin
  If you want local execution:
  1. Make sure Zeppelin container has full Flink distribution:
    $FLINK_HOME/bin
    $FLINK_HOME/lib/flink-dist-*.jar
  2. In Zeppelin interpreter settings:
    flink.home → /opt/flink (inside Zeppelin)
    flink.app.jar → leave empty (Flink 1.17+ should auto-detect lib/)
  3. Restart interpreter.

## Option 3: Use %flink.pyflink or %flink.ipyflink
  1. These don’t require JAR selection.
  2. Good for Python-based DQ pipelines.
  3. You can run:
    %flink.pyflink
    t_env.execute_sql("SELECT 1").print()  

## Step 2: Configure SQL Gateway
 # Edit flink-conf.yaml (or pass as environment variables) with the minimum required settings:
 >> yaml:
  # JobManager host
  jobmanager.rpc.address: localhost          # or docker service name
  # Flink REST endpoint
  rest.address: localhost
  rest.port: 8081
  # SQL Gateway specific
  sql-gateway.endpoint.rest.address: 0.0.0.0
  sql-gateway.endpoint.rest.port: 8083

## Step 3: Start SQL Gateway in Foreground (Debug-Friendly)
  $FLINK_HOME/bin/sql-gateway.sh start-foreground

## Step 4: Verify SQL Gateway
  curl http://localhost:8083/info

## Step 5: Connect Zeppelin %flink.ssql to SQL Gateway
  # 1.  Open Zeppelin interpreter settings → %flink.ssql.
    2. Set:  
  Execution Mode	Remote
  Remote Host	<SQL Gateway host>
  Remote Port	8083
  Flink Home (optional)	leave empty (ignored in remote mode)   

# 3.  Restart Interpreter
    %flink.ssql
    SELECT 1;

## Step 6: Next Steps
  - Once SQL Gateway works, you can:
  - Run dq_generated.sql via Zeppelin %flink.ssql.
  - Read/write Kafka topics.
  - Compute dq_results_* and dq_metrics_*.
  - Optionally send DLQ records to separate Kafka topic.

## Fix SQL-gateway:
  docker exec -it sql-gateway /opt/flink/bin/sql-client.sh gateway


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

# ERROR ==============================
org.apache.fink.table.api.TableException: Table sink doen't support consuming update and delete changes which is produced by node JOin(joinType=[LeftOuterJoin], where=[(pk=pk0)], select=[pk, status, reason, pk0, payload, ts], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])
## This Flink error occurs because your LEFT OUTER JOIN is producing update/delete changes (retractions), but your table sink doesn't support them. This happens when the join inputs lack unique keys.
Root Causes:
1 .No primary keys defined on either the left or right input (NoUniqueKey)
2. Flink must emit retractions to handle JOIN updates correctly
3. Your sink (likely Kafka or a database) doesn't accept update/delete messages
----------------------
Solutions:
## ✅ Option 1: Define Primary Keys (Recommended)
Add PRIMARY KEY constraints to your source tables:

CREATE TABLE left_table (
  pk STRING,
  status STRING,
  reason STRING,
  PRIMARY KEY (pk) NOT ENFORCED
) WITH (...);

CREATE TABLE right_table (
  pk0 STRING,
  payload STRING,
  ts BIGINT,
  PRIMARY KEY (pk0) NOT ENFORCED
) WITH (...);

INSERT  
-- Now the JOIN will work with fewer retractions
SELECT l.pk, l.status, l.reason, r.pk0, r.payload, r.ts
FROM left_table l
LEFT JOIN right_table r ON l.pk = r.pk0;
---------------
## ✅ Option 2: Use Append-Only Mode
SELECT l.pk, l.status, l.reason, r.pk0, rl, r.ts
FROM left_table l
LEFT JOIN right_table r ON l.pk = r.pk0
WHERE r.pk0 IS NOT NULL;  -- Filter to append-only results
---------------
## ✅ Option 3: Use a Retraction-Aware Sink
Switch to a sink that supports updates (e.g., Upsert Kafka, Iceberg, or Hive):
>> CREATE TABLE result_sink (
  pk STRING PRIMARY KEY,
  status STRING,
  reason STRING,
  ...
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'output-topic',
  'properties.bootstrap.servers' = 'localhost:9092'
);
---------------
✅ Option 4: Check Flink Configuration
Ensure your SQL configuration supports retractions:
>>
SET 'sql.exec.sink.upsert-materialize' = 'AUTO';  -- or 'FORCE'

==================================================================
## ERROR: org.apache.fink.table.api.ValidationException: Filed names must contain at least one non-whitespace character
>>>
This Flink validation error occurs when a field name is empty or contains only whitespace. This typically happens in complex SQL queries with multiple joins, aliases, or string operations.

INSERT INTO cassandra_reconcile(pk, payload, ts, dq_status, dq_reason)
SELECT
  'uuid1' AS pk,
  CAST(JSON_OBJECT(
    'cust_id', 'test',
    'idmp_key', 'test',
    'load_ts', '2025-11-20T11:12:15'
  ) AS STRING) AS payload,
  CURRENT_TIMESTAMP AS ts,
  'INVALID' AS dq_status,
  'MISSING_CLOUD' AS dq_reason;

=================================================================
## ERROR: SELECT * from cass_reconcile LIMIT 1; give an ERROR: org.apache.fink.table.api.ValidationException: Filed names must contain at least one non-whitespace character
-------

## ✅ FIX: Use Regular Kafka Connector with Error Handling

```sql
CREATE TABLE cass_reconcile (
  pk STRING,
  payload STRING,
  ts TIMESTAMP(3),
  dq_status STRING,
  dq_reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'cassandra-reconcile',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- Now this will work:
SELECT * FROM cass_reconcile LIMIT 1;
```

**Why this works:**
- Regular `kafka` connector supports `json.fail-on-missing-field` and `json.ignore-parse-errors`
- `upsert-kafka` does NOT support these options (use only `key.format` and `value.format`)
- This configuration skips malformed JSON records instead of throwing validation errors

next:

==================================================================

I can also extend this Compose to include:
DLQ topic for invalid rows
Datadog agent + temporary DD_API_KEY

JSON Schema Validation for rules.yaml
