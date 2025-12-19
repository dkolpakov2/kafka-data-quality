## 1.  dq_generated.sql Is
dq_generated.sql contains only Flink SQL DDL/DML:
    CREATE TABLE (if you add them)
    CREATE VIEW dq_results_<table>
    CREATE VIEW dq_metrics_<table>

## 2. 
Usage Path (High Level)
Kafka Topics
   ↓
Flink SQL (dq_generated.sql)
   ↓
dq_results_*  (row-level DQ)
   ↓
dq_metrics_*  (windowed metrics)
   ↓
Datadog / Alerts

## 1.1 Start Flink (Local or Docker)
    $FLINK_HOME/bin/start-cluster.sh
    curl http://localhost:8081

## 1.2 Start SQL Gateway (Recommended)
    $FLINK_HOME/bin/sql-gateway.sh start
    curl http://localhost:8083/info

## 2. Create Source Tables (ONE-TIME)
    dq_generated.sql assumes Kafka tables already exist.
    I must create them first.    
# 2.1. 
CREATE TABLE onprem_customer_events (
  customer_id STRING,
  action STRING,
  hash_onprem STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'onprem.customer.events',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

# 2.2
CREATE TABLE cloud_customer_hash (
  customer_id STRING,
  hash_cloud STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cloud.customer.hash',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

## 3. Execute dq_generated.sql
>> Option B: Zeppelin
    Use:
        %flink.ssql
## 4 Verify DQ Views Are Active
    >> Run:
        SHOW VIEWS;        

## 5 Validate Row-Level DQ (dq_results_*)
    Send test messages to Kafka.
        run Kafka-feeder App
    5.2. check by:
        SELECT * FROM dq_results_customer;

## Validate Metrics (dq_metrics_*)
    SELECT * FROM dq_metrics_customer;

Expected columns:
    dq.cassandra.hash_mismatch
    dq.cassandra.missing_cloud
    dq_error_total
    dq_warn_total
    dq_info_total    

## 7. Use Metrics (Datadog-Ready)
    7.1 Sink Metrics to Kafka / HTTP / Prometheus
    Example Kafka sink:
    CREATE TABLE dq_metrics_sink (
        window_start TIMESTAMP(3),
        env STRING,
        dc STRING,
        table_name STRING,
        topic_name STRING,
        dq_error_total BIGINT
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'dq.metrics',
        'format' = 'json',
        'properties.bootstrap.servers' = 'localhost:9092'
        );

        INSERT INTO dq_metrics_sink
        SELECT * FROM dq_metrics_customer;

    7.2 Datadog Agent (Optional)
        dq_error_total{env:prod,dc:onprem,table:customer_onprem}

8. Production Deployment Pattern
--------------------------------------------
| Step                   | Frequency       |
| ---------------------- | --------------- |
| Create Kafka tables    | Once            |
| Run `dq_generated.sql` | Once per deploy |
| Send Kafka data        | Continuous      |
| Query metrics          | Continuous      |
| Alerts                 | Continuous      |
--------------------------------------------
9. Safe Re-deploy Rules
  python tools/rules_to_sql.py rules.yaml > dq_generated.sql
   
   Then in Flink:
    DROP VIEW dq_results_customer;
    DROP VIEW dq_metrics_customer;
    SOURCE dq_generated.sql;
(No data loss, state is recomputed)

## 777. What You’ve Achieved

✔ Declarative DQ
✔ Multi-table support
✔ Hash reconciliation
✔ Severity aggregation
✔ Per-topic metrics
✔ Datadog-ready observability
✔ SQL-only Flink job

!!! This is enterprise-grade.