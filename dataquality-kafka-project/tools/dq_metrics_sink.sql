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
