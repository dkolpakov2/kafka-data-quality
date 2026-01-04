-- DLQ + Metrics Sink
CREATE TABLE dq_dlq (
  pk STRING,
  dq_reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'dq_dlq',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO dq_dlq
SELECT pk, dq_reason
FROM dq_results
WHERE dq_status = 'INVALID';
