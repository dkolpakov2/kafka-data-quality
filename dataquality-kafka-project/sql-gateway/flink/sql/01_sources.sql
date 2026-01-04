CREATE TABLE topic1_source (
  pk STRING,
  hash STRING,
  payload STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-topic1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE topic2_hash (
  pk STRING,
  cloud_hash STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'dq-topic2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
