CREATE VIEW dq_results AS
SELECT
  t1.pk,
  t1.action,

  CASE
    WHEN t1.action = 'DELETE' THEN 'DELETE_EVENT'
    WHEN t2.pk IS NULL THEN 'MISSING_CLOUD'
    ELSE 
      'MATCH'
  END AS dq_status,

  CASE
    WHEN t1.action = 'DELETE' THEN 'Delete event - hash comparison skipped'
    WHEN t2.pk IS NULL THEN 'Missing row in cloud Cassandra'
    ELSE 
      'OK'
  END AS dq_reason,
  CURRENT_TIMESTAMP AS processing_time
  FROM enriched_events t1
  LEFT JOIN topic2_cloud t2
  ON t1.pk = t2.pk;



---------------------------------- Data Quality Results View ----
--------------------------------Old VERSION  --------------------

CREATE VIEW IF NOT EXISTS dq_results AS
SELECT
  id,
  ts AS event_ts,
  value,
  row_hash,
  raw_json AS event_json,
  CASE
    WHEN t1.action = 'DELETE' THEN 'DELETE_EVENT'
    WHEN t1.pk IS NULL THEN 'MISSING_PK'
    WHEN t1.action != 'DELETE' AND t2.pk IS NULL THEN 'MISSING_CLOUD'
--    WHEN t1.action != 'DELETE' AND t1.hash_onprem = t2.hash_cloud THEN 'MATCH'
    WHEN t1.action != 'DELETE' AND t1.hash_onprem != t2.hash_cloud THEN 'MISMATCH'
    WHEN t1.hash_onprem IS NOT NULL
       AND NOT REGEXP_LIKE(t1.hash_onprem, '^[a-f0-9]{64}$')
       THEN 'INVALID_HASH'
  ELSE 
    'VALID'
  END AS dq_status,
  CASE
    WHEN t1.action = 'DELETE' THEN 'Delete event - hash comparison skipped'
    WHEN t1.pk IS NULL THEN 'Primary key missing'
    WHEN t1.action != 'DELETE' AND t2.pk IS NULL THEN 'Missing row in cloud Cassandra'
    WHEN t1.action != 'DELETE' AND t1.hash_onprem != t2.hash_cloud THEN 'Hash mismatch between on-prem and cloud'
  ELSE 
    'OK'
  END AS dq_reason

  ------ JOIN between on-prem and cloud datasets
-- `FROM onprem_data t1 
-- LEFT JOIN cloud_data t2
--   ON t1.pk = t2.pk;



