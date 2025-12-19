-- SQL view to compute data quality metrics for Cassandra data comparison
CREATE VIEW dq_metrics AS
SELECT
  TUMBLE_START(processing_time, INTERVAL '1' MINUTE) AS window_start,

  'prod'   AS env,
  'onprem' AS dc,
  'customer_onprem' AS table_name,
  'onprem.events'   AS topic_name,

  SUM(CASE WHEN t1.action = 'DELETE' THEN 1 ELSE 0 END)
    AS `dq.cassandra.delete_count`,

  SUM(CASE WHEN t1.action != 'DELETE' AND t2.pk IS NULL THEN 1 ELSE 0 END)
    AS `dq.cassandra.missing_cloud`,

  SUM(CASE WHEN t1.action != 'DELETE'
            AND t1.hash_onprem != t2.hash_cloud THEN 1 ELSE 0 END)
    AS `dq.cassandra.hash_mismatch`

FROM dq_results
GROUP BY
  TUMBLE(processing_time, INTERVAL '1' MINUTE),
  env,
  dc,
  table_name,
  topic_name;


-- CREATE VIEW dq_metrics AS
-- SELECT
--   TUMBLE_START(processing_time, INTERVAL '10' SECONDS) AS window_start,
--   SUM(
--     CASE WHEN t1.action = 'DELETE' THEN 1 ELSE 0 END
--   ) AS dq_delete_count,

--   SUM(
--     CASE WHEN t1.action != 'DELETE' AND t2.pk IS NULL THEN 1 ELSE 0 END
--   ) AS dq_missing_cloud

-- FROM dq_results
-- GROUP BY
--   TUMBLE(processing_time, INTERVAL '10' SECONDS);
