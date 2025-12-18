CREATE VIEW dq_metrics AS
SELECT
  TUMBLE_START(processing_time, INTERVAL '10' SECONDS) AS window_start,
  SUM(
    CASE WHEN t1.action = 'DELETE' THEN 1 ELSE 0 END
  ) AS dq_delete_count,

  SUM(
    CASE WHEN t1.action != 'DELETE' AND t2.pk IS NULL THEN 1 ELSE 0 END
  ) AS dq_missing_cloud

FROM dq_results
GROUP BY
  TUMBLE(processing_time, INTERVAL '10' SECONDS);
