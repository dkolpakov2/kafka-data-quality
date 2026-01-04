CREATE VIEW dq_metrics AS
SELECT
  dq_status,
  COUNT(*) AS total
FROM dq_results
GROUP BY dq_status;
-- To query the metrics, we can use:
-- SELECT * FROM dq_metrics;