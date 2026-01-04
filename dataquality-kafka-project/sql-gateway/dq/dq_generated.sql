CREATE VIEW dq_results AS
SELECT
  *,
  CASE
    WHEN source_hash IS NULL THEN 'INVALID'
    WHEN cloud_hash IS NULL THEN 'INVALID'
    WHEN source_hash <> cloud_hash THEN 'INVALID'
    ELSE 'VALID'
  END AS dq_status,
  CASE
    WHEN source_hash IS NULL THEN 'SOURCE_HASH_MISSING'
    WHEN cloud_hash IS NULL THEN 'CLOUD_HASH_MISSING'
    WHEN source_hash <> cloud_hash THEN 'HASH_MISMATCH'
    ELSE 'OK'
  END AS dq_reason
FROM enriched_events;
-- This view performs data quality checks by comparing source_hash and cloud_hash,
-- assigning a dq_status and dq_reason based on the comparison results.