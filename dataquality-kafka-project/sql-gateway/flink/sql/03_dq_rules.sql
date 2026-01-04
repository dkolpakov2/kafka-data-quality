-- Stable wrapper layer (never empty)

CREATE VIEW dq_results_final AS
SELECT *
FROM dq_results;

-- Option 2 produced by python
-- Data Quality Rules Implementation

CREATE VIEW dq_results AS
SELECT
  pk,
  source_hash,
  cloud_hash,

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
  END AS dq_reason,

  -- Severity aggregation
  CASE
    WHEN source_hash IS NULL THEN 2
    WHEN cloud_hash IS NULL THEN 2
    WHEN source_hash <> cloud_hash THEN 1
  ELSE 0
  END AS dq_error_total

FROM enriched_events;




-- Example Rule 1: Check for null values in critical columns
-- CREATE VIEW dq_null_check AS    
-- SELECT *,
--        CASE 
--            WHEN critical_column IS NULL THEN 'FAIL'
--            ELSE 'PASS'
--        END AS null_check_result
-- FROM source_table;      
-- -- Example Rule 2: Check for value ranges
-- CREATE VIEW dq_value_range_check AS

-- SELECT *,
--        CASE 
--            WHEN numeric_column < 0 OR numeric_column > 100 THEN 'FAIL'
--            ELSE 'PASS'
--        END AS value_range_check_result
-- FROM source_table;  
-- -- Example Rule 3: Check for duplicate records
-- CREATE VIEW dq_duplicate_check AS
-- SELECT *,
--        CASE 
--            WHEN COUNT(*) OVER (PARTITION BY unique_key_column) > 1 THEN 'FAIL'
--            ELSE 'PASS'
--        END AS duplicate_check_result
-- FROM source_table;  