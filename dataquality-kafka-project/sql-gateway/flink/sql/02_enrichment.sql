CREATE VIEW enriched_events AS
SELECT
  t1.pk,
  t1.hash AS source_hash,
  t2.cloud_hash,
  t1.ts
FROM topic1_source t1
LEFT JOIN topic2_hash t2
ON t1.pk = t2.pk;
-- This view enriches events from topic1_source with cloud_hash from topic2_hash 
-- based on matching primary keys (pk).