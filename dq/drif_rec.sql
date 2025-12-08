-- define views for each cassandra table (if not already defined)
CREATE VIEW IF NOT EXISTS cass_onprem AS
SELECT id, row_hash FROM cassandra_onprem;

CREATE VIEW IF NOT EXISTS cass_azure AS
SELECT id, row_hash FROM cassandra_azure;

-- reconciliation view: full outer join on PK (id)
CREATE VIEW IF NOT EXISTS cassandra_reconciliation AS
SELECT
  COALESCE(a.id, b.id) AS id,
  a.row_hash AS onprem_hash,
  b.row_hash AS azure_hash,
  CASE
    WHEN a.row_hash IS NULL AND b.row_hash IS NULL THEN 'MISSING_BOTH'
    WHEN a.row_hash IS NULL THEN 'MISSING_IN_ONPREM'
    WHEN b.row_hash IS NULL THEN 'MISSING_IN_AZURE'
    WHEN a.row_hash <> b.row_hash THEN 'DRIFT'
    ELSE 'OK'
  END AS reconciliation_status
FROM cass_onprem a
FULL OUTER JOIN cass_azure b
ON a.id = b.id;
-- end of dq/drif_rec.sql