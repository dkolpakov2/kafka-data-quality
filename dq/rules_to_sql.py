#!/usr/bin/env python3
"""
rules_to_sql.py
Reads rules.yaml and emits a Flink SQL CREATE VIEW statement for dq_results.
Usage: python rules_to_sql.py rules.yaml > dq_results.sql
##  open dq_results.sql and paste into Zeppelin paragraph 3, or use Zeppelin's %sh to run the generator and paste output
"""

import sys, yaml, json
from datetime import datetime

def generate_case_expr(rules):
    """Generate CASE expression for dq_status and dq_reason"""
    status_clauses = []
    reason_clauses = []
    # order rules: failures first (so severity fail takes precedence)
    for r in rules:
        rtype = r.get("type")
        rid = r.get("id")
        sev = r.get("severity", "fail")
        msg = r.get("message", rid or "validation_failed")  # Use rule ID or default message
        # Ensure msg is not empty (Flink requires non-whitespace field names)
        msg = msg.strip() if msg else f"rule_{rid}_failed"
        if rtype == "required":
            fields = r.get("fields", [])
            cond = " OR ".join([f"{fld} IS NULL" for fld in fields])
            status = "INVALID" if sev=="fail" else "QUARANTINE"
            status_clauses.append((f"WHEN ({cond}) THEN '{status}'", rid))
            reason_clauses.append((f"WHEN ({cond}) THEN '{msg}'", rid))
        elif rtype == "range" or rtype == "threshold":
            fld = r.get("field")
            minv = r.get("min")
            maxv = r.get("max")
            conds = []
            if minv is not None:
                conds.append(f"{fld} < {minv}")
            if maxv is not None:
                conds.append(f"{fld} > {maxv}")
            if conds:
                cond = " OR ".join(conds)
                status = "INVALID" if sev=="fail" else "QUARANTINE"
                status_clauses.append((f"WHEN ({cond}) THEN '{status}'", rid))
                reason_clauses.append((f"WHEN ({cond}) THEN '{msg}'", rid))
        elif rtype == "regex":
            fld = r.get("field")
            pattern = r.get("pattern").replace("'", "''")
            # Flink SQL has REGEXP (matches)
            cond = f"NOT ({fld} IS NULL) AND NOT REGEXP_LIKE({fld}, '{pattern}')"
            status = "INVALID" if sev=="fail" else "QUARANTINE"
            status_clauses.append((f"WHEN ({cond}) THEN '{status}'", rid))
            reason_clauses.append((f"WHEN ({cond}) THEN '{msg}'", rid))
        elif rtype == "timestamp_freshness" or rtype == "timestamp_fresh":
            fld = r.get("field")
            max_age = int(r.get("max_age_seconds", 86400))
            cond = f"{fld} < (NOW() - INTERVAL '{max_age}' SECOND)"
            status = "INVALID" if r.get("severity","warn")=="fail" else "QUARANTINE"
            status_clauses.append((f"WHEN ({cond}) THEN '{status}'", rid))
            reason_clauses.append((f"WHEN ({cond}) THEN '{msg}'", rid))
        else:
            # unsupported type: ignore or log
            pass

    # default
    status_default = "VALID"
    reason_default = "OK"

    # Build SQL CASE strings
    status_case = "CASE\n"
    for cond, rid in status_clauses:
        status_case += f"  {cond}\n"
    status_case += f"  ELSE '{status_default}'\nEND AS dq_status"

    reason_case = "CASE\n"
    for cond, rid in reason_clauses:
        reason_case += f"  {cond}\n"
    reason_case += f"  ELSE '{reason_default}'\nEND AS dq_reason"

    return status_case, reason_case

def main(rules_file):
    with open(rules_file) as f:
        doc = yaml.safe_load(f)
    rules = doc.get("rules", [])
    status_case, reason_case = generate_case_expr(rules)

    # full CREATE VIEW
    sql = f"""CREATE VIEW IF NOT EXISTS dq_results AS
SELECT
  id,
  ts,
  value,
  row_hash,
  event_json,
  {status_case},
  {reason_case}
FROM enriched_events;
"""
    print(sql)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python rules_to_sql.py rules.yaml > dq_results.sql")
    else:
        main(sys.argv[1])
