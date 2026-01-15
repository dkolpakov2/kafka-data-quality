#!/usr/bin/env python3
"""
Advanced rules_to_sql.py
Generates DQ SQL CASE expressions from rules.yaml
Supports: required, range, threshold, regex, timestamp_freshness,
          cross_field, lookup_in, lookup_not_in, external_lookup_http,
          null_handling, custom_sql
Also supports pushing generated SQL into Zeppelin via REST API.
"""

import sys, yaml, json, argparse, requests, os
from datetime import datetime

# -------------------------------------
# External lookup fetcher
# -------------------------------------
def fetch_external_lookup(url):
    try:
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, list):
            return data
        raise ValueError("External lookup HTTP must return list")
    except Exception as ex:
        print(f"ERROR fetching external lookup URL {url}: {ex}")
        sys.exit(1)

# -------------------------------------
# SQL Escaping Helper
# -------------------------------------
def sql_list(items):
    """Convert python list to SQL IN list: ('a','b','c')"""
    escaped = [f"'{str(i).replace(\"'\",\"''\")}'" for i in items]
    return "(" + ",".join(escaped) + ")"

# -------------------------------------
# Core Rule → SQL translation
# -------------------------------------
def rule_to_condition(rule):
    rtype = rule.get("type")
    sev = rule.get("severity", "fail")
    rid = rule.get("id", "unknown")
    msg = rule.get("message", "")
    # Ensure msg is not empty (Flink requires non-whitespace field names)
    msg = msg.strip() if msg else f"rule_{rid}_failed"
    null_handling = rule.get("null_handling", "default")  # default / fail / skip

    cond = None

    # ----- required -----
    if rtype == "required":
        fields = rule["fields"]
        if null_handling == "skip":
            cond = "FALSE"
        else:
            conds = []
            for fld in fields:
                if null_handling in ("fail","default"):
                    conds.append(f"{fld} IS NULL")
            cond = " OR ".join(conds)

    # ----- range / threshold -----
    elif rtype in ("range","threshold"):
        fld = rule["field"]
        minv = rule.get("min")
        maxv = rule.get("max")
        conds = []
        if minv is not None:
            conds.append(f"{fld} < {minv}")
        if maxv is not None:
            conds.append(f"{fld} > {maxv}")
        if null_handling == "fail":
            conds.append(f"{fld} IS NULL")
        cond = " OR ".join(conds)

    # ----- regex -----
    elif rtype == "regex":
        fld = rule["field"]
        pattern = rule["pattern"].replace("'", "''")
        if null_handling == "skip":
            cond = f"{fld} IS NOT NULL AND NOT REGEXP_LIKE({fld}, '{pattern}')"
        elif null_handling == "fail":
            cond = f"{fld} IS NULL OR NOT REGEXP_LIKE({fld}, '{pattern}')"
        else:
            cond = f"NOT REGEXP_LIKE({fld}, '{pattern}')"

    # ----- timestamp freshness -----
    elif rtype in ("timestamp_freshness","timestamp_fresh"):
        fld = rule["field"]
        max_age = int(rule["max_age_seconds"])
        cond = f"{fld} < (NOW() - INTERVAL '{max_age}' SECOND)"

    # ----- cross-field -----
    elif rtype == "cross_field":
        left = rule["left"]
        op = rule["op"]
        right = rule["right"]
        cond = f"NOT ({left} {op} {right})"

    # ----- lookup_in -----
    elif rtype == "lookup_in":
        fld = rule["field"]
        vals = rule.get("values")
        url = rule.get("url")
        if url:
            vals = fetch_external_lookup(url)
        cond = f"{fld} NOT IN {sql_list(vals)}"

    # ----- lookup_not_in -----
    elif rtype == "lookup_not_in":
        fld = rule["field"]
        vals = rule.get("values")
        url = rule.get("url")
        if url:
            vals = fetch_external_lookup(url)
        cond = f"{fld} IN {sql_list(vals)}"

    # ----- custom SQL -----
    elif rtype == "custom_sql":
        cond = rule["sql"]

    else:
        print(f"Unknown rule type: {rtype}")
        cond = "FALSE"

    # Determine resulting DQ status
    if sev == "fail":
        status = "INVALID"
    elif sev == "warn":
        status = "QUARANTINE"
    else:
        status = "UNKNOWN"

    return cond, status, msg

# -------------------------------------
# Build CASE expression
# -------------------------------------
def generate_cases(rules):
    status_case = "CASE\n"
    reason_case = "CASE\n"

    for r in rules:
        cond, status, msg = rule_to_condition(r)
        status_case += f"  WHEN ({cond}) THEN '{status}'\n"
        reason_case += f"  WHEN ({cond}) THEN '{msg}'\n"

    status_case += "  ELSE 'VALID'\nEND AS dq_status"
    reason_case += "  ELSE 'OK'\nEND AS dq_reason"

    return status_case, reason_case

# -------------------------------------
# Build full CREATE VIEW
# -------------------------------------
def build_view(status_case, reason_case):
    sql = f"""
CREATE VIEW IF NOT EXISTS dq_results AS
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
    return sql.strip()

# -------------------------------------
# Push SQL into Zeppelin Paragraph
# -------------------------------------
def push_to_zeppelin(note_id, paragraph_id, sql, endpoint, token):
    url = f"{endpoint}/api/notebook/{note_id}/paragraph/{paragraph_id}"
    payload = {
        "text": f"%flink.ssql\n{sql}"
    }
    headers = {
        "Authorization": f"Bearer {token}"
    }
    r = requests.put(url, headers=headers, json=payload)
    if r.status_code not in (200,201):
        print("Error pushing to Zeppelin:", r.text)
        sys.exit(1)
    print("✓ Successfully pushed SQL to Zeppelin paragraph.")

# -------------------------------------
# Main
# -------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("rules_yaml", help="Path to rules.yaml")
    ap.add_argument("--push-to-zeppelin", action="store_true")
    ap.add_argument("--note-id", help="Zeppelin note ID")
    ap.add_argument("--paragraph-id", help="Zeppelin paragraph ID to replace")
    ap.add_argument("--zeppelin-endpoint", help="Zeppelin URL")
    args = ap.parse_args()

    with open(args.rules_yaml) as f:
        rules = yaml.safe_load(f)["rules"]

    status_case, reason_case = generate_cases(rules)
    sql = build_view(status_case, reason_case)

    print(sql)

    if args.push_to_zeppelin:
        token = os.getenv("ZEPPELIN_TOKEN")
        if not token:
            print("ERROR: Must set ZEPPELIN_TOKEN environment variable.")
            sys.exit(1)

        if not (args.note_id and args.paragraph_id and args.zeppelin_endpoint):
            print("ERROR: note-id, paragraph-id, zeppelin-endpoint required.")
            sys.exit(1)

        push_to_zeppelin(
            args.note_id,
            args.paragraph_id,
            sql,
            args.zeppelin_endpoint,
            token
        )
