#!/usr/bin/env python3
import sys
import yaml

def load_rules(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def generate_case(rules):
    case_lines = []
    for rule in rules:
        condition = rule.get("when")
        status = rule["emit"]["dq_status"]

        if not condition:
            continue

        # IMPORTANT: do NOT evaluate condition
        # Treat it as SQL string
        case_lines.append(
            f"WHEN {condition} THEN '{status}'"
        )

    case_lines.append("ELSE 'VALID'")
    return "CASE\n  " + "\n  ".join(case_lines) + "\nEND AS dq_status"

def generate_sql(cfg):
    case_expr = generate_case(cfg["rules"])

    sql = f"""
CREATE VIEW {cfg['outputs']['table']} AS
SELECT
  t1.pk,
  t1.action,
  {case_expr},
  t1.hash_onprem,
  t2.hash_cloud,
  CURRENT_TIMESTAMP AS processing_time
FROM topic1_onprem t1
LEFT JOIN topic2_cloud t2
  ON t1.pk = t2.pk;
"""
    return sql.strip()

def generate_sql(cfg):
    status_case = generate_case(cfg["rules"])
    reason_case = generate_reason_case(cfg["rules"])

    return f"""
CREATE VIEW {cfg['outputs']['table']} AS
SELECT
  t1.pk,
  t1.action,
  {status_case},
  {reason_case},
  t1.hash_onprem,
  t2.hash_cloud,
  CURRENT_TIMESTAMP AS processing_time
FROM enriched_events t1
LEFT JOIN topic2_cloud t2
  ON t1.pk = t2.pk;
""".strip()

def generate_reason_case(rules):
    reason_lines = []
    for rule in rules:
        condition = rule.get("when")
        reason = rule["emit"].get("dq_reason")

        if not condition or not reason:
            continue

        # IMPORTANT: do NOT evaluate condition
        # Treat it as SQL string
        reason_lines.append(
            f"WHEN {condition} THEN '{reason}'"
        )

    reason_lines.append("ELSE 'OK'")
    return "CASE\n  " + "\n  ".join(reason_lines) + "\nEND AS dq_reason"

if __name__ == "__main__":
    rules_path = sys.argv[1]
    config = load_rules(rules_path)
    print(generate_sql(config))
