## python tools/rules_to_sql.py rules.yaml > dq_generated.sql
#!/usr/bin/env python3
import sys
import yaml
from typing import Dict, Any, List

# -----------------------------
# Utilities
# -----------------------------
print("USING FINAL MULTI-TABLE DQ GENERATOR v2", file=sys.stderr)
def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def render_condition(template: str, table: Dict[str, Any]) -> str:
    """
    Replace rule placeholders with table-specific values
    """
    replacements = {
        "{t1}": table["sources"]["onprem"]["alias"],
        "{t2}": table["sources"]["cloud"]["alias"],
        "{pk}": table["pk"],
        "{hash_onprem}": table["hash"]["onprem_field"],
        "{hash_cloud}": table["hash"]["cloud_field"],
    }

    rendered = template
    for k, v in replacements.items():
        rendered = rendered.replace(k, v)

    return rendered

# -----------------------------
# CASE builders
# -----------------------------

def build_status_case(rules: List[Dict[str, Any]], table: Dict[str, Any]) -> str:
    lines = []

    for rule in rules:
        condition = render_condition(rule["when"], table)
        status = rule["emit"]["dq_status"]
        lines.append(f"WHEN {condition} THEN '{status}'")

    lines.append("ELSE 'VALID'")
    return "CASE\n  " + "\n  ".join(lines) + "\nEND"

def build_reason_case(rules: List[Dict[str, Any]], table: Dict[str, Any]) -> str:
    lines = []

    for rule in rules:
        condition = render_condition(rule["when"], table)
        reason = rule.get("description", rule["name"])
        lines.append(f"WHEN {condition} THEN '{reason}'")

    lines.append("ELSE 'OK'")
    return "CASE\n  " + "\n  ".join(lines) + "\nEND"

# -----------------------------
# Metrics builders
# -----------------------------

def build_metric_columns(cfg: Dict[str, Any], table: Dict[str, Any]) -> str:
    namespace = cfg["metrics"]["namespace"]
    rules = cfg["rules"]

    metric_sql = []

    for rule in rules:
        metric = rule["emit"].get("metric")
        if not metric:
            continue

        condition = render_condition(rule["when"], table)

        metric_sql.append(f"""
SUM(
  CASE WHEN {condition} THEN 1 ELSE 0 END
) AS `{namespace}.{metric}`
""".strip())

    return ",\n".join(metric_sql)

# -----------------------------
# SQL generators
# -----------------------------

def generate_results_view(cfg: Dict[str, Any], table: Dict[str, Any]) -> str:
    table_name = table["name"]
    pk = table["pk"]

    t1 = table["sources"]["onprem"]
    t2 = table["sources"]["cloud"]

    status_case = build_status_case(cfg["rules"], table)
    reason_case = build_reason_case(cfg["rules"], table)

    return f"""
-- ============================================
-- DQ RESULTS VIEW: {table_name}
-- ============================================

CREATE VIEW dq_results_{table_name} AS
SELECT
  {t1["alias"]}.{pk} AS pk,
  {t1["alias"]}.action,

  {status_case} AS dq_status,
  {reason_case} AS dq_reason,

  CURRENT_TIMESTAMP AS processing_time
FROM {t1["topic"]} {t1["alias"]}
LEFT JOIN {t2["topic"]} {t2["alias"]}
  ON {t1["alias"]}.{pk} = {t2["alias"]}.{pk};
""".strip()

def generate_metrics_view(cfg: Dict[str, Any], table: Dict[str, Any]) -> str:
    table_name = table["name"]
    window = cfg["metrics"]["window"]
    env = cfg["metadata"]["env"]

    src = table["sources"]["onprem"]

    metric_columns = build_metric_columns(cfg, table)

    return f"""
-- ============================================
-- DQ METRICS VIEW: {table_name}
-- ============================================

CREATE VIEW dq_metrics_{table_name} AS
SELECT
  TUMBLE_START(processing_time, INTERVAL '{window}') AS window_start,

  '{env}' AS env,
  '{src["dc"]}' AS dc,
  '{src["table"]}' AS table_name,
  '{src["topic"]}' AS topic_name,

  {metric_columns}

FROM dq_results_{table_name}
GROUP BY
  TUMBLE(processing_time, INTERVAL '{window}'),
  env,
  dc,
  table_name,
  topic_name;
""".strip()

# -----------------------------
# Main
# -----------------------------

def main():
    if len(sys.argv) != 2:
        print("Usage: rules_to_sql.py rules.yaml", file=sys.stderr)
        sys.exit(1)

    cfg = load_yaml(sys.argv[1])

    sql_blocks = []

    for table in cfg["tables"]:
        sql_blocks.append(generate_results_view(cfg, table))
        sql_blocks.append(generate_metrics_view(cfg, table))

    print("\n\n".join(sql_blocks))


## Add severity aggregators:
def build_severity_metrics(cfg, table):
    rules = cfg["rules"]

    severity_conditions = {
        "ERROR": [],
        "WARN": [],
        "INFO": []
    }

    for rule in rules:
        severity = rule.get("emit", {}).get("severity")
        condition = rule.get("when")

        if severity and condition:
            rendered = render_condition(condition, table)
            severity_conditions[severity].append(f"({rendered})")

    sql_lines = []

    for sev, conditions in severity_conditions.items():
        if not conditions:
            continue

        combined = " OR ".join(conditions)

        metric_name = f"dq_{sev.lower()}_total"

        sql_lines.append(f"""
SUM(
  CASE WHEN {combined} THEN 1 ELSE 0 END
) AS {metric_name}
""".strip())

    return ",\n".join(sql_lines)
##



if __name__ == "__main__":
    main()
