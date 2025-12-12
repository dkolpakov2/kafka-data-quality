#!/usr/bin/env python3
import sys, yaml, argparse, requests, os
def sql_list(items):
    escaped = [f"'{str(i).replace("'","''")}'" for i in items]
    return "(" + ",".join(escaped) + ")"
def fetch_external_lookup(url):
    r = requests.get(url, timeout=5); r.raise_for_status(); data = r.json()
    if not isinstance(data, list): raise SystemExit('external lookup must return list')
    return data
def rule_to_condition(rule):
    rtype = rule.get('type'); sev = rule.get('severity','fail'); msg = rule.get('message','')
    null_handling = rule.get('null_handling','default')
    cond = 'FALSE'
    if rtype=='required':
        fields = rule['fields']; conds=[]
        for f in fields:
            if null_handling!='skip': conds.append(f"{f} IS NULL")
        cond = ' OR '.join(conds) if conds else 'FALSE'
    elif rtype in ('range','threshold'):
        fld=rule['field']; minv=rule.get('min'); maxv=rule.get('max'); conds=[]
        if minv is not None: conds.append(f"{fld} < {minv}")
        if maxv is not None: conds.append(f"{fld} > {maxv}")
        if null_handling=='fail': conds.append(f"{fld} IS NULL")
        cond = ' OR '.join(conds) if conds else 'FALSE'
    elif rtype=='regex':
        fld=rule['field']; pattern=rule['pattern'].replace("'", "''")
        if null_handling=='skip':
            cond = f"{fld} IS NOT NULL AND NOT REGEXP_LIKE({fld}, '{pattern}')"
        elif null_handling=='fail':
            cond = f"{fld} IS NULL OR NOT REGEXP_LIKE({fld}, '{pattern}')"
        else:
            cond = f"NOT REGEXP_LIKE({fld}, '{pattern}')"
    elif rtype in ('timestamp_freshness','timestamp_fresh'):
        fld=rule['field']; max_age=int(rule.get('max_age_seconds',86400))
        cond = f"{fld} < (NOW() - INTERVAL '{max_age}' SECOND)"
    elif rtype=='cross_field':
        left=rule['left']; op=rule['op']; right=rule['right']; cond = f"NOT ({left} {op} {right})"
    elif rtype in ('lookup_in','lookup_not_in'):
        fld=rule['field']; vals=rule.get('values'); url=rule.get('url')
        if url: vals = fetch_external_lookup(url)
        if vals is None: cond='FALSE'
        else:
            if rtype=='lookup_in':
                cond = f"{fld} NOT IN {sql_list(vals)}"
            else:
                cond = f"{fld} IN {sql_list(vals)}"
    elif rtype=='custom_sql':
        cond = rule.get('sql','FALSE')
    elif rtype=='schema_registry':
        cond = 'FALSE'
    else:
        cond = 'FALSE'
    status = 'INVALID' if sev=='fail' else ('QUARANTINE' if sev=='warn' else 'UNKNOWN')
    return cond, status, msg
def generate_cases(rules):
    status_case = 'CASE\n'; reason_case='CASE\n'
    for r in rules:
        cond,status,msg = rule_to_condition(r)
        status_case += f"  WHEN ({cond}) THEN '{status}'\n"
        reason_case += f"  WHEN ({cond}) THEN '{msg}'\n"
    status_case += "  ELSE 'VALID'\nEND AS dq_status"
    reason_case += "  ELSE 'OK'\nEND AS dq_reason"
    return status_case, reason_case
def build_view(status_case, reason_case):
    sql = f"""CREATE VIEW IF NOT EXISTS dq_results AS
SELECT
  id,
  ts AS event_ts,
  value,
  row_hash,
  raw_json AS event_json,
  {status_case},
  {reason_case}
FROM enriched_events;
"""
    return sql
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('rules_yaml')
    ap.add_argument('--push-to-zeppelin', action='store_true')
    ap.add_argument('--note-id')
    ap.add_argument('--paragraph-id')
    ap.add_argument('--zeppelin-endpoint')
    args=ap.parse_args()
    with open(args.rules_yaml) as f:
        rules = yaml.safe_load(f).get('rules', [])
    status_case, reason_case = generate_cases(rules)
    sql = build_view(status_case, reason_case)
    print(sql)
    if args.push_to_zeppelin:
        token = os.getenv('ZEPPELIN_TOKEN')
        if not token: raise SystemExit('ZEPPELIN_TOKEN required')
        if not (args.note_id and args.paragraph_id and args.zeppelin_endpoint): raise SystemExit('note-id, paragraph-id, endpoint required')
        url = f"{args.zeppelin_endpoint.rstrip('/')}/api/notebook/{args.note_id}/paragraph/{args.paragraph_id}"
        payload = {'text': f"%flink.ssql\n{sql}"}
        headers = {'Authorization': f'Bearer {token}'}
        r = requests.put(url, json=payload, headers=headers, timeout=10)
        if r.status_code not in (200,201): raise SystemExit(f'Zeppelin push failed: {r.status_code} {r.text}')
        print('Pushed to Zeppelin')
if __name__=='__main__':
    main()
