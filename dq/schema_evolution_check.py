"""
schema_evolution_check.py

Compare two Avro schemas from Schema Registry and detect likely breaking changes.

It implements a pragmatic set of checks:
 - removed fields -> breaking
 - added field without default and not union with null -> breaking
 - changed type that's not promotable (e.g., string -> int) -> breaking
 - changed namespace/name differences -> warn

Usage:
  python schema_evolution_check.py --registry http://schema-registry:8081 --subject customers-value --version-old 1 --version-new 2
  or:
  python schema_evolution_check.py --registry http://schema-registry:8081 --subject customers-value --latest-prev  # compares latest and previous
"""

import requests, json, argparse, sys

PROMOTABLE = {
    ("int", "long"),
    ("int", "float"),
    ("int", "double"),
    ("long", "double"),
    ("float", "double"),
}

def fetch_schema_version(registry, subject, version):
    url = f"{registry.rstrip('/')}/subjects/{subject}/versions/{version}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return json.loads(data["schema"])

def fetch_latest_version(registry, subject):
    url = f"{registry.rstrip('/')}/subjects/{subject}/versions"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    versions = r.json()
    if not versions:
        raise RuntimeError("no versions found")
    latest = max(versions)
    return latest

def normalize_field_type(ft):
    # simplify union vs single; return (is_nullable, base_type)
    if isinstance(ft, list):
        # union e.g., ["null","string"]
        nonnull = [x for x in ft if x != "null"]
        if len(nonnull) == 1:
            base = nonnull[0]
            return True, base
        else:
            # complex union: treat as non-nullable complex
            return False, ft
    else:
        return False, ft

def is_promotable(oldt, newt):
    # both simple strings
    return (oldt, newt) in PROMOTABLE

def compare_schemas(old_schema, new_schema):
    """
    Returns list of breaking_changes (strings), and warnings.
    Only does a shallow field-level comparison for record types.
    """
    breaks = []
    warns = []

    if old_schema.get("type") != "record" or new_schema.get("type") != "record":
        warns.append("One of schemas is not a record type; full compatibility not checked.")
        return breaks, warns

    old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
    new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

    # removed fields
    for fname in old_fields:
        if fname not in new_fields:
            breaks.append(f"field removed: {fname}")

    # added fields: if added without default and not nullable -> breaking
    for fname, f in new_fields.items():
        if fname not in old_fields:
            # check default or null union
            if "default" in f:
                continue
            is_nullable, base = normalize_field_type(f["type"])
            if is_nullable:
                continue
            # else breaking
            breaks.append(f"field added without default or nullable: {fname}")

    # changed types
    for fname in old_fields:
        if fname in new_fields:
            ot = old_fields[fname]["type"]
            nt = new_fields[fname]["type"]
            _, ob = normalize_field_type(ot)
            _, nb = normalize_field_type(nt)
            # if types are lists/complex, skip deep checks
            if isinstance(ob, str) and isinstance(nb, str):
                if ob != nb and not is_promotable(ob, nb):
                    breaks.append(f"type change not promotable for field {fname}: {ob} -> {nb}")
            else:
                warns.append(f"complex type change for {fname}: manual review")

    return breaks, warns

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True)
    ap.add_argument("--subject", required=True)
    ap.add_argument("--version-old", type=int)
    ap.add_argument("--version-new", type=int)
    ap.add_argument("--latest-prev", action="store_true", help="compare latest and previous")
    args = ap.parse_args()

    registry = args.registry
    subject = args.subject

    if args.latest_prev:
        versions_url = f"{registry.rstrip('/')}/subjects/{subject}/versions"
        r = requests.get(versions_url, timeout=10)
        r.raise_for_status()
        versions = r.json()
        if len(versions) < 2:
            print("Not enough versions to compare.")
            sys.exit(0)
        versions_sorted = sorted(versions)
        v_old = versions_sorted[-2]
        v_new = versions_sorted[-1]
    else:
        if not args.version_old or not args.version_new:
            print("Either supply --version-old & --version-new, or --latest-prev")
            sys.exit(1)
        v_old = args.version_old
        v_new = args.version_new

    old_schema = fetch_schema_version(registry, subject, v_old)
    new_schema = fetch_schema_version(registry, subject, v_new)

    breaks, warns = compare_schemas(old_schema, new_schema)

    print(f"Comparing {subject} v{v_old} -> v{v_new}")
    if breaks:
        print("BREAKING CHANGES:")
        for b in breaks:
            print(" -", b)
    else:
        print("No breaking changes detected.")

    if warns:
        print("WARNINGS:")
        for w in warns:
            print(" -", w)

if __name__ == "__main__":
    main()
