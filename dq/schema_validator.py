"""
Schema registry validator for Avro (Confluent) or Azure Schema Registry.
Simple interface: validate(record: dict, subject: str) -> (bool, str)
"""
import os
import json
import requests

MODE = os.getenv("SCHEMA_MODE", "confluent")  # confluent | azure
CONFLUENT_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
AZURE_SCOPE = os.getenv("AZURE_SCHEMA_GROUP", "kafka-schemas")

# For full production use, use official clients: confluent-kafka + confluent_kafka.schema_registry
# Here we use a lightweight check: compare required fields & types loaded from schema registry

def _get_confluent_schema(subject):
    # subject: <topic>-value usually
    url = f"{CONFLUENT_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=5)
    if resp.status_code != 200:
        raise RuntimeError(f"schema registry error {resp.status_code}: {resp.text}")
    data = resp.json()
    # schema is JSON string
    schema_json = json.loads(data["schema"])
    return schema_json

def validate(record: dict, subject: str) -> (bool, str):
    try:
        if MODE == "confluent":
            schema = _get_confluent_schema(subject)
            fields = {f["name"]: f["type"] for f in schema.get("fields", [])}
            # basic type map for avro->python
            for fname, ftype in fields.items():
                if fname not in record:
                    return False, f"missing field {fname} per schema"
                val = record[fname]
                if ftype in ("int","long") and not isinstance(val, int):
                    return False, f"field {fname} should be int"
                if ftype in ("float","double") and not isinstance(val, (int,float)):
                    return False, f"field {fname} should be float"
                if ftype == "string" and not isinstance(val, str):
                    return False, f"field {fname} should be string"
            return True, None
        elif MODE == "azure":
            # for Azure Schema Registry prefer azure.schemaregistry client.
            # Here we assume a local cache file named azure_schema_<subject>.json exists for speed
            cache_file = f"/app/azure_schema_{subject}.json"
            if not os.path.exists(cache_file):
                return False, f"azure schema for {subject} not found locally"
            schema = json.load(open(cache_file))
            fields = {f["name"]: f["type"] for f in schema.get("fields", [])}
            for fname in fields:
                if fname not in record:
                    return False, f"missing {fname}"
            return True, None
        else:
            return False, "unknown schema mode"
    except Exception as e:
        return False, f"schema validation error: {str(e)}"
