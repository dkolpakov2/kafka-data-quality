"""
schema_validator.py

Lightweight runtime validator for Avro records against schema registry.
- Supports Confluent Schema Registry REST API (GET /subjects/<subject>/versions/latest)
- Uses fastavro.validate to confirm JSON -> Avro compatibility
- Returns (True, None) or (False, "reason")

Usage:
    from schema_validator import SchemaValidator
    sv = SchemaValidator(schema_registry_url="http://schema-registry:8081")
    ok, reason = sv.validate_dict(record_dict, subject="my-topic-value")
"""

import requests, json, time
from fastavro import parse_schema, validate

class SchemaFetchError(Exception):
    pass

class SchemaValidator:
    def __init__(self, schema_registry_url=None, cache_ttl_seconds=300):
        self.registry = schema_registry_url or "http://schema-registry:8081"
        self.cache = {}  # subject -> {schema_obj, ts}
        self.ttl = cache_ttl_seconds

    def _fetch_latest_schema(self, subject):
        # Confluent: GET /subjects/{subject}/versions/latest
        url = f"{self.registry.rstrip('/')}/subjects/{subject}/versions/latest"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            raise SchemaFetchError(f"Schema registry returned {resp.status_code}: {resp.text}")
        payload = resp.json()
        schema_str = payload.get("schema")
        if not schema_str:
            raise SchemaFetchError("Schema payload missing 'schema' field")
        # schema_str is a JSON string representation of Avro schema
        schema_json = json.loads(schema_str)
        parsed = parse_schema(schema_json)
        return parsed

    def get_schema(self, subject):
        now = time.time()
        cached = self.cache.get(subject)
        if cached and (now - cached["ts"]) < self.ttl:
            return cached["schema"]
        parsed = self._fetch_latest_schema(subject)
        self.cache[subject] = {"schema": parsed, "ts": now}
        return parsed

    def validate_dict(self, record: dict, subject: str):
        """
        Validate Python dict record against latest schema for subject.
        Returns (True, None) if OK; (False, reason) otherwise.
        """
        try:
            schema = self.get_schema(subject)
        except Exception as e:
            return False, f"schema fetch error: {e}"

        # fastavro.validate expects Avro-parsable record and parsed schema
        try:
            ok = validate(record, schema)
            if ok:
                return True, None
            else:
                return False, "fastavro.validate returned False"
        except Exception as e:
            return False, f"validation error: {e}"


if __name__ == "__main__":
    # quick demo
    sv = SchemaValidator(schema_registry_url="http://schema-registry:8081")
    sample = {"customer_id": "C123", "email": "x@example.com", "age": 30}
    ok, reason = sv.validate_dict(sample, subject="customers-value")
    print("OK:", ok, "reason:", reason)
