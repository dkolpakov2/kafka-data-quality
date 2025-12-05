import re
import logging
from datadog import initialize, statsd

initialize(api_key=os.getenv("DD_API_KEY"))

logger = logging.getLogger("kafka-dq")
logger.setLevel(logging.INFO)


def validate_required_fields(record, required_fields):
    return all(field in record for field in required_fields)

def validate_datatypes(record, schema_types):
    for field, expected_type in schema_types.items():
        if field not in record or not isinstance(record[field], expected_type):
            return False
    return True

def validate_business_rules(record):
    if record.get("age", 0) < 0:
        return False
    if not re.match(r"[^@]+@[^@]+\.[^@]+", record.get("email", "")):
        return False
    return True

def validate_schema(record, avro_schema):
    schema_fields = {f["name"] for f in avro_schema["fields"]}
    if not schema_fields.issubset(record.keys()):
        return False

    for f in avro_schema["fields"]:
        field = f["name"]
        expected = f["type"]
        if expected == "int" and not isinstance(record[field], int):
            return False
        if expected == "string" and not isinstance(record[field], str):
            return False
    return True
