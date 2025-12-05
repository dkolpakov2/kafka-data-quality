import json
import os
from fastavro.schema import load_schema, validate
from jsonschema import validate as json_validate, ValidationError

AVRO_DIR = "schemas/avro"
JSON_DIR = "schemas/json"

report = []

# ----- AVRO -----
for schema_file in os.listdir(AVRO_DIR):
    if schema_file.endswith(".avsc"):
        schema_path = os.path.join(AVRO_DIR, schema_file)
        schema = load_schema(schema_path)

        # Validate sample record file
        sample_path = schema_path.replace(".avsc", ".sample.json")
        if os.path.exists(sample_path):
            with open(sample_path) as f:
                sample = json.load(f)

            if validate(sample, schema):
                report.append(f"✓ AVRO schema OK: {schema_file}")
            else:
                report.append(f" AVRO schema FAIL: {schema_file}")

# ----- JSON Schema -----
for schema_file in os.listdir(JSON_DIR):
    if schema_file.endswith(".json"):
        schema_path = os.path.join(JSON_DIR, schema_file)

        with open(schema_path) as f:
            schema = json.load(f)

        sample_path = schema_path.replace(".json", ".sample.json")
        if os.path.exists(sample_path):
            with open(sample_path) as f:
                sample = json.load(f)

            try:
                json_validate(instance=sample, schema=schema)
                report.append(f"✓ JSON schema OK: {schema_file}")
            except ValidationError:
                report.append(f" JSON schema FAIL: {schema_file}")

with open("schema-validation-report.txt", "w") as f:
    f.write("\n".join(report))

print("\n".join(report))
