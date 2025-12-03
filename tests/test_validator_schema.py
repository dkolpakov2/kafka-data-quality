import pytest
from producer.validator import validate_schema

schema = {
    "type": "record",
    "name": "Customer",
    "fields": [
        {"name": "customer_id", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}

def test_schema_validation_pass():
    record = {"customer_id": "C1", "email": "test@domain.com", "age": 40}
    assert validate_schema(record, schema) is True

def test_schema_validation_fail_missing_field():
    record = {"customer_id": "C1", "age": 40}
    assert validate_schema(record, schema) is False

def test_schema_validation_fail_wrong_type():
    record = {"customer_id": "C1", "email": "test@domain.com", "age": "40"}
    assert validate_schema(record, schema) is False
