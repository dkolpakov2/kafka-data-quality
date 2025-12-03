import pytest
from producer.validator import validate_datatypes

def test_datatype_validation_pass():
    schema = {"customer_id": str, "email": str, "age": int}
    record = {"customer_id": "C1", "email": "x@x.com", "age": 29}

    assert validate_datatypes(record, schema) is True

def test_datatype_validation_fail_wrong_type():
    schema = {"customer_id": str, "email": str, "age": int}
    record = {"customer_id": "C1", "email": "x@x.com", "age": "29"}  # age wrong type

    assert validate_datatypes(record, schema) is False
