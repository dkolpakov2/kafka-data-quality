import pytest
from consumer.rules import validate_business_rules

def test_business_rules_valid():
    msg = {"customer_id": "C1", "email": "t@t.com", "age": 30}
    assert validate_business_rules(msg) is True

def test_business_rules_fail_age_negative():
    msg = {"customer_id": "C1", "email": "t@t.com", "age": -4}
    assert validate_business_rules(msg) is False

def test_business_rules_fail_email_invalid():
    msg = {"customer_id": "C1", "email": "not-an-email", "age": 22}
    assert validate_business_rules(msg) is False
