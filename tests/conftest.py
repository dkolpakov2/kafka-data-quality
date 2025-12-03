import pytest

@pytest.fixture
def sample_valid_record():
    return {"customer_id": "C1", "email": "user@test.com", "age": 33}

@pytest.fixture
def sample_invalid_record():
    return {"customer_id": "C1", "age": "oops"}  # missing email, wrong type
