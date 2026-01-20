import pytest
import json
from utils.api_client import APIClient
from utils.db_client import DatabaseClient

@pytest.fixture(scope="session")
def api_client():
    """Session-scoped API client fixture"""
    return APIClient()

@pytest.fixture(scope="session")
def db_client():
    """Session-scoped database client fixture"""
    return DatabaseClient()

@pytest.fixture(scope="module")
def sample_product():
    """Sample product data for testing"""
    return {
        "id": 1,
        "title": "Test Product",
        "price": 29.99,
        "description": "Test product description",
        "category": "electronics",
        "image": "https://example.com/image.jpg",
        "rating": {
            "rate": 4.5,
            "count": 100
        }
    }

@pytest.fixture(scope="module")
def product_schema():
    """Product JSON schema fixture"""
    with open('schemas/product_schema.json', 'r') as f:
        return json.load(f)

@pytest.fixture(scope="module")
def user_schema():
    """User JSON schema fixture"""
    with open('schemas/user_schema.json', 'r') as f:
        return json.load(f)

@pytest.fixture(scope="function")
def test_data_cleanup():
    """Fixture for test data cleanup"""
    yield
    # Cleanup code here if needed
    pass