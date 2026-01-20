import pytest
import logging
import yaml
from utils.api_client import APIClient
from utils.sqlite_client import SQLiteClient

class BaseTest:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_client = APIClient("https://fakestoreapi.com")
        self.db_client = SQLiteClient()
        
        # Load config
        with open('config/config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
    
    def validate_response_status(self, response, expected_status=200):
        assert response.status_code == expected_status, f"Expected {expected_status}, got {response.status_code}"
    
    def validate_response_schema(self, response_data, schema):
        from jsonschema import validate
        validate(instance=response_data, schema=schema)
    
    def validate_required_fields(self, data, required_fields):
        for field in required_fields:
            assert field in data, f"Required field '{field}' missing"