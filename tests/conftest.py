import json
import pytest
import requests

from chord_variant_service.app import application
from chord_variant_service.datasets import MemoryTableManager


@pytest.fixture
def client():
    application.config["TESTING"] = True
    application.config["TABLE_MANAGER"] = MemoryTableManager()
    client = application.test_client()
    yield client


@pytest.fixture(scope="module")
def json_schema():
    r = requests.get("http://json-schema.org/draft-07/schema#")
    schema = json.loads(r.content)
    print(schema)
    yield schema
