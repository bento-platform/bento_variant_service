import pytest

from chord_variant_service.app import application
from jsonschema import validate


# TODO: Move to chord_lib
SERVICE_INFO_SCHEMA = {
    "$id": "https://example.com/address.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "type": {"type": "string"},
        "description": {"type": "string"},
        "organization": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "url": {"type": "string"}
            },
            "required": ["name", "url"]
        },
        "contactUrl": {"type": "string"},
        "documentationUrl": {"type": "string"},
        "createdAt": {"type": "string"},
        "updatedAt": {"type": "string"},
        "environment": {"type": "string"},
        "version": {"type": "string"}
    },
    "required": ["id", "name", "type", "organization", "version"]
}


@pytest.fixture
def client():
    application.config["TESTING"] = True
    client = application.test_client()
    yield client


def test_service_info(client):
    rv = client.get("/service-info")
    data = rv.get_json()

    validate(data, SERVICE_INFO_SCHEMA)
