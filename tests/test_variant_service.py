import chord_lib
import pytest

from chord_variant_service.app import application
from jsonschema import validate


@pytest.fixture
def client():
    application.config["TESTING"] = True
    client = application.test_client()
    yield client


def test_service_info(client):
    rv = client.get("/service-info")
    data = rv.get_json()

    validate(data, chord_lib.schemas.ga4gh.SERVICE_INFO_SCHEMA)
