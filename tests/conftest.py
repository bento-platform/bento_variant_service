import pytest

from chord_variant_service.app import application


@pytest.fixture
def client():
    application.config["TESTING"] = True
    client = application.test_client()
    yield client
