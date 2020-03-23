import json
import pytest
import requests

from chord_variant_service import table_manager as tm
from chord_variant_service.app import application


@pytest.fixture
def app():
    application.config["TESTING"] = True
    application.config["TABLE_MANAGER"] = "memory"

    with application.app_context():
        tm._table_manager = None
        tm.get_table_manager()
    yield application


@pytest.fixture
def vcf_app(tmpdir):
    data_path = tmpdir / "vcf_data"
    data_path.mkdir()

    application.config["TESTING"] = True
    application.config["DATA_PATH"] = str(data_path)
    application.config["TABLE_MANAGER"] = "vcf"

    with application.app_context():
        tm._table_manager = None
        tm.get_table_manager()

    yield application


@pytest.fixture()
def table_manager(app):
    with app.app_context():
        yield tm.get_table_manager()


@pytest.fixture()
def vcf_table_manager(vcf_app):
    with vcf_app.app_context():
        yield tm.get_table_manager()


@pytest.fixture
def client(app):
    yield app.test_client()


@pytest.fixture
def vcf_client(vcf_app):
    yield vcf_app.test_client()


@pytest.fixture(scope="module")
def json_schema():
    r = requests.get("http://json-schema.org/draft-07/schema#")
    schema = json.loads(r.content)
    yield schema
