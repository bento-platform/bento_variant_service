import json
import pytest
import requests

from bento_variant_service import table_manager as tm
from bento_variant_service.app import create_app


@pytest.fixture
def app():
    tm._table_manager = None
    yield create_app({
        "TESTING": True,
        "TABLE_MANAGER": tm.MANAGER_TYPE_MEMORY
    })


@pytest.fixture
def uninitialized_app():
    tm._table_manager = None
    yield create_app({
        "TESTING": True,
        "TABLE_MANAGER": tm.MANAGER_TYPE_MEMORY,
        "INITIALIZE_IMMEDIATELY": False
    })


@pytest.fixture
def vcf_app(tmpdir):
    data_path = tmpdir / "vcf_data"
    data_path.mkdir()

    tm._table_manager = None
    yield create_app({
        "TESTING": True,
        "DATA_PATH": str(data_path),
        "TABLE_MANAGER": tm.MANAGER_TYPE_VCF,
    })


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
def uninitialized_client(uninitialized_app):
    yield uninitialized_app.test_client()


@pytest.fixture
def vcf_client(vcf_app):
    yield vcf_app.test_client()


@pytest.fixture(scope="module")
def json_schema():
    r = requests.get("http://json-schema.org/draft-07/schema#")
    schema = json.loads(r.content)
    yield schema
