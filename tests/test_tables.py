import json
from chord_variant_service.variants import VARIANT_TABLE_METADATA_SCHEMA, VARIANT_SCHEMA
from jsonschema import validate

from .shared_data import VARIANT_1, VARIANT_2, VARIANT_3

DATASET_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "metadata": VARIANT_TABLE_METADATA_SCHEMA,
        "schema": {"$ref": "http://json-schema.org/draft-07/schema#"}
    },
    "required": ["id", "schema"]
}


def test_tables(client):
    rv = client.get("/tables")
    assert rv.status_code == 400

    rv = client.get("/tables?data-type=variant")
    data = rv.get_json()

    assert isinstance(data, list) and len(data) == 0

    rv = client.post("/tables?data-type=variant")
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=invalid_data_type")
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    data = rv.get_json()

    assert rv.status_code == 201
    validate(data, DATASET_SCHEMA)

    rv = client.post("/tables?data-type=variant", json=["name", "metadata"])
    assert rv.status_code == 400  # Not an object

    rv = client.post("/tables?data-type=variant", json={"metadata": {}})
    assert rv.status_code == 400  # No name

    rv = client.post("/tables?data-type=variant", json={"name": "test table"})
    assert rv.status_code == 400  # No metadata

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": None
    })
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    assert rv.status_code == 500  # No IDs left


def test_table_detail(client):
    client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })

    rv = client.get("/tables/none")
    assert rv.status_code == 404

    rv = client.get("/tables/fixed_id")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data, sort_keys=True) == json.dumps({
        "id": "fixed_id",
        "name": "test table",
        "metadata": {},
        "schema": VARIANT_SCHEMA,
        "assembly_ids": ["GRCh37"],
    }, sort_keys=True)

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 204

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 404


def test_table_data(app, client):
    mm = app.config["TABLE_MANAGER"]

    # Create a new table with ID fixed_id and name test
    table = mm.create_table_and_update("test", {})
    table.variant_store.append(VARIANT_1)
    table.variant_store.append(VARIANT_2)
    table.variant_store.append(VARIANT_3)

    rv = client.get("/private/tables/none/data")
    assert rv.status_code == 404

    rv = client.get("/private/tables/fixed_id/data")
    assert rv.status_code == 200
    data = rv.get_json()
    # TODO: Test schema
    assert json.dumps(data["schema"], sort_keys=True ) == json.dumps(VARIANT_SCHEMA, sort_keys=True)
    assert data["pagination"]["previous_page_url"] is None
    assert data["pagination"]["next_page_url"] is None
    assert json.dumps(data["data"], sort_keys=True) == json.dumps([
        VARIANT_1.as_chord_representation(),
        VARIANT_2.as_chord_representation(),
        VARIANT_3.as_chord_representation(),
    ], sort_keys=True)

    rv = client.get("/private/tables/fixed_id/data", query_string={"offset": "not_an_int"})
    assert rv.status_code == 400

    rv = client.get("/private/tables/fixed_id/data", query_string={"count": "not_an_int"})
    assert rv.status_code == 400

    rv = client.get("/private/tables/fixed_id/data", query_string={"offset": 1})
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["pagination"]["previous_page_url"] is not None
    assert data["pagination"]["next_page_url"] is None
    assert json.dumps(data["data"], sort_keys=True) == json.dumps([
        VARIANT_2.as_chord_representation(),
        VARIANT_3.as_chord_representation(),
    ], sort_keys=True)

    rv = client.get("/private/tables/fixed_id/data", query_string={"offset": 1, "count": 1})
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["pagination"]["previous_page_url"] is not None
    assert data["pagination"]["next_page_url"] is not None
    assert json.dumps(data["data"], sort_keys=True) == json.dumps([VARIANT_2.as_chord_representation()], sort_keys=True)

    rv = client.get("/private/tables/fixed_id/data", query_string={"count": 1})
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["pagination"]["previous_page_url"] is None
    assert data["pagination"]["next_page_url"] is not None
    assert json.dumps(data["data"], sort_keys=True) == json.dumps([VARIANT_1.as_chord_representation()], sort_keys=True)
