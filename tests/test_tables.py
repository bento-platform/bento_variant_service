from chord_variant_service.variants import VARIANT_TABLE_METADATA_SCHEMA
from jsonschema import validate

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
    assert rv.status_code == 404

    rv = client.get("/tables?data-type=variant")
    data = rv.get_json()

    assert isinstance(data, list) and len(data) == 0

    rv = client.post("/tables?data-type=variant")
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    data = rv.get_json()

    assert rv.status_code == 201
    validate(data, DATASET_SCHEMA)

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

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 204

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 404
