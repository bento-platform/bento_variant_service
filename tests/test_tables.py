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


def test_datasets(client):
    rv = client.get("/datasets")
    assert rv.status_code == 404

    rv = client.get("/datasets?data-type=variant")
    data = rv.get_json()

    assert isinstance(data, list) and len(data) == 0

    rv = client.post("/datasets?data-type=variant")
    assert rv.status_code == 400

    rv = client.post("/datasets?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    data = rv.get_json()

    assert rv.status_code == 201
    validate(data, DATASET_SCHEMA)
