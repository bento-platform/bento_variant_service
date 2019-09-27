from jsonschema import validate


# TODO: Move schemas to chord_lib

DATA_TYPE_SCHEMA = {
    "$id": "https://distributedgenomics.ca/chord/data_type.schema.json",  # TODO: Not a real URL
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "schema": {"$ref": "http://json-schema.org/draft-07/schema#"}
    },
    "required": ["id", "schema"]
}

DATA_TYPES_SCHEMA = {
    "$id": "https://distributedgenomics.ca/chord/data_types.schema.json",  # TODO: Not a real URL
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "array",
    "items": DATA_TYPE_SCHEMA
}


def test_data_types(client):
    rv = client.get("/data-types")
    data = rv.get_json()

    validate(data, DATA_TYPES_SCHEMA)
    assert len(data) == 1
    assert data[0]["id"] == "variant"
    # TODO: Check schemas are valid


def test_data_type_variant(client, json_schema):
    rv = client.get("/data-types/variant")
    data = rv.get_json()

    validate(data, DATA_TYPE_SCHEMA)
    assert data["id"] == "variant"
    validate(data["schema"], json_schema)  # Validate that the schema given is a valid JSON schema


def test_data_type_variant_schema(client, json_schema):
    rv = client.get("/data-types/variant/schema")
    data = rv.get_json()
    validate(data, json_schema)
