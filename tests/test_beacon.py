# noinspection PyProtectedMember
from chord_variant_service.beacon import generate_beacon_id
from chord_variant_service.datasets import make_beacon_dataset_id
from jsonschema import validate
from uuid import uuid4

# Adapted from the OpenAPI 1.0.1 spec
# https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml

BEACON_KEY_VALUE_PAIR_SCHEMA = {
    "type": "object",
    "required": ["key", "value"]
}

BEACON_ORGANIZATION_SCHEMA = {
    "type": "object",
    "required": ["id", "name"],
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "address": {"type": "string"},
        "welcomeUrl": {"type": "string"},
        "contactUrl": {"type": "string"},
        "logoUrl": {"type": "string"},
        "info": {
            "type": "array",
            "items": BEACON_KEY_VALUE_PAIR_SCHEMA
        }
    }
}

BEACON_DATASET_SCHEMA = {
    "type": "object",
    "required": ["id", "name", "assemblyId", "createDateTime", "updateDateTime"],
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "assemblyId": {"type": "string"},
        "createDateTime": {"type": "string"},
        "updateDateTime": {"type": "string"},
        "version": {"type": "string"},
        "variantCount": {
            "type": "integer",
            "format": "int64",
            "minimum": 0
        },
        "callCount": {
            "type": "integer",
            "format": "int64",
            "minimum": 0
        },
        "sampleCount": {
            "type": "integer",
            "format": "int64",
            "minimum": 0
        },
        "externalUrl": {"type": "string", },
        "info": {
            "type": "array",
            "items": BEACON_KEY_VALUE_PAIR_SCHEMA
        },
        "dataUseConditions": {
            # TODO
            "type": "object"
        }
    }
}

BEACON_ALLELE_REQUEST_SCHEMA = {
    "type": "object",
    "required": ["referenceName", "referenceBases", "assemblyId"],
    "properties": {
        "referenceName": {
            "type": "string",
            "enum": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                     "19", "20", "21", "22", "X", "Y", "MT"],
        },
        "start": {
            "type": "integer",
            "format": "int64",
            "minimum": 0
        },
        "end": {"type": "integer"},
        "startMin": {"type": "integer"},
        "startMax": {"type": "integer"},
        "endMin": {"type": "integer"},
        "endMax": {"type": "integer"},
        "referenceBases": {
            "type": "string",
            "pattern": "^([ACGT]+|N)$"
        },
        "alternateBases": {
            "type": "string",
            "pattern": "^([ACGT]+|N)$"
        },
        "variantType": {"type": "string"},
        "assemblyId": {"type": "string"},
        "datasetIds": {
            "type": "array",
            "items": {"type": "string"}
        },
        "includeDatasetResponses": {
            "type": "string",
            "enum": ["ALL", "HIT", "MISS", "NONE"]
        }
    }
}

BEACON_SCHEMA = {
    "type": "object",
    "required": [
        "id",
        "name",
        "apiVersion",
        "organization",
        "datasets"
    ],
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "apiVersion": {"type": "string"},
        "organization": BEACON_ORGANIZATION_SCHEMA,
        "description": {"type": "string"},
        "version": {"type": "string"},
        "welcomeUrl": {"type": "string"},
        "alternativeUrl": {"type": "string"},
        "createDateTime": {"type": "string"},
        "updateDateTime": {"type": "string"},
        "datasets": {
            "minItems": 1,
            "type": "array",
            "items": BEACON_DATASET_SCHEMA
        },
        "sampleAlleleRequests": {
            "type": "array",
            "items": BEACON_ALLELE_REQUEST_SCHEMA
        },
        "info": {
            "description": "Additional structured metadata, key-value pairs.",
            "type": "array",
            "items": BEACON_KEY_VALUE_PAIR_SCHEMA
        }
    }
}


def test_generate_beacon_id():
    assert generate_beacon_id("example.org") == "org.example.beacon"
    assert generate_beacon_id("f2j-043.example.org:5000") == "org.example.f2j-043.5000.beacon"


def test_make_beacon_dataset_id():
    some_id = str(uuid4())
    assert make_beacon_dataset_id((some_id, "GRCh37")) == f"{some_id}:GRCh37"


def test_beacon_response(client):
    # Add a dummy dataset first (beacon API needs one or more datasets) TODO ENCODE
    client.post("/datasets?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })

    rv = client.get("/beacon")
    data = rv.get_json()

    validate(data, BEACON_SCHEMA)
