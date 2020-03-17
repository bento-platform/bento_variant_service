# noinspection PyProtectedMember
from chord_variant_service.beacon.routes import generate_beacon_id
from chord_variant_service.beacon.datasets import make_beacon_dataset_id
from chord_variant_service.pool import get_pool, teardown_pool
from chord_variant_service.tables.memory import MemoryTableManager
from jsonschema import validate
from uuid import uuid4

from .shared_data import VARIANT_1, VARIANT_2, VARIANT_3

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

BEACON_ERROR_SCHEMA = {
    "type": "object",
    "required": ["errorCode"],
    "properties": {
        "errorCode": {
            "type": "integer",
            "format": "int32"
        },
        "errorMessage": {
            "type": "string"
        }
    }
}

BEACON_DATASET_ALLELE_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["datasetId"],
    "properties": {
        "datasetId": {"type": "string"},
        "exists": {"type": "boolean"},
        "error": {
            "$ref": "#/components/schemas/BeaconError"
        },
        "frequency": {
            "type": "number",
            "minimum": 0,
            "maximum": 1
        },
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
        "note": {"type": "string"},
        "externalUrl": {"type": "string"},
        "info": {
            "type": "array",
            "items": BEACON_KEY_VALUE_PAIR_SCHEMA
        }
    }
}

BEACON_ALLELE_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["beaconId"],
    "properties": {
        "beaconId": {"type": "string"},
        "apiVersion": {"type": "string"},
        "exists": {"type": "boolean"},
        "alleleRequest": BEACON_ALLELE_REQUEST_SCHEMA,
        "datasetAlleleResponses": {
            "type": "array",
            "items": BEACON_DATASET_ALLELE_RESPONSE_SCHEMA
        },
        "error": BEACON_ERROR_SCHEMA
    }
}

BEACON_SCHEMA = {
    "type": "object",
    "required": ["id", "name", "apiVersion", "organization", "datasets"],
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


SHARED_REQUEST_BASE = {
    "referenceName": "1",
    "assemblyId": "GRCh37",
    "includeDatasetResponses": "HIT"
}


BEACON_REQUEST_1 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
}

BEACON_REQUEST_2 = {  # test inference of startMax
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "startMin": 4999,  # 0-based coordinates
    "endMax": 5001,  # "
}

BEACON_REQUEST_3 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
    "datasetIds": ["fixed_id:GRCh37"]
}


EMPTY_BEACON_REQUEST_1 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
    "datasetIds": ["does_not_exist"]
}

EMPTY_BEACON_REQUEST_2 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "G",
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
    "datasetIds": ["fixed_id:GRCh37"]
}

EMPTY_BEACON_REQUEST_3 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "G",
    "start": 4999,  # 0-based coordinates
    "end": 5999,  # "
    "datasetIds": ["fixed_id:GRCh37"]
}


INVALID_BEACON_REQUEST_1 = {
    **SHARED_REQUEST_BASE,
    "alternateBases": "T",
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
}

INVALID_BEACON_REQUEST_2 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "startMin": 5000,  # 0-based coordinates
    "startMax": 4999,  # "
}

INVALID_BEACON_REQUEST_3 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "startMin": 5000,  # 0-based coordinates
    "endMax": 4999,  # "
}

INVALID_BEACON_REQUEST_4 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "variantType": "DEL",  # Conflicts with alternateBases
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
}

INVALID_BEACON_REQUEST_5 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",  # No alternate
    "start": 4999,  # 0-based coordinates
    "end": 5000,  # "
}

INVALID_BEACON_REQUEST_6 = {
    **SHARED_REQUEST_BASE,
    "referenceBases": "C",
    "alternateBases": "T",
    "start": "b",  # Invalid start
    "end": 5000,
}


def test_generate_beacon_id():
    assert generate_beacon_id("example.org") == "org.example.beacon"
    assert generate_beacon_id("f2j-043.example.org:5000") == "org.example.f2j-043.5000.beacon"


def test_make_beacon_dataset_id():
    some_id = str(uuid4())
    assert make_beacon_dataset_id((some_id, "GRCh37")) == f"{some_id}:GRCh37"


def test_beacon_response(app, client, table_manager):
    # Add a dummy dataset first (beacon API needs one or more datasets)

    with app.app_context():
        pool = get_pool()

        try:
            mm: MemoryTableManager = table_manager

            # Create a new dataset with ID fixed_id and name test table, and add a variant to it
            ds = mm.create_table_and_update("test table", {})

            # Test valid but empty dataset
            rv = client.post("/beacon/query", json=BEACON_REQUEST_3)
            assert rv.status_code == 200

            data = rv.get_json()

            validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
            assert not data["exists"]
            assert len(data["datasetAlleleResponses"]) == 0

            # Add variants to store
            ds.add_variant(VARIANT_1)
            ds.add_variant(VARIANT_2)
            ds.add_variant(VARIANT_3)

            # Check beacon info response

            rv = client.get("/beacon")
            data = rv.get_json()

            validate(data, BEACON_SCHEMA)

            # Beacon query (JSON)

            # - Bad queries

            # -- No query
            rv = client.post("/beacon/query")
            assert rv.status_code == 400

            # -- Invalid queries
            for q in (INVALID_BEACON_REQUEST_1, INVALID_BEACON_REQUEST_2, INVALID_BEACON_REQUEST_3,
                      INVALID_BEACON_REQUEST_4, INVALID_BEACON_REQUEST_5, INVALID_BEACON_REQUEST_6):
                invalid_data = []

                rv = client.post("/beacon/query", json=q)
                assert rv.status_code == 400
                invalid_data.append(rv.get_json())

                rv = client.get("/beacon/query", query_string=q)
                assert rv.status_code == 400
                invalid_data.append(rv.get_json())

                for data in invalid_data:
                    assert isinstance(data, dict) and len(list(data.keys())) in (1, 2) and "errorCode" in data
                    assert data["errorCode"] == 400
                    assert "errorMessage" not in data or isinstance(data["errorMessage"], str)

            # - Valid queries

            for q in (BEACON_REQUEST_1, BEACON_REQUEST_2, BEACON_REQUEST_3):
                valid_data = []

                rv = client.post("/beacon/query", json=q)
                assert rv.status_code == 200
                valid_data.append(rv.get_json())

                rv = client.get("/beacon/query", query_string=q)
                assert rv.status_code == 200
                valid_data.append(rv.get_json())

                for data in valid_data:
                    validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
                    assert data["exists"]
                    assert len(data["datasetAlleleResponses"]) == 1
                    assert data["datasetAlleleResponses"][0]["datasetId"] == "fixed_id:GRCh37"

            # - Empty queries

            for q in (EMPTY_BEACON_REQUEST_1, EMPTY_BEACON_REQUEST_2, EMPTY_BEACON_REQUEST_3):
                valid_data = []

                rv = client.post("/beacon/query", json=q)
                assert rv.status_code == 200
                valid_data.append(rv.get_json())

                rv = client.get("/beacon/query", query_string=q)
                assert rv.status_code == 200
                valid_data.append(rv.get_json())

                for data in valid_data:
                    validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
                    assert not data["exists"]
                    assert len(data["datasetAlleleResponses"]) == 0

            # Test different includeDatasetResponses values

            br_all = {
                **BEACON_REQUEST_1,
                "includeDatasetResponses": "ALL"
            }

            br_miss = {
                **BEACON_REQUEST_1,
                "includeDatasetResponses": "MISS"
            }

            br_none = {
                **BEACON_REQUEST_1,
                "includeDatasetResponses": "NONE"
            }

            # All

            rv = client.post("/beacon/query", json=br_all)
            assert rv.status_code == 200

            data = rv.get_json()

            validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
            assert data["exists"]
            assert len(data["datasetAlleleResponses"]) == 2
            assert data["datasetAlleleResponses"][0]["datasetId"] == "fixed_id:GRCh37"
            assert data["datasetAlleleResponses"][0]["exists"]
            assert data["datasetAlleleResponses"][1]["datasetId"] == "fixed_id:GRCh38"
            assert not data["datasetAlleleResponses"][1]["exists"]

            # Miss

            rv = client.post("/beacon/query", json=br_miss)
            assert rv.status_code == 200

            data = rv.get_json()

            validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
            assert data["exists"]
            assert len(data["datasetAlleleResponses"]) == 1
            assert data["datasetAlleleResponses"][0]["datasetId"] == "fixed_id:GRCh38"
            assert not data["datasetAlleleResponses"][0]["exists"]

            # None

            rv = client.post("/beacon/query", json=br_none)
            assert rv.status_code == 200

            data = rv.get_json()

            validate(data, BEACON_ALLELE_RESPONSE_SCHEMA)
            assert data["exists"]
            assert len(data["datasetAlleleResponses"]) == 0

        finally:
            teardown_pool(None)
            pool.join()
