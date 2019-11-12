import chord_variant_service
import os

from chord_lib.search.queries import (
    convert_query_to_ast,
    and_asts_to_ast,

    FUNCTION_RESOLVE,
    FUNCTION_LE,
    FUNCTION_GE,
    FUNCTION_EQ
)

from itertools import chain
from flask import Blueprint, current_app, json, jsonify, request
from jsonschema import validate, ValidationError
from urllib.parse import urlparse

from .datasets import TableManager
from .search import generic_variant_search


CHORD_URL = os.environ.get("CHORD_URL", "http://localhost:5000/")
CHORD_DOMAIN = urlparse(CHORD_URL).netloc

BEACON_IDR_ALL = "ALL"
BEACON_IDR_HIT = "HIT"
BEACON_IDR_MISS = "MISS"
BEACON_IDR_NONE = "NONE"

BEACON_API_VERSION = "v1.0"

bp_beacon = Blueprint("beacon", __name__)

with bp_beacon.open_resource("schemas/beacon_allele_request.schema.json") as bars:
    BEACON_ALLELE_REQUEST_SCHEMA = json.load(bars)


def generate_beacon_id(domain: str) -> str:
    return ".".join((
        *(reversed(domain.split(":")[0].split("."))),
        *((domain.split(':')[1],) if len(domain.split(":")) > 1 else ()),
        "beacon"
    ))


# Create a reverse DNS beacon ID, e.g. com.dlougheed.1.beacon
BEACON_ID = generate_beacon_id(CHORD_DOMAIN)


@bp_beacon.route("/beacon", methods=["GET"])
def beacon_get():
    return jsonify({
        "id": BEACON_ID,
        "name": f"CHORD Beacon (ID: {BEACON_ID})",  # TODO: Nicer name
        "apiVersion": BEACON_API_VERSION,
        "organization": {  # TODO: Make this dynamic for user?
            "id": "ca.computationalgenomics",
            "name": "Canadian Centre for Computational Genomics"
        },
        "description": "Beacon provided for a researcher by a CHORD instance.",  # TODO: More specific
        "version": chord_variant_service.__version__,
        "datasets": [d.as_beacon_dataset_response()
                     for d in current_app.config["TABLE_MANAGER"].get_beacon_datasets().values()]
    })


@bp_beacon.route("/beacon/query", methods=["GET", "POST"])
def beacon_query():
    # TODO: Careful with end, it should be exclusive

    query = {
        k: v for k, v in (request.json if request.method == "POST" else {
            "referenceName": request.args.get("referenceName"),
            "start": request.args.get("start", None),
            "startMin": request.args.get("startMin", None),
            "startMax": request.args.get("startMax", None),
            "end": request.args.get("end", None),
            "endMin": request.args.get("endMin", None),
            "endMax": request.args.get("endMax", None),
            "referenceBases": request.args.get("referenceBases", "N"),
            "alternateBases": request.args.get("alternateBases", "N"),
            "variantType": request.args.get("variantType", None),
            "assemblyId": request.args.get("assemblyId"),
            "datasetIds": request.args.get("datasetIds", None),
            "includeDatasetResponses": request.args.get("includeDatasetResponses", BEACON_IDR_NONE)
        }) if v is not None
    }

    # Validate query

    try:
        validate(instance=query, schema=BEACON_ALLELE_REQUEST_SCHEMA)
    except ValidationError:
        return current_app.response_class(status=400)  # TODO: Beacon error response

    # TODO: Other validation, or put more in schema?

    start = query.get("start", None)
    start_min = query.get("startMin", None) if start is None else start
    start_max = query.get("startMax", None) if start is None else start

    end = query.get("end", None)
    end_min = query.get("endMin", None) if end is None else end - 1  # Subtract one, since end is exclusive
    end_max = query.get("endMax", None) if end is None else end - 1  # "

    # Convert to VCF coordinates (1-indexed)

    start_min = start_min + 1 if start_min is not None else None
    start_max = start_max + 1 if start_max is not None else None
    end_min = end_min + 1 if end_min is not None else None
    end_max = end_max + 1 if end_max is not None else None

    # TODO: Start can be used without end, calculate max end!! (via referenceBases?)

    ref = query.get("referenceBases", None)

    alt_bases = query.get("alternateBases", None)
    alt_id = query.get("variantType", None)  # e.g. DUP, DEL [symbolic alternates]

    if (alt_bases is None and alt_id is None) or (alt_bases is not None and alt_id is not None):
        # Error one or the other is required
        return current_app.response_class(status=400)  # TODO: Beacon error response

    assembly_id = query["assemblyId"]

    dataset_ids = query.get("datasetIds", None)
    if dataset_ids is not None:
        dataset_ids = tuple(set(d.split(":")[0] for d in dataset_ids))

    table_manager: TableManager = current_app.config["TABLE_MANAGER"]

    # Create an additional filtering query based on the rest of the Beacon request
    rest_of_query = and_asts_to_ast(tuple(
        convert_query_to_ast([fn, [FUNCTION_RESOLVE, field], value])
        for fn, field, value in (
            (FUNCTION_GE, "end", end_min),
            (FUNCTION_LE, "end", end_max),
            (FUNCTION_EQ, "ref", ref),

            # One of the two below will be none
            (FUNCTION_EQ, "alt", alt_bases),
            (FUNCTION_EQ, "alt", alt_id),
        ) if value is not None
    ))

    results = generic_variant_search(table_manager, chromosome=query["referenceName"], start_min=start_min,
                                     start_max=start_max, rest_of_query=and_asts_to_ast(tuple(query_list)),
                                     assembly_id=assembly_id, dataset_ids=dataset_ids)

    include_dataset_responses = query.get("includeDatasetResponses", BEACON_IDR_NONE)
    dataset_matches = set(bd.beacon_id for bd in chain.from_iterable(d.beacon_datasets for d, _ in results)
                          if bd.assembly_id == assembly_id)

    if include_dataset_responses == BEACON_IDR_ALL:
        beacon_dataset_hits = [{"datasetId": bd.beacon_id, "exists": bd.beacon_id in dataset_matches}
                               for bd in table_manager.get_beacon_datasets().values()]

    elif include_dataset_responses == BEACON_IDR_HIT:
        beacon_dataset_hits = [{"datasetId": ds, "exists": True} for ds in dataset_matches]

    elif include_dataset_responses == BEACON_IDR_MISS:
        beacon_dataset_hits = [{"datasetId": bd.beacon_id, "exists": False}
                               for bd in table_manager.get_beacon_datasets().values()
                               if bd.beacon_id not in dataset_matches]

    else:  # BEACON_IDR_NONE
        # Don't return anything
        beacon_dataset_hits = None

    return jsonify({
        "beaconId": BEACON_ID,
        "apiVersion": BEACON_API_VERSION,
        "exists": len(dataset_matches) > 0,
        "alleleRequest": query,
        "datasetAlleleResponses": beacon_dataset_hits
    })
