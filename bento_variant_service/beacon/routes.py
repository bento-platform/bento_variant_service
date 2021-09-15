# Implementation of the GA4GH Beacon v1.0.1 specification
# https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml

import bento_variant_service
import os

from bento_lib.search.queries import (
    convert_query_to_ast,
    and_asts_to_ast,

    FUNCTION_RESOLVE,
    FUNCTION_NOT,
    FUNCTION_LE,
    FUNCTION_GE,
    FUNCTION_EQ
)

from flask import Blueprint, json, jsonify, request, Response
from itertools import chain
from jsonschema import validate, ValidationError
from typing import Callable, List, Optional, Tuple
from urllib.parse import urlparse

from bento_variant_service.search import generic_variant_search
from bento_variant_service.tables.base import TableManager
from bento_variant_service.table_manager import get_table_manager
from bento_variant_service.variants.genotypes import GT_HOMOZYGOUS_REFERENCE


CHORD_URL = os.environ.get("CHORD_URL", "http://localhost:5000/")
CHORD_HOST = urlparse(CHORD_URL).netloc

BEACON_IDR_ALL = "ALL"
BEACON_IDR_HIT = "HIT"
BEACON_IDR_MISS = "MISS"
BEACON_IDR_NONE = "NONE"

BEACON_API_VERSION = "v1.0"

BEACON_SEARCH_TIMEOUT = 30

bp_beacon = Blueprint("beacon", __name__)

with bp_beacon.open_resource("schemas/beacon_allele_request.schema.json") as bars:
    BEACON_ALLELE_REQUEST_SCHEMA = json.load(bars)


def generate_beacon_id(domain: str) -> str:
    # https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L288
    return ".".join((
        *(reversed(domain.split(":")[0].split("."))),
        *((domain.split(':')[1],) if len(domain.split(":")) > 1 else ()),
        "beacon"
    ))


def beacon_error(error_code: int, error_message: Optional[str]) -> Tuple[Response, int]:
    # https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L619
    return jsonify({
        "errorCode": error_code,
        **({"errorMessage": error_message} if error_message is not None else {})
    }), error_code


# Create a reverse DNS beacon ID, e.g. com.dlougheed.1.beacon
BEACON_ID = generate_beacon_id(CHORD_HOST)


@bp_beacon.route("/beacon", methods=["GET"])
def beacon_get():
    # https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L279
    return jsonify({
        "id": BEACON_ID,
        "name": f"Bento Beacon (ID: {BEACON_ID})",  # TODO: Nicer name
        "apiVersion": BEACON_API_VERSION,
        "organization": {  # TODO: Make this dynamic for user?
            "id": "ca.computationalgenomics",
            "name": "Canadian Centre for Computational Genomics"
        },
        "description": "Beacon provided for a researcher by a Bento node.",  # TODO: More specific
        "version": bento_variant_service.__version__,
        "datasets": [d.as_beacon_dataset_response()
                     for d in get_table_manager().beacon_datasets.values()]
    })


def beacon_allele_response(exists: bool, allele_request: dict, dataset_allele_responses: List[dict]) -> Response:
    # https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L441
    return jsonify({
        "beaconId": BEACON_ID,
        "apiVersion": BEACON_API_VERSION,
        "exists": exists,
        "alleleRequest": allele_request,
        "datasetAlleleResponses": dataset_allele_responses
    })


def apply_if_not_none(f: Callable, v: Optional):
    return f(v) if v is not None else None


def filter_dict_none_values(d: dict):
    return {k: v for k, v in d.items() if v is not None}


def beacon_coord_to_vcf_coord(v: Optional[int], last: bool = False) -> Optional[int]:
    # https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L357
    return v + (2 if last else 1) if v is not None else None


@bp_beacon.route("/beacon/query", methods=["GET", "POST"])
def beacon_query():
    # TODO: Careful with end, it should be exclusive

    if request.method == "POST" and not isinstance(request.json, dict):
        return beacon_error(400, "Missing or invalid query")

    if request.method == "POST":
        query = request.json
    else:
        try:
            query = {
                "referenceName": request.args["referenceName"],
                "start": apply_if_not_none(int, request.args.get("start", None)),
                "startMin": apply_if_not_none(int, request.args.get("startMin", None)),
                "startMax": apply_if_not_none(int, request.args.get("startMax", None)),
                "end": apply_if_not_none(int, request.args.get("end", None)),
                "endMin": apply_if_not_none(int, request.args.get("endMin", None)),
                "endMax": apply_if_not_none(int, request.args.get("endMax", None)),
                "referenceBases": request.args["referenceBases"],
                "alternateBases": request.args.get("alternateBases", None),
                "variantType": request.args.get("variantType", None),
                "assemblyId": request.args["assemblyId"],
                "datasetIds": request.args.getlist("datasetIds"),
                "includeDatasetResponses": request.args.get("includeDatasetResponses", BEACON_IDR_NONE)
            }

            # TODO: Empty list vs. not specified...
            if len(query["datasetIds"]) == 0:
                del query["datasetIds"]

        except (KeyError, ValueError):
            return beacon_error(400, "Invalid query")

    # Filter out null values from query for validation
    query = filter_dict_none_values(query)

    # Validate query

    try:
        validate(instance=query, schema=BEACON_ALLELE_REQUEST_SCHEMA)
    except ValidationError:
        return beacon_error(400, "Invalid query")  # TODO: Detailed schema error message

    # TODO: Other validation, or put more in schema?

    # Retrieve coordinates (0-indexed)

    start_min = beacon_coord_to_vcf_coord(query.get("start", query.get("startMin", None)))
    start_max = beacon_coord_to_vcf_coord(query.get("start", query.get("startMax", None)), last=True)

    end_min = beacon_coord_to_vcf_coord(query.get("end", query.get("endMin", None)))
    end_max = beacon_coord_to_vcf_coord(query.get("end", query.get("endMax", None)), last=True)

    # Check that bounds make sense

    if start_min is not None and ((start_max is not None and start_max <= start_min) or
                                  (end_max is not None and end_max <= start_min)):
        return beacon_error(400, "Invalid variant bounds")

    # Sort out reference vs. alternate

    ref = query.get("referenceBases", None)

    alt_allele = query.get("alternateBases", None)
    alt_id = query.get("variantType", None)  # e.g. DUP, DEL [symbolic alternates]

    if (alt_allele is None and alt_id is None) or (alt_allele is not None and alt_id is not None):
        # Error one or the other is required
        return beacon_error(400, "Exactly one of alternateBases or variantType must be specified")

    # Get limiting assembly ID / dataset IDs for query

    assembly_id = query["assemblyId"]

    dataset_ids = query.get("datasetIds", None)
    if dataset_ids is not None:
        dataset_ids = tuple(set(d.split(":")[0] for d in dataset_ids))

    table_manager: TableManager = get_table_manager()

    # Create an additional filtering query based on the rest of the Beacon request, plus other filtering we want to do
    rest_of_query = and_asts_to_ast((
        *(
            convert_query_to_ast([fn, [FUNCTION_RESOLVE, *field], value])
            for fn, field, value in (
                (FUNCTION_GE, ("end",), end_min),
                (FUNCTION_LE, ("end",), end_max),
                (FUNCTION_EQ, ("ref",), ref),

                # One of the two below will be none
                (FUNCTION_EQ, ("calls", "[item]", "genotype_alleles", "[item]"), alt_allele),
                (FUNCTION_EQ, ("calls", "[item]", "genotype_alleles", "[item]"), alt_id),
            ) if value is not None
        ),

        # Only want interesting results
        convert_query_to_ast([FUNCTION_NOT, [
            FUNCTION_EQ,
            [FUNCTION_RESOLVE, "calls", "[item]", "genotype_type"],
            GT_HOMOZYGOUS_REFERENCE
        ]])
    ))

    # noinspection PyTypeChecker
    results = generic_variant_search(table_manager, chromosome=query["referenceName"], start_min=start_min,
                                     start_max=start_max, rest_of_query=rest_of_query, assembly_id=assembly_id,
                                     dataset_ids=dataset_ids, timeout=BEACON_SEARCH_TIMEOUT)

    include_dataset_responses = query.get("includeDatasetResponses", BEACON_IDR_NONE)
    dataset_matches = set(bd.beacon_id for bd in chain.from_iterable(d.beacon_datasets for d, _ in results)
                          if bd.assembly_id == assembly_id)

    if include_dataset_responses == BEACON_IDR_ALL:
        beacon_dataset_hits = [{"datasetId": bd.beacon_id, "exists": bd.beacon_id in dataset_matches}
                               for bd in table_manager.beacon_datasets.values()]

    elif include_dataset_responses == BEACON_IDR_HIT:
        beacon_dataset_hits = [{"datasetId": ds, "exists": True} for ds in dataset_matches]

    elif include_dataset_responses == BEACON_IDR_MISS:
        beacon_dataset_hits = [{"datasetId": bd.beacon_id, "exists": False}
                               for bd in table_manager.beacon_datasets.values()
                               if bd.beacon_id not in dataset_matches]

    else:  # BEACON_IDR_NONE
        # Don't return anything
        beacon_dataset_hits = []

    return beacon_allele_response(exists=len(dataset_matches) > 0,
                                  allele_request=query,
                                  dataset_allele_responses=beacon_dataset_hits)
