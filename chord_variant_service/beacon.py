import chord_variant_service
import os

from flask import Blueprint, current_app, json, jsonify, request
from jsonschema import validate, ValidationError
from urllib.parse import urlparse

from .datasets import datasets
from .search import generic_variant_search


CHORD_URL = os.environ.get("CHORD_URL", "http://localhost:5000/")
CHORD_DOMAIN = urlparse(CHORD_URL).netloc

# Create a reverse DNS beacon ID, e.g. com.dlougheed.1.beacon
BEACON_ID = ".".join((
    *(CHORD_DOMAIN.split(":")[0].split(".")),
    *((CHORD_DOMAIN.split(':')[1],) if len(CHORD_DOMAIN.split(":")) > 1 else ()),
    "beacon"
))

BEACON_IDR_ALL = "ALL"
BEACON_IDR_HIT = "HIT"
BEACON_IDR_MISS = "MISS"
BEACON_IDR_NONE = "NONE"

BEACON_API_VERSION = "v1.0"

bp_beacon = Blueprint("beacon", __name__)

with bp_beacon.open_resource("schemas/beacon_allele_request.schema.json") as bars:
    BEACON_ALLELE_REQUEST_SCHEMA = json.load(bars)


@bp_beacon.route("/beacon", methods=["GET"])
def beacon_get():
    return jsonify({
        "id": BEACON_ID,  # TODO
        "name": f"CHORD Beacon (ID: {BEACON_ID})",  # TODO: Nicer name
        "apiVersion": BEACON_API_VERSION,
        "organization": {  # TODO: Make this dynamic for user?
            "id": "ca.computationalgenomics",
            "name": "Canadian Centre for Computational Genomics"
        },
        "description": "Beacon provided for a researcher by a CHORD instance.",  # TODO, optional
        "version": chord_variant_service.__version__,
        "datasets": [{
            "id": d_id,
            "name": d["name"],
            "assemblyId": "TODO",  # TODO
            "createDateTime": "TODO",  # TODO
            "updateDateTime": "TODO"  # TODO
        } for d_id, d in datasets.items()]
    })


@bp_beacon.route("/beacon/query", methods=["GET", "POST"])
def beacon_query():
    # TODO: Careful with end, it should be exclusive

    if request.method == "POST":
        # TODO: What if request.json is non-dict? Should handle better
        query = {k: v for k, v in request.json if v is not None}
    else:
        query = {k: v for k, v in ({
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
            "assemblyId": request.args.get("assemblyId"),  # TODO
            "datasetIds": request.args.get("datasetIds", None),
            "includeDatasetResponses": request.args.get("includeDatasetResponses", BEACON_IDR_NONE)
        }).items() if v is not None}

    # Validate query

    try:
        validate(instance=query, schema=BEACON_ALLELE_REQUEST_SCHEMA)
    except ValidationError:
        return current_app.response_class(status=400)  # TODO: Beacon error response

    # TODO: Other validation, or put more in schema?

    # TODO: Run query
    #  All coordinates are 0 INDEXED!
    #  - referenceName: chromosome
    #  - start: precise, equivalent to (startMin
    #  - startMin: equivalent to start >= x
    #  - startMax: equivalent to start <= x
    #  - end: precise, equivalent to (endMin = endMax = x - 1)
    #  - endMin: equivalent to end >= x
    #  - endMax: equivalent to end <= x
    #  - referenceBases === ref
    #  - alternateBases === alt
    #  - variantType: how to implement? looks like maybe an enum of DEL, INS, DUP, INV, CNV, DUP:TANDEM, DEL:ME, INS:ME
    #  - assemblyId: how to implement? metadata? what if it's missing?
    #  - datasetIds: do we implement?
    #  - includeDatasetResponses: include datasetAlleleResponses?
    # TODO: Are max/min inclusive? Looks like it

    # For tabix:
    #  - referenceName, startMin, endMax are passed as is
    #  - start: reduce to startMax = startMin
    #  - end:   endMin = endMax
    #  - startMax, endMin are iterated
    #  - referenceBases, alternateBases are iterated
    #  - need op for referenceBases / alternateBases (CHORD search)
    #  - TODO: How to do variantType?
    #  - TODO: Assembly ID - in VCF header?

    # TODO: Check we have one of these... rules in Beacon schema online?

    start = query.get("start", None)
    start_min = query.get("startMin", None)
    start_max = query.get("startMax", None)

    end = query.get("end", None)
    end_min = query.get("endMin", None)
    end_max = query.get("endMax", None)

    if start is not None:
        start_min = start
        start_max = start

    if end is not None:
        # Subtract one, since end is exclusive
        end_min = end - 1
        end_max = end - 1

    # Convert to VCF coordinates (1-indexed)

    start_min = start_min + 1 if start_min is not None else None
    start_max = start_max + 1 if start_max is not None else None
    end_min = end_min + 1 if end_min is not None else None
    end_max = end_max + 1 if end_max is not None else None

    # TODO: Start can be used without end, calculate max end!! (via referenceBases?)

    ref = query.get("referenceBases", None)
    alt = query.get("alternateBases", None)

    # TODO: variantType, assemblyId, datasetIds

    results = generic_variant_search(chromosome=query["referenceName"], start_min=start_min, start_max=start_max,
                                     end_min=end_min, end_max=end_max, ref=ref, alt=alt)

    include_dataset_responses = query.get("includeDatasetResponses", BEACON_IDR_NONE)
    dataset_matches = [ds["id"] for ds in results]
    if include_dataset_responses == BEACON_IDR_ALL:
        beacon_datasets = [{"datasetId": ds, "exists": ds in dataset_matches} for ds in datasets.keys()]
    elif include_dataset_responses == BEACON_IDR_HIT:
        beacon_datasets = [{"datasetId": ds, "exists": True} for ds in dataset_matches]
    elif include_dataset_responses == BEACON_IDR_MISS:
        beacon_datasets = [{"datasetId": ds, "exists": False} for ds in datasets.keys() if ds not in dataset_matches]
    else:  # BEACON_IDR_NONE
        # Don't return anything
        beacon_datasets = None

    return jsonify({
        "beaconId": BEACON_ID,
        "apiVersion": BEACON_API_VERSION,
        "exists": len(dataset_matches) > 0,
        "alleleRequest": query,
        "datasetAlleleResponses": beacon_datasets
    })
