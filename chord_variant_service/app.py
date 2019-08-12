import chord_variant_service
import datetime
import os
import requests
# noinspection PyUnresolvedReferences
import tabix
import tqdm
import uuid

from flask import Flask, g, json, jsonify, request
from multiprocessing import Pool

WORKERS = len(os.sched_getaffinity(0))

# Possible operations: eq, lt, gt, le, ge, co

VARIANT_SCHEMA = {
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
    "type": "object",
    "required": ["chromosome", "start", "end", "ref", "alt"],
    "operations": [],
    "properties": {
        "chromosome": {
            "type": "string",
            "operations": ["eq"],
            "canNegate": False,
            "requiredForSearch": True
        },
        "start": {
            "type": "integer",
            "operations": ["eq"],
            "canNegate": False,
            "requiredForSearch": True
        },
        "end": {
            "type": "integer",
            "operations": ["eq"],
            "canNegate": False,
            "requiredForSearch": True
        },
        "ref": {
            "type": "string",
            "operations": ["eq"],
            "canNegate": True,
            "requiredForSearch": False
        },
        "alt": {
            "type": "string",
            "operations": ["eq"],
            "canNegate": True,
            "requiredForSearch": False
        }
    }
}


application = Flask(__name__)

DATA_PATH = os.environ.get("DATA", "data/")
datasets = {}


def get_pool():
    if "pool" not in g:
        g.pool = Pool(processes=WORKERS)

    return g.pool


@application.teardown_appcontext
def teardown_pool(err):
    if err is not None:
        print(err)
    pool = g.pop("pool", None)
    if pool is not None:
        pool.close()


def update_datasets():
    global datasets
    datasets = {d: [file for file in os.listdir(os.path.join(DATA_PATH, d)) if file[-6:] == "vcf.gz"]
                for d in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, d))}


update_datasets()
if len(datasets.keys()) == 0:
    # Add some fake data
    new_id_1 = str(uuid.uuid4())
    new_id_2 = str(uuid.uuid4())

    os.makedirs(os.path.join(DATA_PATH, new_id_1))
    os.makedirs(os.path.join(DATA_PATH, new_id_2))

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "CEU.trio.2010_07.indel.sites.vcf.gz", stream=True) as r:
        with open(os.path.join(DATA_PATH, new_id_1, "ceu.vcf.gz"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "CEU.trio.2010_07.indel.sites.vcf.gz.tbi",
                      stream=True) as r:
        with open(os.path.join(DATA_PATH, new_id_1, "ceu.vcf.gz.tbi"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "YRI.trio.2010_07.indel.sites.vcf.gz",
                      stream=True) as r:
        with open(os.path.join(DATA_PATH, new_id_2, "yri.vcf.gz"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "YRI.trio.2010_07.indel.sites.vcf.gz.tbi",
                      stream=True) as r:
        with open(os.path.join(DATA_PATH, new_id_2, "yri.vcf.gz.tbi"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    update_datasets()


def data_type_404(data_type_id):
    return json.dumps({
        "code": 404,
        "message": "Data type not found",
        "timestamp": datetime.datetime.utcnow().isoformat("T") + "Z",
        "errors": [{"code": "not_found", "message": f"Data type with ID {data_type_id} was not found"}]
    })


@application.route("/data-types", methods=["GET"])
def data_type_list():
    # Data types are basically stand-ins for schema blocks

    return jsonify([{"id": "variant", "schema": VARIANT_SCHEMA}])


@application.route("/data-types/variant", methods=["GET"])
def data_type_detail():
    return jsonify({
        "id": "variant",
        "schema": VARIANT_SCHEMA
    })


@application.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)


@application.route("/datasets", methods=["GET"])
def dataset_list():
    dt = request.args.get("data-type", default="")

    if dt != "variant":
        return data_type_404(dt)

    return jsonify([{
        "id": d,
        "schema": VARIANT_SCHEMA
    } for d in datasets.keys()])


# TODO: Implement
# @application.route("/datasets/<uuid:dataset_id>", methods=["GET"])


SEARCH_NEGATION = ("pos", "neg")
SEARCH_CONDITIONS = ("eq", "lt", "le", "gt", "ge", "co")
SQL_SEARCH_CONDITIONS = {
    "eq": "=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "co": "LIKE"
}


def search_worker_prime(d, chromosome, start_pos, end_pos, ref_query, alt_query, condition_dict):
    found = False
    for vcf in (os.path.join(DATA_PATH, d, vf) for vf in datasets[d]):
        if found:
            break

        tbx = tabix.open(vcf)

        try:
            # TODO: Security of passing this? Verify values
            # TODO: Support negation of ref/alt eq
            for row in tbx.query(chromosome, start_pos, end_pos):
                if found:
                    break

                if ref_query and not alt_query:
                    found = found or row[3].upper() == condition_dict["ref"]["searchValue"].upper()
                elif not ref_query and alt_query:
                    found = found or row[4].upper() == condition_dict["alt"]["searchValue"].upper()
                elif ref_query and alt_query:
                    found = found or (row[3].upper() == condition_dict["ref"]["searchValue"].upper() and
                                      row[4].upper() == condition_dict["alt"]["searchValue"].upper())
                else:
                    found = True

        except ValueError as e:
            # TODO
            print(str(e))
            break

    return {"id": d, "data_type": "variant"} if found else None


def search_worker(args):
    return search_worker_prime(*args)


@application.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    dt = request.json["dataTypeID"]
    if dt != "variant":
        # TODO: Error
        return jsonify({"results": []})

    conditions = request.json["conditions"]
    conditions_filtered = [c for c in conditions
                           if c["searchField"].split(".")[-1] in VARIANT_SCHEMA["properties"].keys() and
                           c["negation"] in SEARCH_NEGATION and c["condition"] in SEARCH_CONDITIONS]

    condition_fields = [c["searchField"].split(".")[-1] for c in conditions_filtered]

    if "chromosome" not in condition_fields or "start" not in condition_fields or "end" not in condition_fields:
        # TODO: Error
        # TODO: Not hardcoded?
        # TODO: More conditions
        return jsonify({"results": []})

    # TODO: Handle non-equality

    condition_dict = {c["searchField"].split(".")[-1]: c for c in conditions_filtered}
    dataset_results = []

    try:
        chromosome = condition_dict["chromosome"]["searchValue"]
        start_pos = int(condition_dict["start"]["searchValue"])
        end_pos = int(condition_dict["end"]["searchValue"])
        ref_query = "ref" in condition_dict
        alt_query = "alt" in condition_dict

        pool = get_pool()
        dataset_results = [d for d in pool.imap_unordered(
            search_worker,
            ((d, chromosome, start_pos, end_pos, ref_query, alt_query, condition_dict) for d in datasets)
        ) if d is not None]

    except ValueError as e:
        # TODO
        print(str(e))

    return jsonify({"results": dataset_results})


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

    return jsonify({
        "id": "ca.distributedgenomics.chord_variant_service",  # TODO: Should be globally unique?
        "name": "CHORD Variant Service",                       # TODO: Should be globally unique?
        "type": "urn:ga4gh:search",                            # TODO
        "description": "Variant service for a CHORD application.",
        "organization": "GenAP",
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__,
        "extension": {}
    })
