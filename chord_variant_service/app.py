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
from operator import eq, ne

WORKERS = len(os.sched_getaffinity(0))

# Possible operations: eq, lt, gt, le, ge, co
# TODO: Regex verification with schema, to front end

VARIANT_SCHEMA = {
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
    "type": "object",
    "required": ["chromosome", "start", "end", "ref", "alt"],
    "search": {
        "operations": [],
    },
    "properties": {
        "chromosome": {
            "type": "string",
            # TODO: Choices
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 0
            }
        },
        "start": {
            "type": "integer",
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 1
            }
        },
        "end": {
            "type": "integer",
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 2
            }
        },
        "ref": {
            "type": "string",
            "search": {
                "operations": ["eq"],
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 3
            }
        },
        "alt": {
            "type": "string",
            "search": {
                "operations": ["eq"],
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 4
            }
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


SEARCH_OPERATIONS = ("eq", "lt", "le", "gt", "ge", "co")
SQL_SEARCH_CONDITIONS = {
    "eq": "=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "co": "LIKE"
}


def search_worker_prime(d, chromosome, start_pos, end_pos, ref_query, alt_query, ref_op, alt_op, condition_dict,
                        internal_data):
    found = False
    matches = []
    for vcf in (os.path.join(DATA_PATH, d, vf) for vf in datasets[d]):
        if found:
            break

        tbx = tabix.open(vcf)

        try:
            # TODO: Security of passing this? Verify values
            # TODO: Support negation of ref/alt eq
            for row in tbx.query(chromosome, start_pos, end_pos):
                if not internal_data and found:
                    break

                if ref_query and not alt_query:
                    match = ref_op(row[3].upper(), condition_dict["ref"]["searchValue"].upper())
                elif not ref_query and alt_query:
                    match = alt_op(row[4].upper(), condition_dict["alt"]["searchValue"].upper())
                elif ref_query and alt_query:
                    match = (ref_op(row[3].upper(), condition_dict["ref"]["searchValue"].upper()) and
                             alt_op(row[4].upper(), condition_dict["alt"]["searchValue"].upper()))
                else:
                    match = True

                found = found or match
                if match and internal_data:
                    matches.append({
                        "chromosome": row[0],
                        "start": row[1],
                        "end": row[2],
                        "ref": row[3],
                        "alt": row[4]
                    })

        except ValueError as e:
            # TODO
            print(str(e))
            break

    if internal_data:
        return d, {"data_type": "variant", "matches": matches} if found else None

    return {"id": d, "data_type": "variant"} if found else None


def search_worker(args):
    return search_worker_prime(*args)


def search(dt, conditions, internal_data=False):
    null_result = {} if internal_data else []

    if dt != "variant":
        return null_result

    conditions_filtered = [c for c in conditions
                           if c["field"].split(".")[-1] in VARIANT_SCHEMA["properties"].keys() and
                           isinstance(c["negated"], bool) and c["operation"] in SEARCH_OPERATIONS]

    condition_fields = [c["field"].split(".")[-1] for c in conditions_filtered]

    if "chromosome" not in condition_fields or "start" not in condition_fields or "end" not in condition_fields:
        # TODO: Error
        # TODO: Not hardcoded?
        # TODO: More conditions
        return null_result

    condition_dict = {c["field"].split(".")[-1]: c for c in conditions_filtered}
    dataset_results = {} if internal_data else []

    try:
        chromosome = condition_dict["chromosome"]["searchValue"]  # TODO: Check domain for chromosome
        start_pos = int(condition_dict["start"]["searchValue"])
        end_pos = int(condition_dict["end"]["searchValue"])
        ref_query = "ref" in condition_dict
        alt_query = "alt" in condition_dict
        ref_op = ne if ref_query and condition_dict["ref"]["negated"] else eq
        alt_op = ne if alt_query and condition_dict["alt"]["negated"] else eq

        pool = get_pool()

        pool_map = pool.imap_unordered(
            search_worker,
            ((d, chromosome, start_pos, end_pos, ref_query, alt_query, ref_op, alt_op, condition_dict,
              internal_data)
             for d in datasets)
        )

        if internal_data:
            dataset_results = {d: e for d, e in pool_map if e is not None}
        else:
            dataset_results = [d for d in pool_map if d is not None]

    except ValueError as e:
        # TODO
        print(str(e))

    return dataset_results


@application.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    return jsonify({"results": search(request.json["dataTypeID"], request.json["conditions"], False)})


@application.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly

    return jsonify({"results": search(request.json["dataTypeID"], request.json["conditions"], True)})


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

    return jsonify({
        "id": "ca.distributedgenomics.chord_variant_service",  # TODO: Should be globally unique?
        "name": "CHORD Variant Service",                       # TODO: Should be globally unique?
        "type": "ca.distributedgenomics:chord_variant_service:{}".format(chord_variant_service.__version__),  # TODO
        "description": "Variant service for a CHORD application.",
        "organization": {
            "name": "GenAP",
            "url": "https://genap.ca/"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__
    })
