import chord_lib.ingestion
import chord_lib.search
import chord_variant_service
import datetime
import os
import requests
import shutil
# noinspection PyUnresolvedReferences
import tabix
import tqdm
import uuid

from flask import Flask, g, json, jsonify, request
from jsonschema import validate, ValidationError
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

ID_RETRIES = 100
MIME_TYPE = "application/json"

with application.open_resource("beacon_allele_request.schema.json") as bars:
    BEACON_ALLELE_REQUEST_SCHEMA = json.load(bars)

BEACON_IDR_ALL = "ALL"
BEACON_IDR_HIT = "HIT"
BEACON_IDR_MISS = "MISS"
BEACON_IDR_NONE = "NONE"

BEACON_API_VERSION = "v1.0"

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


# Ingest files into datasets
# Ingestion doesn't allow uploading files directly, it simply moves them from a different location on the filesystem.
@application.route("/ingest", methods=["POST"])
def ingest():
    try:
        assert "dataset_id" in request.form
        assert "workflow_name" in request.form
        assert "workflow_metadata" in request.form
        assert "workflow_outputs" in request.form
        assert "workflow_params" in request.form

        dataset_id = request.form["dataset_id"]  # TODO: WES needs to be able to forward this on...

        assert dataset_id in datasets
        dataset_id = str(uuid.UUID(dataset_id))  # Check that it's a valid UUID and normalize it to UUID's str format.

        workflow_name = request.form["workflow_name"].strip()
        workflow_metadata = json.loads(request.form["workflow_metadata"])
        workflow_outputs = json.loads(request.form["workflow_outputs"])
        workflow_params = json.loads(request.form["workflow_params"])

        output_params = chord_lib.ingestion.make_output_params(workflow_name, workflow_params,
                                                               workflow_metadata["inputs"])

        prefix = chord_lib.ingestion.find_common_prefix(os.path.join(DATA_PATH, dataset_id), workflow_metadata,
                                                        output_params)

        # Move files from the temporary file system location to their final resting place
        for file in workflow_metadata["outputs"]:
            if file not in workflow_outputs:  # TODO: Is this formatted with output_params or not?
                # Missing output
                print("Missing {} in {}".format(file, workflow_outputs))
                return application.response_class(status=400)

            # Full path to to-be-newly-ingested file
            file_path = os.path.join(DATA_PATH, dataset_id, chord_lib.ingestion.output_file_name(file, output_params))

            # Rename file if a duplicate name exists (ex. dup.vcf.gz becomes 1_dup.vcf.gz)
            if prefix is not None:
                file_path = os.path.join(DATA_PATH, dataset_id, chord_lib.ingestion.file_with_prefix(
                    chord_lib.ingestion.output_file_name(file, output_params), prefix))

            # Move the file from its temporary location on the filesystem to its location in the service's data folder.
            shutil.move(workflow_outputs[file], file_path)  # TODO: Is this formatted with output_params or not?

        update_datasets()

        return application.response_class(status=204)

    except (AssertionError, ValueError):  # assertion or JSON conversion failure
        # TODO: Better errors
        print("Assertion or value error")
        return application.response_class(status=400)


# Fetch or create datasets
@application.route("/datasets", methods=["GET", "POST"])
def dataset_list():
    dt = request.args.getlist("data-type")

    if "variant" not in dt or len(dt) != 1:
        return data_type_404(dt)

    # TODO: This POST stuff is not compliant with the GA4GH Search API
    if request.method == "POST":
        new_id = str(uuid.uuid4())

        i = 0
        while new_id in datasets and i < ID_RETRIES:
            new_id = str(uuid.uuid4())
            i += 1

        if i == ID_RETRIES:
            print("Couldn't generate new ID")
            return application.response_class(status=500)

        os.makedirs(os.path.join(DATA_PATH, new_id))

        update_datasets()

        return application.response_class(response=json.dumps({"id": new_id, "schema": VARIANT_SCHEMA}),
                                          mimetype=MIME_TYPE, status=201)

    return jsonify([{
        "id": d,
        "schema": VARIANT_SCHEMA
    } for d in datasets.keys()])


# TODO: Implement GET, DELETE
# @application.route("/datasets/<uuid:dataset_id>", methods=["POST"])


# TODO: To chord_lib? Maybe conditions_dict should be a class or something...
def parse_conditions(conditions):
    conditions_filtered = [c for c in conditions
                           if c["field"].split(".")[-1] in VARIANT_SCHEMA["properties"].keys() and
                           isinstance(c["negated"], bool) and c["operation"] in chord_lib.search.SEARCH_OPERATIONS]

    condition_dict = {c["field"].split(".")[-1]: c for c in conditions_filtered}

    return condition_dict


def search_worker_prime(d, chromosome, start_min, start_max, end_min, end_max, ref, alt, ref_op, alt_op, internal_data):
    found = False
    matches = []
    for vcf in (os.path.join(DATA_PATH, d, vf) for vf in datasets[d]):
        if found:
            break

        # TODO: Need to figure out if pytabix does stuff in a defined coordinate system...

        tbx = tabix.open(vcf)

        try:
            # TODO: Security of passing this? Verify values
            for row in tbx.query(chromosome, start_min, end_max):
                if not internal_data and found:
                    break

                if (start_max is not None and row[1] > start_max) or (end_min is not None and row[2] < end_min):
                    # TODO: Are start_max and end_min both inclusive for sure? Pretty sure but unclear
                    continue

                if ref is not None and alt is None:
                    match = ref_op(row[3].upper(), ref.upper())
                elif ref is None and alt is not None:
                    match = alt_op(row[4].upper(), alt.upper())
                elif ref is not None and alt is not None:
                    match = (ref_op(row[3].upper(), ref.upper()) and
                             alt_op(row[4].upper(), alt.upper()))
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


def generic_variant_search(chromosome, start_min, start_max=None, end_min=None, end_max=None, ref=None, alt=None,
                           ref_op=eq, alt_op=eq, internal_data=False):

    # TODO: Sane defaults
    # TODO: Figure out inclusion/exclusion with start_min/end_max

    dataset_results = {} if internal_data else []

    try:
        pool = get_pool()
        pool_map = pool.imap_unordered(
            search_worker,
            ((d, chromosome, start_min, start_max, end_min, end_max, ref, alt, ref_op, alt_op, internal_data)
             for d in datasets)
        )

        if internal_data:
            dataset_results = {d: e for d, e in pool_map if e is not None}
        else:
            dataset_results = [d for d in pool_map if d is not None]

    except ValueError as e:
        print(str(e))

    return dataset_results


def chord_search(dt, conditions, internal_data=False):
    null_result = {} if internal_data else []

    if dt != "variant":
        return null_result

    condition_dict = parse_conditions(conditions)

    if "chromosome" not in condition_dict or "start" not in condition_dict or "end" not in condition_dict:
        # TODO: Error
        # TODO: Not hardcoded?
        # TODO: More conditions
        return null_result

    dataset_results = {} if internal_data else []

    try:
        chromosome = condition_dict["chromosome"]["searchValue"]  # TODO: Check domain for chromosome
        start_pos = int(condition_dict["start"]["searchValue"])
        end_pos = int(condition_dict["end"]["searchValue"])
        ref_cond = condition_dict.get("ref", None)
        alt_cond = condition_dict.get("alt", None)
        ref_op = ne if ref_cond is not None and ref_cond["negated"] else eq
        alt_op = ne if alt_cond is not None and alt_cond["negated"] else eq

        return generic_variant_search(chromosome=chromosome, start_min=start_pos, end_max=end_pos,
                                      ref=ref_cond["searchValue"] if ref_cond is not None else None,
                                      alt=alt_cond["searchValue"] if alt_cond is not None else None,
                                      ref_op=ref_op, alt_op=alt_op, internal_data=internal_data)

    except ValueError as e:
        # TODO
        print(str(e))

    return dataset_results


@application.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    return jsonify({"results": chord_search(request.json["dataTypeID"], request.json["conditions"], False)})


@application.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly

    return jsonify({"results": chord_search(request.json["dataTypeID"], request.json["conditions"], True)})


@application.route("/beacon", methods=["GET"])
def beacon_get():
    return jsonify({
        "id": "TODO",  # TODO
        "name": "TODO",  # TODO
        "apiVersion": BEACON_API_VERSION,
        "organization": "GenAP",
        "description": "TODO",  # TODO, optional
        "version": chord_variant_service.__version__,
        "datasets": "TODO"  # TODO
    })


@application.route("/beacon/query", methods=["GET", "POST"])
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
        return application.response_class(status=400)  # TODO: Beacon error response

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
        "beaconId": "TODO",  # TODO
        "apiVersion": BEACON_API_VERSION,
        "exists": len(dataset_matches) > 0,
        "alleleRequest": query,
        "datasetAlleleResponses": beacon_datasets
    })


with application.open_resource("workflows/chord_workflows.json") as wf:
    # TODO: Schema
    WORKFLOWS = json.loads(wf.read())


@application.route("/workflows", methods=["GET"])
def workflow_list():
    return jsonify(WORKFLOWS)


@application.route("/workflows/<string:workflow_name>", methods=["GET"])
def workflow_detail(workflow_name):
    # TODO: Better errors
    if workflow_name not in WORKFLOWS["ingestion"] and workflow_name not in WORKFLOWS["analysis"]:
        return application.response_class(status=404)

    return jsonify(WORKFLOWS["ingestion"][workflow_name] if workflow_name in WORKFLOWS["ingestion"]
                   else WORKFLOWS["analysis"][workflow_name])


@application.route("/workflows/<string:workflow_name>.wdl", methods=["GET"])
def workflow_wdl(workflow_name):
    # TODO: Better errors
    if workflow_name not in WORKFLOWS["ingestion"] and workflow_name not in WORKFLOWS["analysis"]:
        return application.response_class(status=404)

    workflow = (WORKFLOWS["ingestion"][workflow_name] if workflow_name in WORKFLOWS["ingestion"]
                else WORKFLOWS["analysis"][workflow_name])

    # TODO: Clean workflow name
    with application.open_resource("workflows/{}".format(workflow["file"])) as wfh:
        return application.response_class(response=wfh.read(), mimetype="text/plain", status=200)


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
