import chord_variant_service
import datetime
import os
import requests
import tabix
import tqdm
import uuid

from flask import Flask, json, jsonify, request

VARIANT_SCHEMA = {
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
    "type": "object",
    "required": ["chromosome", "start", "end", "ref", "alt"],
    "properties": {
        "chromosome": {
            "type": "string"
        },
        "start": {
            "type": "integer"
        },
        "end": {
            "type": "integer"
        },
        "ref": {
            "type": "string"
        },
        "alt": {
            "type": "string"
        }
    }
}


application = Flask(__name__)

data_path = os.environ.get("DATA", "data/")
datasets = {}


def update_datasets():
    global datasets
    datasets = {d: [file for file in os.listdir(os.path.join(data_path, d)) if file[-6:] == "vcf.gz"]
                for d in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, d))}


update_datasets()
print(datasets)
if len(datasets.keys()) == 0:
    # Add some fake data
    new_id_1 = str(uuid.uuid4())
    new_id_2 = str(uuid.uuid4())

    os.makedirs(os.path.join(data_path, new_id_1))
    os.makedirs(os.path.join(data_path, new_id_2))

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "CEU.trio.2010_07.indel.sites.vcf.gz", stream=True) as r:
        with open(os.path.join(data_path, new_id_1, "ceu.vcf.gz"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "CEU.trio.2010_07.indel.sites.vcf.gz.tbi",
                      stream=True) as r:
        with open(os.path.join(data_path, new_id_1, "ceu.vcf.gz.tbi"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "YRI.trio.2010_07.indel.sites.vcf.gz",
                      stream=True) as r:
        with open(os.path.join(data_path, new_id_2, "yri.vcf.gz"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "YRI.trio.2010_07.indel.sites.vcf.gz.tbi",
                      stream=True) as r:
        with open(os.path.join(data_path, new_id_2, "yri.vcf.gz.tbi"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    update_datasets()
    print(datasets)


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


@application.route("/datasets/<uuid:dataset_id>", methods=["GET"])
def dataset_detail():
    # Not implementing this
    pass


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

        for d in datasets:
            vcfs = [os.path.join(data_path, d, vf) for vf in datasets[d]]
            found = False
            for vcf in vcfs:
                if found:
                    break

                tbx = tabix.open(vcf)

                try:
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
                    found = True

            if found:
                dataset_results.append({"id": d, "data_type": "variant"})

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
        "description": "Example service for a CHORD application.",
        "organization": "GenAP",
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__,
        "extension": {}
    })
