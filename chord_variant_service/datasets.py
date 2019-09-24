import datetime
import os
import requests
import shutil
import tqdm
import uuid

from flask import Blueprint, current_app, json, jsonify, request

from .variants import VARIANT_SCHEMA


__all__ = [
    "DATA_PATH",
    "DATASET_NAME_FILE",
    "ID_RETRIES",
    "MIME_TYPE",

    "update_datasets",
    "download_example_datasets",
    "bp_datasets",
]


DATA_PATH = os.environ.get("DATA", "data/")
DATASET_NAME_FILE = ".chord_dataset_name"
ID_RETRIES = 100
MIME_TYPE = "application/json"

datasets = {}


def update_datasets():
    global datasets
    datasets = {d: {
        "name": (open(os.path.join(DATA_PATH, d, DATASET_NAME_FILE), "r").read().strip()
                 if os.path.exists(os.path.join(DATA_PATH, d, DATASET_NAME_FILE)) else None),
        "files": [file for file in os.listdir(os.path.join(DATA_PATH, d)) if file[-6:] == "vcf.gz"]
    } for d in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, d))}


def download_example_datasets():
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

        with open(os.path.join(DATA_PATH, new_id_1, DATASET_NAME_FILE), "w") as nf:
            nf.write("CEU trio")

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

        with open(os.path.join(DATA_PATH, new_id_2, DATASET_NAME_FILE), "w") as nf:
            nf.write("YRI trio")

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


update_datasets()

if len(datasets.keys()) == 0 and os.environ.get("DEMO_DATA", "") != "":
    download_example_datasets()


bp_datasets = Blueprint("datasets", __name__)


def data_type_404(data_type_id):
    return json.dumps({
        "code": 404,
        "message": "Data type not found",
        "timestamp": datetime.datetime.utcnow().isoformat("T") + "Z",
        "errors": [{"code": "not_found", "message": f"Data type with ID {data_type_id} was not found"}]
    })


# Fetch or create datasets
@bp_datasets.route("/datasets", methods=["GET", "POST"])
def dataset_list():
    dt = request.args.getlist("data-type")

    if "variant" not in dt or len(dt) != 1:
        return data_type_404(dt)

    # TODO: This POST stuff is not compliant with the GA4GH Search API
    if request.method == "POST":
        new_id = str(uuid.uuid4())

        name = request.form["name"].strip()

        i = 0
        while new_id in datasets and i < ID_RETRIES:
            new_id = str(uuid.uuid4())
            i += 1

        if i == ID_RETRIES:
            print("Couldn't generate new ID")
            return current_app.response_class(status=500)

        os.makedirs(os.path.join(DATA_PATH, new_id))

        with open(os.path.join(DATA_PATH, new_id, DATASET_NAME_FILE), "w") as nf:
            nf.write(name)

        update_datasets()

        return current_app.response_class(response=json.dumps({
            "id": new_id,
            "name": datasets[new_id]["name"],
            "schema": VARIANT_SCHEMA
        }), mimetype=MIME_TYPE, status=201)

    return jsonify([{
        "id": d,
        "name": datasets[d]["name"],
        "schema": VARIANT_SCHEMA
    } for d in datasets.keys()])


# TODO: Implement GET, POST
@bp_datasets.route("/datasets/<uuid:dataset_id>", methods=["DELETE"])
def dataset_detail(dataset_id):
    if str(dataset_id) not in datasets:
        # TODO: More standardized error
        return current_app.response_class(status=404)

    shutil.rmtree(os.path.join(DATA_PATH, str(dataset_id)))
    update_datasets()

    # TODO: More complete response?
    return current_app.response_class(status=204)


@bp_datasets.route("/data-types", methods=["GET"])
def data_type_list():
    # Data types are basically stand-ins for schema blocks

    return jsonify([{"id": "variant", "schema": VARIANT_SCHEMA}])


@bp_datasets.route("/data-types/variant", methods=["GET"])
def data_type_detail():
    return jsonify({
        "id": "variant",
        "schema": VARIANT_SCHEMA
    })


@bp_datasets.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)
