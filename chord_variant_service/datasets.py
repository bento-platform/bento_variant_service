from abc import ABC, abstractmethod
import datetime
import os
import requests
import shutil
import tqdm
import uuid

from flask import Blueprint, current_app, json, jsonify, request
from jsonschema import validate, ValidationError
from pysam import VariantFile
from typing import Optional

from .variants import VARIANT_SCHEMA, VARIANT_TABLE_METADATA_SCHEMA


__all__ = [
    "DATA_PATH",
    "DATASET_NAME_FILE",
    "ID_RETRIES",
    "MIME_TYPE",

    "TableManager",
    "MemoryTableManager",
    "VCFTableManager",

    "download_example_datasets",
    "bp_datasets",
]


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"

DATA_PATH = os.environ.get("DATA", "data/")
DATASET_NAME_FILE = ".chord_dataset_name"
DATASET_METADATA_FILE = ".chord_dataset_metadata"
ID_RETRIES = 100
MIME_TYPE = "application/json"


class IDGenerationFailure(Exception):
    pass


class TableManager(ABC):  # pragma: no cover
    # TODO: Rename
    @abstractmethod
    def get_dataset(self, dataset_id: str) -> Optional[dict]:
        pass

    @abstractmethod
    def get_datasets(self) -> dict:  # TODO: Rename get_tables
        return {}

    @abstractmethod
    def get_beacon_datasets(self) -> dict:
        return {}

    @abstractmethod
    def update_datasets(self):
        pass

    @abstractmethod
    def _generate_dataset_id(self) -> Optional[str]:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def create_dataset_and_update(self, name: str, metadata: dict) -> dict:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def delete_dataset_and_update(self, dataset_id: str):
        pass


class MemoryTableManager(TableManager):
    def __init__(self):
        self.datasets = {}
        self.beacon_datasets = {}
        self.id_to_generate = "fixed_id"

    def get_dataset(self, dataset_id: str) -> Optional[dict]:
        return self.datasets.get(dataset_id, None)

    def get_datasets(self) -> dict:
        return self.datasets

    def get_beacon_datasets(self) -> dict:
        return self.beacon_datasets

    def update_datasets(self):
        pass

    def _generate_dataset_id(self) -> Optional[str]:
        return None if self.id_to_generate in self.datasets else self.id_to_generate

    def create_dataset_and_update(self, name: str, metadata: dict) -> dict:
        dataset_id = self._generate_dataset_id()
        if dataset_id is None:
            raise IDGenerationFailure()

        new_dataset = {
            "id": dataset_id,
            "name": name,
            "metadata": metadata,
            "assembly_ids": []
        }

        self.datasets[dataset_id] = new_dataset

        return new_dataset

    def delete_dataset_and_update(self, dataset_id: str):
        del self.datasets[dataset_id]


class VCFTableManager(TableManager):
    def __init__(self, data_path: str):
        self.DATA_PATH = data_path
        self.datasets = {}
        self.beacon_datasets = {}

    def get_dataset(self, dataset_id: str) -> Optional[dict]:
        return self.datasets.get(dataset_id, None)

    def get_datasets(self) -> dict:
        return self.datasets

    def get_beacon_datasets(self) -> dict:
        return self.beacon_datasets

    @staticmethod
    def _get_assembly_id(vcf_path: str) -> str:
        vcf = VariantFile(vcf_path)
        assembly_id = "Other"
        for h in vcf.header.records:
            if h.key == ASSEMBLY_ID_VCF_HEADER:
                assembly_id = h.value
        return assembly_id

    def update_datasets(self):
        for d in os.listdir(self.DATA_PATH):
            table_dir = os.path.join(self.DATA_PATH, d)

            if not os.path.isdir(table_dir):
                continue

            name_path = os.path.join(table_dir, DATASET_NAME_FILE)
            metadata_path = os.path.join(table_dir, DATASET_METADATA_FILE)
            vcf_files = tuple(os.path.join(table_dir, file) for file in os.listdir(table_dir)
                              if file[-6:] == "vcf.gz")
            assembly_ids = tuple(self._get_assembly_id(vcf_path) for vcf_path in vcf_files)

            self.datasets[d] = {
                "id": d,
                "name": open(name_path, "r").read().strip() if os.path.exists(name_path) else None,
                "files": vcf_files,
                "metadata": (json.load(open(metadata_path, "r")) if os.path.exists(metadata_path) else {}),
                "assembly_ids": assembly_ids
            }

            for a in assembly_ids:
                self.beacon_datasets[(d, a)] = {
                    "name": open(name_path, "r").read().strip() if os.path.exists(name_path) else None,
                    "files": tuple(f1 for f1, a1 in zip(vcf_files, assembly_ids) if a1 == a),
                    "metadata": (json.load(open(metadata_path, "r")) if os.path.exists(metadata_path) else {}),
                    "assembly_id": a
                }

    def _generate_dataset_id(self) -> Optional[str]:
        new_id = str(uuid.uuid4())
        i = 0
        while new_id in self.datasets and i < ID_RETRIES:
            new_id = str(uuid.uuid4())
            i += 1

        return None if i == ID_RETRIES else new_id

    def create_dataset_and_update(self, name: str, metadata: dict):
        dataset_id = self._generate_dataset_id()
        if dataset_id is None:
            raise IDGenerationFailure()

        os.makedirs(os.path.join(self.DATA_PATH, dataset_id))

        with open(os.path.join(self.DATA_PATH, dataset_id, DATASET_NAME_FILE), "w") as nf:
            nf.write(name)

        with open(os.path.join(self.DATA_PATH, dataset_id, DATASET_METADATA_FILE), "w") as nf:
            now = datetime.datetime.utcnow().isoformat() + "Z"
            json.dump({
                "name": name,
                **metadata,
                "created": now,
                "updated": now
            }, nf)

        self.update_datasets()

        return self.datasets[dataset_id]  # TODO: Handle KeyError (i.e. something wrong somewhere...)

    def delete_dataset_and_update(self, dataset_id: str):
        shutil.rmtree(os.path.join(self.DATA_PATH, str(dataset_id)))
        self.update_datasets()


def download_example_datasets(table_manager):  # pragma: no cover
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

        with open(os.path.join(DATA_PATH, new_id_1, DATASET_METADATA_FILE), "w") as nf:
            now = datetime.datetime.utcnow().isoformat() + "Z"
            json.dump({
                "name": "CEU trio",
                "created": now,
                "updated": now
            }, nf)

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

        with open(os.path.join(DATA_PATH, new_id_1, DATASET_METADATA_FILE), "w") as nf:
            now = datetime.datetime.utcnow().isoformat() + "Z"
            nf.write(json.dump({
                "name": "YRI trio",
                "created": now,
                "updated": now
            }, nf))

    with requests.get("http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/indels/"
                      "YRI.trio.2010_07.indel.sites.vcf.gz.tbi",
                      stream=True) as r:
        with open(os.path.join(DATA_PATH, new_id_2, "yri.vcf.gz.tbi"), "wb") as f:
            for data in tqdm.tqdm(r.iter_content(1024), total=int(r.headers.get("content-length", 0)) // 1024):
                if not data:
                    break

                f.write(data)
                f.flush()

    table_manager.update_datasets()


bp_datasets = Blueprint("datasets", __name__)


def data_type_404(data_type_id):
    return jsonify({
        "code": 404,
        "message": "Data type not found",
        "timestamp": datetime.datetime.utcnow().isoformat("T") + "Z",
        "errors": [{"code": "not_found", "message": f"Data type with ID {data_type_id} was not found"}]
    }), 404


# Fetch or create datasets
@bp_datasets.route("/datasets", methods=["GET", "POST"])
def dataset_list():
    dt = request.args.getlist("data-type")

    if "variant" not in dt or len(dt) != 1:
        return data_type_404(dt)

    # TODO: This POST stuff is not compliant with the GA4GH Search API
    if request.method == "POST":
        if request.json is None:
            # TODO: Better error
            return current_app.response_class(status=400)

        name = request.json["name"].strip()
        metadata = request.json["metadata"]

        try:
            validate(metadata, VARIANT_TABLE_METADATA_SCHEMA)
        except ValidationError:
            return current_app.response_class(status=400)  # TODO: Error message

        try:
            new_table = current_app.config["TABLE_MANAGER"].create_dataset_and_update(name, metadata)

            return current_app.response_class(response=json.dumps({
                "id": new_table["id"],
                "name": new_table["name"],
                "metadata": new_table["metadata"],
                "schema": VARIANT_SCHEMA
            }), mimetype=MIME_TYPE, status=201)

        except IDGenerationFailure:
            print("Couldn't generate new ID")
            return current_app.response_class(status=500)

    return jsonify([{
        "id": d["id"],
        "name": d["name"],
        "metadata": d["metadata"],
        "schema": VARIANT_SCHEMA
    } for d in current_app.config["TABLE_MANAGER"].get_datasets().values()])


# TODO: Implement GET, POST
@bp_datasets.route("/datasets/<uuid:dataset_id>", methods=["DELETE"])
def dataset_detail(dataset_id):
    if current_app.config["TABLE_MANAGER"].get_dataset(dataset_id) is None:
        # TODO: More standardized error
        # TODO: Refresh cache if needed?
        return current_app.response_class(status=404)

    current_app.config["TABLE_MANAGER"].delete_dataset_and_update(str(dataset_id))

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
        "schema": VARIANT_SCHEMA,
        "metadata_schema": VARIANT_TABLE_METADATA_SCHEMA
    })


@bp_datasets.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)


@bp_datasets.route("/data-types/variant/metadata_schema", methods=["GET"])
def data_type_metadata_schema():
    return jsonify(VARIANT_TABLE_METADATA_SCHEMA)
