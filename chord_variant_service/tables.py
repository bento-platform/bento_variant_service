from abc import ABC, abstractmethod
import datetime
import os
import re
import shutil
import tabix
import uuid

from chord_lib.auth.flask_decorators import flask_permissions
from flask import Blueprint, current_app, json, jsonify, request
from itertools import chain
from jsonschema import validate, ValidationError
from pysam import VariantFile
from typing import Dict, Generator, List, Optional, Tuple

from .variants import VARIANT_SCHEMA, VARIANT_TABLE_METADATA_SCHEMA, SampleVariant


__all__ = [
    "DATA_PATH",
    "TABLE_NAME_FILE",
    "ID_RETRIES",
    "MIME_TYPE",

    "make_beacon_dataset_id",

    "BeaconDataset",

    "VariantTable",
    "MemoryVariantTable",
    "VCFVariantTable",

    "IDGenerationFailure",
    "TableManager",
    "MemoryTableManager",
    "VCFTableManager",

    "bp_tables",
]


MAX_SIGNED_INT_32 = 2 ** 31 - 1


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"

DATA_PATH = os.environ.get("DATA", "data/")
TABLE_NAME_FILE = ".chord_table_name"
TABLE_METADATA_FILE = ".chord_table_metadata"
ID_RETRIES = 100
MIME_TYPE = "application/json"

VCF_GENOTYPE = "GT"

REGEX_GENOTYPE_SPLIT = re.compile(r"[|/]")


def make_beacon_dataset_id(tp: Tuple[str, str]) -> str:
    return f"{tp[0]}:{tp[1]}"


class BeaconDataset:
    def __init__(self, table_id: str, table_name: str, table_metadata: dict, assembly_id: str, files=()):
        self.table_id = table_id
        self.table_name = table_name
        self.table_metadata = table_metadata
        self.assembly_id = assembly_id
        self.files = files

    @property
    def beacon_id_tuple(self):
        return self.table_id, self.assembly_id

    @property
    def beacon_id(self):
        return make_beacon_dataset_id(self.beacon_id_tuple)

    @property
    def beacon_name(self):
        return f"{self.table_name} ({self.assembly_id})"

    def as_beacon_dataset_response(self):
        return {
            "id": self.beacon_id,
            "name": self.beacon_name,
            "assemblyId": self.assembly_id,

            # Use utcnow() for old ones
            "createDateTime": self.table_metadata.get("created", datetime.datetime.utcnow().isoformat() + "Z"),
            "updateDateTime": self.table_metadata.get("updated", datetime.datetime.utcnow().isoformat() + "Z")
        }


class VariantTable(ABC):  # pragma: no cover
    def __init__(self, table_id, name, metadata, assembly_ids=()):
        self.table_id = table_id
        self.name = name
        self.metadata = metadata
        self.assembly_ids = set(assembly_ids)

    def as_table_response(self):
        # Don't leak sample IDs to the outside world
        return {
            "id": self.table_id,
            "name": self.name,
            "metadata": self.metadata,
            "assembly_ids": list(self.assembly_ids),
            "schema": VARIANT_SCHEMA
        }

    @property
    def beacon_datasets(self):
        return tuple(
            BeaconDataset(table_id=self.table_id, table_name=self.name, table_metadata=self.metadata, assembly_id=a)
            for a in sorted(self.assembly_ids)
        )

    @abstractmethod
    def variants(self, assembly_id: Optional[str], chromosome: str, start_min: Optional[int] = None,
                 start_max: Optional[int] = None) -> Generator[SampleVariant, None, None]:
        yield None


class MemoryVariantTable(VariantTable):
    def __init__(self, table_id, name, metadata, assembly_ids=()):
        super().__init__(table_id, name, metadata, assembly_ids)
        self.variant_store: List[SampleVariant] = []

    def variants(self, assembly_id: Optional[str], chromosome: str, start_min: Optional[int] = None,
                 start_max: Optional[int] = None) -> Generator[SampleVariant, None, None]:
        for v in self.variant_store:
            if v.chromosome != chromosome:
                continue

            if start_min is not None and v.start_pos < start_min:
                continue

            if start_max is not None and v.start_pos > start_max:
                continue

            if assembly_id is not None and v.assembly_id != assembly_id:
                continue

            yield v

    def add_variant(self, variant: SampleVariant):
        self.variant_store.append(variant)
        self.assembly_ids.add(variant.assembly_id)


class VCFVariantTable(VariantTable):  # pragma: no cover
    def __init__(self, table_id, name, metadata, assembly_ids=(), files=(), file_metadata: dict = None):
        super().__init__(table_id, name, metadata, assembly_ids)
        # TODO: Redo assembly IDs here
        self.assembly_ids = set(assembly_ids) if file_metadata is None else \
            set(fm["assembly_id"] for fm in file_metadata.values())
        self.file_metadata = file_metadata if file_metadata is not None else {}
        self.files = files

    @property
    def beacon_datasets(self):
        return tuple(
            BeaconDataset(
                table_id=self.table_id,
                table_name=self.name,
                table_metadata=self.metadata,
                assembly_id=a,
                files=tuple(f1 for f1, v in self.file_metadata.items() if v["assembly_id"] == a)
            ) for a in sorted(self.assembly_ids)
        )

    def variants(self, assembly_id: Optional[str], chromosome: str, start_min: Optional[int] = None,
                 start_max: Optional[int] = None) -> Generator[SampleVariant, None, None]:
        for vcf, vcf_metadata in filter(lambda fm: assembly_id is None or fm[1]["assembly_id"] == assembly_id,
                                        self.file_metadata.items()):
            try:
                # May throw ValueError from cast

                tbx = tabix.open(vcf)

                # TODO: Security of passing this? Verify values in non-Beacon searches
                # TODO: What if the VCF includes telomeres (off the end)?
                for row in tbx.query(chromosome,
                                     start_min if start_min is not None else 0,
                                     start_max if start_max is not None else MAX_SIGNED_INT_32):
                    if len(row) < 9:
                        # Badly formatted VCF  TODO: Catch on ingest
                        continue

                    for sample_id, row_data in zip(vcf_metadata["sample_ids"], row[9:]):
                        row_info = {k: v for k, v in zip(row[8].split(":"), row_data.split(":"))}

                        if VCF_GENOTYPE not in row_info:
                            continue

                        genotype = re.split(REGEX_GENOTYPE_SPLIT, row_info[VCF_GENOTYPE])

                        if len([g for g in genotype if g not in ("0", ".")]) == 0:
                            # Uninteresting, not present on sample
                            continue

                        yield SampleVariant(
                            assembly_id=vcf_metadata["assembly_id"],
                            chromosome=row[0],
                            start_pos=int(row[1]),
                            ref_bases=row[3],
                            alt_bases=row[4],
                            sample_id=sample_id
                        )

            except tabix.TabixError:
                # Region not found in Tabix file
                continue


class IDGenerationFailure(Exception):
    pass


class TableManager(ABC):  # pragma: no cover
    # TODO: Rename
    @abstractmethod
    def get_table(self, table_id: str) -> Optional[VariantTable]:
        pass

    @abstractmethod
    def get_tables(self) -> Dict[str, VCFVariantTable]:  # TODO: Rename get_tables
        return {}

    @abstractmethod
    def get_beacon_datasets(self) -> Dict[Tuple[str, str], BeaconDataset]:
        return {}

    @abstractmethod
    def update_tables(self):
        pass

    @abstractmethod
    def _generate_table_id(self) -> Optional[str]:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def create_table_and_update(self, name: str, metadata: dict) -> VariantTable:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def delete_table_and_update(self, table_id: str):
        pass


class MemoryTableManager(TableManager):
    def __init__(self):
        self.tables = {}
        self.id_to_generate = "fixed_id"

    def get_table(self, table_id: str) -> Optional[dict]:
        return self.tables.get(table_id, None)

    def get_tables(self) -> dict:
        return self.tables

    def get_beacon_datasets(self) -> Dict[Tuple[str, str], BeaconDataset]:
        return {bd.beacon_id_tuple: bd for bd in chain.from_iterable(d.beacon_datasets for d in self.tables.values())}

    def update_tables(self):  # pragma: no cover
        pass

    def _generate_table_id(self) -> Optional[str]:
        return None if self.id_to_generate in self.tables else self.id_to_generate

    def create_table_and_update(self, name: str, metadata: dict) -> MemoryVariantTable:
        table_id = self._generate_table_id()
        if table_id is None:
            raise IDGenerationFailure()

        new_table = MemoryVariantTable(table_id=table_id, name=name, metadata=metadata, assembly_ids=("GRCh37",))
        self.tables[table_id] = new_table

        return new_table

    def delete_table_and_update(self, table_id: str):
        del self.tables[table_id]


class VCFTableManager(TableManager):  # pragma: no cover
    def __init__(self, data_path: str):
        self.DATA_PATH = data_path
        self.tables = {}
        self.beacon_datasets = {}

    def get_table(self, table_id: str) -> Optional[dict]:
        return self.tables.get(table_id, None)

    def get_tables(self) -> dict:
        return self.tables

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

    @staticmethod
    def _get_sample_ids(vcf_path: str) -> Tuple[str, ...]:
        vcf = VariantFile(vcf_path)
        return tuple(vcf.header.samples)

    def update_tables(self):
        for d in os.listdir(self.DATA_PATH):
            table_dir = os.path.join(self.DATA_PATH, d)

            if not os.path.isdir(table_dir):
                continue

            name_path = os.path.join(table_dir, TABLE_NAME_FILE)
            metadata_path = os.path.join(table_dir, TABLE_METADATA_FILE)
            vcf_files = tuple(os.path.join(table_dir, file) for file in os.listdir(table_dir)
                              if file[-6:] == "vcf.gz")
            assembly_ids = tuple(self._get_assembly_id(vcf_path) for vcf_path in vcf_files)
            sample_ids = tuple(self._get_sample_ids(vcf_path) for vcf_path in vcf_files)

            ds = VCFVariantTable(
                table_id=d,
                name=open(name_path, "r").read().strip() if os.path.exists(name_path) else None,
                metadata=(json.load(open(metadata_path, "r")) if os.path.exists(metadata_path) else {}),
                files=vcf_files,
                file_metadata={f: {"assembly_id": a, "sample_ids": s}
                               for f, a, s in zip(vcf_files, assembly_ids, sample_ids)}
            )

            self.tables[d] = ds
            for bd in ds.beacon_datasets:
                self.beacon_datasets[bd.beacon_id_tuple] = bd

    def _generate_table_id(self) -> Optional[str]:
        new_id = str(uuid.uuid4())
        i = 0
        while new_id in self.tables and i < ID_RETRIES:
            new_id = str(uuid.uuid4())
            i += 1

        return None if i == ID_RETRIES else new_id

    def create_table_and_update(self, name: str, metadata: dict) -> VCFVariantTable:
        table_id = self._generate_table_id()
        if table_id is None:
            raise IDGenerationFailure()

        os.makedirs(os.path.join(self.DATA_PATH, table_id))

        with open(os.path.join(self.DATA_PATH, table_id, TABLE_NAME_FILE), "w") as nf:
            nf.write(name)

        with open(os.path.join(self.DATA_PATH, table_id, TABLE_METADATA_FILE), "w") as nf:
            now = datetime.datetime.utcnow().isoformat() + "Z"
            json.dump({
                "name": name,
                **metadata,
                "created": now,
                "updated": now
            }, nf)

        self.update_tables()

        return self.tables[table_id]  # TODO: Handle KeyError (i.e. something wrong somewhere...)

    def delete_table_and_update(self, table_id: str):
        shutil.rmtree(os.path.join(self.DATA_PATH, str(table_id)))
        self.update_tables()


bp_tables = Blueprint("tables", __name__)


def data_type_404(data_type_id):
    return jsonify({
        "code": 404,
        "message": "Data type not found",
        "timestamp": datetime.datetime.utcnow().isoformat("T") + "Z",
        "errors": [{"code": "not_found", "message": f"Data type with ID {data_type_id} was not found"}]
    }), 404


# Fetch or create tables
@bp_tables.route("/tables", methods=["GET", "POST"])
@flask_permissions({"POST": {"owner"}})
def table_list():
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
            new_table = current_app.config["TABLE_MANAGER"].create_table_and_update(name, metadata)
            return current_app.response_class(response=json.dumps(new_table.as_table_response()),
                                              mimetype=MIME_TYPE, status=201)

        except IDGenerationFailure:
            print("Couldn't generate new ID")
            return current_app.response_class(status=500)

    return jsonify([d.as_table_response() for d in current_app.config["TABLE_MANAGER"].get_tables().values()])


# TODO: Implement GET, POST (separate permissions)
@bp_tables.route("/tables/<string:table_id>", methods=["DELETE"])
@flask_permissions({"DELETE": {"owner"}})
def table_detail(table_id):
    if current_app.config["TABLE_MANAGER"].get_table(table_id) is None:
        # TODO: More standardized error
        # TODO: Refresh cache if needed?
        return current_app.response_class(status=404)

    current_app.config["TABLE_MANAGER"].delete_table_and_update(table_id)

    # TODO: More complete response?
    return current_app.response_class(status=204)


@bp_tables.route("/data-types", methods=["GET"])
def data_type_list():
    # Data types are basically stand-ins for schema blocks

    return jsonify([{
        "id": "variant",
        "schema": VARIANT_SCHEMA,
        "metadata_schema": VARIANT_TABLE_METADATA_SCHEMA
    }])


@bp_tables.route("/data-types/variant", methods=["GET"])
def data_type_detail():
    return jsonify({
        "id": "variant",
        "schema": VARIANT_SCHEMA,
        "metadata_schema": VARIANT_TABLE_METADATA_SCHEMA
    })


@bp_tables.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)


# TODO: Consistent snake or kebab
@bp_tables.route("/data-types/variant/metadata_schema", methods=["GET"])
def data_type_metadata_schema():
    return jsonify(VARIANT_TABLE_METADATA_SCHEMA)
