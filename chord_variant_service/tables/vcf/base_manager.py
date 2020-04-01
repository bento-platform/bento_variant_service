import abc
import datetime
import os
import shutil
import uuid

from collections import namedtuple
from flask import json
from typing import Dict, Optional, Sequence, Tuple

from chord_variant_service.beacon.datasets import BeaconDatasetIDTuple, BeaconDataset
from chord_variant_service.tables.base import TableManager
from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.tables.vcf.file import VCFFile
from chord_variant_service.tables.vcf.table import VCFVariantTable


__all__ = [
    "VCFTableFolder",
    "BaseVCFTableManager",
]


TABLE_NAME_FILE = ".chord_table_name"
TABLE_METADATA_FILE = ".chord_table_metadata"
ID_RETRIES = 100


# TODO: Data class
VCFTableFolder = namedtuple("VCFTableFolder", ("id", "dir", "name", "metadata"))


TableDict = Dict[str, VCFVariantTable]


class BaseVCFTableManager(TableManager, abc.ABC):
    def __init__(self, data_path: str):
        self._DATA_PATH = data_path
        self._tables: TableDict = {}
        self._beacon_datasets: Dict[BeaconDatasetIDTuple, BeaconDataset] = {}

    @property
    def data_path(self):
        return self._DATA_PATH

    @staticmethod
    def get_vcf_file_record(vcf_path: str, index_path: Optional[str] = None) -> VCFFile:
        return VCFFile(vcf_path, index_path)

    def get_table(self, table_id: str) -> Optional[VCFVariantTable]:
        return self._tables.get(table_id, None)

    @property
    def tables(self) -> TableDict:
        return self._tables

    @property
    def beacon_datasets(self) -> Dict[BeaconDatasetIDTuple, BeaconDataset]:
        return self._beacon_datasets

    def _generate_table_id(self) -> Optional[str]:
        new_id = str(uuid.uuid4())
        i = 0
        while new_id in self._tables and i < ID_RETRIES:  # pragma: no cover
            new_id = str(uuid.uuid4())
            i += 1

        return None if i == ID_RETRIES else new_id

    @property
    def table_folders(self) -> Sequence[VCFTableFolder]:
        for t in os.listdir(self._DATA_PATH):
            table_dir = os.path.join(self._DATA_PATH, t)

            if not os.path.isdir(table_dir):
                continue

            name_path = os.path.join(table_dir, TABLE_NAME_FILE)
            metadata_path = os.path.join(table_dir, TABLE_METADATA_FILE)

            name = None
            if os.path.exists(name_path):
                with open(name_path) as nf:
                    name = nf.read().strip()

            metadata = {}
            if os.path.exists(metadata_path):
                with open(metadata_path) as mf:
                    metadata = json.load(mf)

            yield VCFTableFolder(id=t, dir=table_dir, name=name, metadata=metadata)

    def create_table_and_update(self, name: str, metadata: dict) -> VCFVariantTable:
        table_id = self._generate_table_id()
        if table_id is None:  # pragma: no cover
            raise IDGenerationFailure()

        table_path = os.path.join(self._DATA_PATH, table_id)

        os.makedirs(table_path)

        with open(os.path.join(table_path, TABLE_NAME_FILE), "w") as nf:
            nf.write(name)

        with open(os.path.join(table_path, TABLE_METADATA_FILE), "w") as nf:
            now = datetime.datetime.utcnow().isoformat() + "Z"
            json.dump({
                "name": name,
                **metadata,
                "created": now,
                "updated": now
            }, nf)

        self.update_tables()

        return self._tables[table_id]  # TODO: Handle KeyError (i.e. something wrong somewhere...)

    def delete_table_and_update(self, table_id: str):
        shutil.rmtree(os.path.join(self._DATA_PATH, str(table_id)))
        self._tables[table_id].delete()
        self.update_tables()

    @abc.abstractmethod
    def _get_table_vcf_files(self, table_folder: VCFTableFolder) -> Tuple[VCFFile, ...]:  # pragma: no cover
        pass

    def update_tables(self):
        # Loop through table folders to do the following:
        #  - Add new tables if entries on the file system have been added
        #  - Update existing tables
        #  - Remove tables if entries on the file system have been removed

        table_folders = {t.id: t for t in self.table_folders}

        for t in self.table_folders:
            files = self._get_table_vcf_files(t)
            if t.id in self._tables:
                # Table exists already, so update it
                self._tables[t.id].update_with_files(t.name, t.metadata, files)
            else:
                self._tables[t.id] = VCFVariantTable(table_id=t.id, name=t.name, metadata=t.metadata, files=files)

            for bd in self._tables[t.id].beacon_datasets:
                self._beacon_datasets[bd.beacon_id_tuple] = bd

        # Remove any existing tables that shouldn't be there
        self._tables = {k: v for k, v in self._tables.items() if k in table_folders}
