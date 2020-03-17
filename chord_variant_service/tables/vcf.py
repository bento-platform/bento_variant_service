import abc
import datetime
import os
import pysam
import re
import shutil
import uuid

from collections import namedtuple
from flask import json
from typing import Dict, Generator, Optional, Set, Tuple

from chord_variant_service.beacon.datasets import BeaconDatasetIDTuple, BeaconDataset
from chord_variant_service.pool import WORKERS
from chord_variant_service.tables.base import VariantTable, TableManager
from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.tables.vcf_file import VCFFile
from chord_variant_service.variants.models import Variant, Call


__all__ = [
    "VCFVariantTable",
    "VCFTableManager",
]


MAX_SIGNED_INT_32 = 2 ** 31 - 1

REGEX_GENOTYPE_SPLIT = re.compile(r"[|/]")
VCF_GENOTYPE = "GT"


TABLE_NAME_FILE = ".chord_table_name"
TABLE_METADATA_FILE = ".chord_table_metadata"
ID_RETRIES = 100


class VCFVariantTable(VariantTable):  # pragma: no cover
    def __init__(
        self,
        table_id,
        name,
        metadata,
        assembly_ids=(),
        files: Tuple[VCFFile] = (),
    ):
        super().__init__(table_id, name, metadata, assembly_ids)
        self.assembly_ids: Set[str] = set(vf.assembly_id for vf in files)
        self.files = files

    @property
    def beacon_datasets(self):
        return tuple(
            BeaconDataset(
                table_id=self.table_id,
                table_name=self.name,
                table_metadata=self.metadata,
                assembly_id=a,
                files=tuple(vf for vf in self.files if vf.assembly_id == a)
            ) for a in sorted(self.assembly_ids)
        )

    @staticmethod
    def _variant_calls(variant: Variant, sample_ids: tuple, row: tuple, only_interesting: bool = False):
        for sample_id, row_data in zip(sample_ids, row[9:]):
            row_info = {k: v for k, v in zip(row[8].split(":"), row_data.split(":"))}

            if VCF_GENOTYPE not in row_info:
                # Only include samples which have genotypes
                continue

            genotype = tuple(None if g == "." else int(g) for g in
                             re.split(REGEX_GENOTYPE_SPLIT, row_info[VCF_GENOTYPE]))

            call = Call(variant=variant, genotype=genotype, sample_id=sample_id)

            if only_interesting and not call.is_interesting:
                # Uninteresting, not present on sample
                continue

            yield call

    @property
    def n_of_variants(self) -> int:
        return sum(vf.n_of_variants for vf in self.files)

    @property
    def n_of_samples(self) -> int:
        sample_set = set()
        for vcf in self.files:
            sample_set.update(vcf.sample_ids)
        return len(sample_set)

    def variants(
        self,
        assembly_id: Optional[str] = None,
        chromosome: Optional[str] = None,
        start_min: Optional[int] = None,
        start_max: Optional[int] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        only_interesting: bool = False,
    ) -> Generator[Variant, None, None]:
        # If offset isn't specified, set it to 0 (the very start)
        offset: int = 0 if offset is None else offset

        variants_passed = 0
        variants_seen = 0

        # TODO: Optimize offset/count
        #  e.g. by skipping entire VCFs if we know their row counts a-priori

        for vcf in filter(lambda vf: assembly_id is None or vf.assembly_id == assembly_id, self.files):
            if (
                chromosome is None and  # No filters (otherwise we wouldn't be able to assume we're skipping the VCF)
                start_min is None and  # "
                start_max is None and  # "
                not only_interesting and  # "
                vcf.n_of_variants < offset - variants_seen
            ):
                # If the entire file has less variants than the remaining offset, skip it. This saves time crawling
                # through an entire VCF if we cannot use any of them.
                variants_seen += vcf.n_of_variants
                continue

            try:
                # Parse as a Tabix file instead of a Variant file for performance reasons, and to get rows as tuples.
                f = pysam.TabixFile(vcf.path, index=vcf.index_path, parser=pysam.asTuple(), threads=WORKERS)

                # TODO: Security of passing this? Verify values in non-Beacon searches
                # TODO: What if the VCF includes telomeres (off the end)?]

                # TODO: pysam uses 0-based indexing, double-check

                query = ()
                if chromosome is not None:
                    query = (
                        chromosome,
                        start_min - 1 if start_min is not None else 0,
                        start_max - 1 if start_max is not None else MAX_SIGNED_INT_32,
                    )

                for row in f.fetch(*query):
                    variants_passed += 1

                    if len(row) < 9:
                        # Badly formatted VCF  TODO: Catch on ingest
                        continue

                    if variants_passed < offset:
                        continue

                    if count is not None and variants_seen >= count:
                        return

                    if chromosome is None:
                        # Didn't index in, so check start_min / start_max by hand
                        if start_min is not None and int(row[1]) < start_min:
                            continue
                        elif start_max is not None and int(row[1]) >= start_max:
                            continue

                    variant = Variant(
                        assembly_id=vcf.assembly_id,
                        chromosome=row[0],
                        start_pos=int(row[1]),
                        ref_bases=row[3],
                        alt_bases=tuple(row[4].split(",")),
                    )

                    variant.calls = tuple(VCFVariantTable._variant_calls(variant, vcf.sample_ids, row,
                                                                         only_interesting=only_interesting))

                    if only_interesting and len(variant.calls) == 0:
                        # Uninteresting; no calls of note on the variant
                        continue

                    yield variant

                    variants_seen += 1

            except ValueError:
                # Region not found in Tabix file
                continue


# TODO: Data class
VCFTableFolder = namedtuple("VCFTableFolder", ("id", "dir", "name", "metadata"))


class BaseVCFTableManager(abc.ABC, TableManager):  # pragma: no cover
    def __init__(self, data_path: str):
        self._DATA_PATH = data_path
        self._tables: Dict[str, VCFVariantTable] = {}
        self._beacon_datasets: Dict[BeaconDatasetIDTuple, BeaconDataset] = {}

    @staticmethod
    def get_vcf_file_record(vcf_path: str) -> VCFFile:
        return VCFFile(vcf_path)

    def get_table(self, table_id: str) -> Optional[dict]:
        return self._tables.get(table_id, None)

    def get_tables(self) -> dict:
        return self._tables

    def get_beacon_datasets(self) -> Dict[BeaconDatasetIDTuple, BeaconDataset]:
        return self._beacon_datasets

    def _generate_table_id(self) -> Optional[str]:
        new_id = str(uuid.uuid4())
        i = 0
        while new_id in self._tables and i < ID_RETRIES:
            new_id = str(uuid.uuid4())
            i += 1

        return None if i == ID_RETRIES else new_id

    @property
    def table_folders(self) -> Generator[VCFTableFolder]:
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
        if table_id is None:
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
        self.update_tables()

    @abc.abstractmethod
    def update_tables(self):
        pass


class VCFTableManager(BaseVCFTableManager):  # pragma: no cover
    def update_tables(self):
        for t in self.table_folders:
            vcf_files = tuple(BaseVCFTableManager.get_vcf_file_record(os.path.join(t.dir, file))
                              for file in os.listdir(t.dir))

            table = VCFVariantTable(
                table_id=t.id,
                name=t.name,
                metadata=t.metadata,
                files=vcf_files,
            )

            self._tables[t] = table
            for bd in table.beacon_datasets:
                self._beacon_datasets[bd.beacon_id_tuple] = bd
