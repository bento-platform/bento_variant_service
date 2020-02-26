import datetime
import os
import pysam
import re
import shutil
import subprocess
import uuid

from flask import json
from pysam import VariantFile
from typing import Generator, Optional, Tuple

from chord_variant_service.beacon.datasets import BeaconDataset
from chord_variant_service.pool import WORKERS
from chord_variant_service.tables.base import VariantTable, TableManager
from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.variants.models import Variant, Call


__all__ = [
    "VCFVariantTable",
    "VCFTableManager",
]


MAX_SIGNED_INT_32 = 2 ** 31 - 1

REGEX_GENOTYPE_SPLIT = re.compile(r"[|/]")
VCF_GENOTYPE = "GT"


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"

TABLE_NAME_FILE = ".chord_table_name"
TABLE_METADATA_FILE = ".chord_table_metadata"
ID_RETRIES = 100


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

    @staticmethod
    def _variant_calls(variant, sample_ids, row):
        for sample_id, row_data in zip(sample_ids, row[9:]):
            row_info = {k: v for k, v in zip(row[8].split(":"), row_data.split(":"))}

            if VCF_GENOTYPE not in row_info:
                # Only include samples which have genotypes
                continue

            genotype = tuple(None if g == "." else int(g) for g in
                             re.split(REGEX_GENOTYPE_SPLIT, row_info[VCF_GENOTYPE]))

            # if len([g for g in genotype if g not in ("0", ".")]) == 0:
            #     # Uninteresting, not present on sample
            #     continue

            yield Call(variant=variant, genotype=genotype, sample_id=sample_id)

    @property
    def n_of_variants(self) -> int:
        return sum(v["n_of_variants"] for v in self.file_metadata.values())

    def variants(
        self,
        assembly_id: Optional[str] = None,
        chromosome: Optional[str] = None,
        start_min: Optional[int] = None,
        start_max: Optional[int] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Generator[Variant, None, None]:
        variants_passed = 0
        variants_seen = 0

        # TODO: Optimize offset/count
        #  e.g. by skipping entire VCFs if we know their row counts a-priori

        for vcf, vcf_metadata in filter(lambda fm: assembly_id is None or fm[1]["assembly_id"] == assembly_id,
                                        self.file_metadata.items()):
            if all((
                chromosome is None,
                start_min is None,
                start_max is None,
                offset is not None,
                vcf_metadata["n_of_variants"] < offset - variants_seen,
            )):
                # If the entire file has less variants than the remaining offset, skip it.
                variants_seen += vcf_metadata["n_of_variants"]
                continue

            try:
                f = pysam.TabixFile(vcf, parser=pysam.asTuple(), threads=WORKERS)

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

                    if offset is not None and variants_passed < offset:
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
                        assembly_id=vcf_metadata["assembly_id"],
                        chromosome=row[0],
                        start_pos=int(row[1]),
                        ref_bases=row[3],
                        alt_bases=tuple(row[4].split(",")),
                    )

                    variant.calls = tuple(VCFVariantTable._variant_calls(variant, vcf_metadata["sample_ids"], row))
                    yield variant

                    variants_seen += 1

            except ValueError:
                # Region not found in Tabix file
                continue


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

    @staticmethod
    def _get_vcf_row_count(vcf_path: str) -> int:
        p = subprocess.Popen(("bcftools", "index", "--nrecords", vcf_path), stdout=subprocess.PIPE)
        return int(p.stdout.read().strip())  # TODO: Handle error

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
            vcf_row_counts = tuple(self._get_vcf_row_count(vcf_path) for vcf_path in vcf_files)

            ds = VCFVariantTable(
                table_id=d,
                name=open(name_path, "r").read().strip() if os.path.exists(name_path) else None,
                metadata=(json.load(open(metadata_path, "r")) if os.path.exists(metadata_path) else {}),
                files=vcf_files,
                file_metadata={f: {"assembly_id": a, "sample_ids": s, "n_of_variants": v}
                               for f, a, s, v in zip(vcf_files, assembly_ids, sample_ids, vcf_row_counts)}
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
