import json
import os
import sys

from jsonschema import validate, ValidationError
from typing import Tuple

from chord_variant_service.constants import SERVICE_NAME
from chord_variant_service.tables.vcf.base_manager import BaseVCFTableManager, VCFTableFolder
from chord_variant_service.tables.vcf.drs_utils import DRS_DATA_SCHEMA, drs_vcf_to_internal_paths
from chord_variant_service.tables.vcf.file import VCFFile


class DRSVCFTableManager(BaseVCFTableManager):  # pragma: no cover
    def _get_table_vcf_files(self, table_folder: VCFTableFolder) -> Tuple[VCFFile, ...]:
        vcf_files = []

        for file in os.listdir(table_folder.dir):
            if not file.endswith(".drs.json"):
                continue

            # We define a custom .drs.json schema for dealing with DRS records
            with open(os.path.join(table_folder.dir, file)) as df:
                drs_data = json.load(df)

                try:
                    validate(drs_data, DRS_DATA_SCHEMA)
                except ValidationError:
                    # TODO: Report this better
                    print(f"[{SERVICE_NAME}] Error processing DRS record: {os.path.join(table_folder.dir, file)}",
                          file=sys.stderr, flush=True)
                    continue

                try:
                    vcf, idx, _vh, _ih = drs_vcf_to_internal_paths(drs_data["data"], drs_data["index"])
                    vcf_files.append(BaseVCFTableManager.get_vcf_file_record(vcf, idx))
                except ValueError:
                    print(f"[{SERVICE_NAME}] Could not load variant file '{os.path.join(table_folder.dir, file)}'")

        return tuple(vcf_files)
