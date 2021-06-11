import json
import os
import sys

from typing import Tuple

from bento_variant_service.constants import SERVICE_NAME
from bento_variant_service.tables.vcf.base_manager import BaseVCFTableManager, VCFTableFolder
from bento_variant_service.tables.vcf.drs_utils import DRS_DATA_SCHEMA_VALIDATOR
from bento_variant_service.tables.vcf.file import VCFFile


class DRSVCFTableManager(BaseVCFTableManager):
    def _get_table_vcf_files(self, table_folder: VCFTableFolder) -> Tuple[VCFFile, ...]:
        vcf_files = []

        for file in (f for f in os.listdir(table_folder.dir) if f.endswith(".drs.json")):
            # We define a custom .drs.json schema for dealing with DRS records
            with open(os.path.join(table_folder.dir, file)) as df:
                drs_data = json.load(df)

                if not DRS_DATA_SCHEMA_VALIDATOR.is_valid(drs_data):
                    # TODO: Report this better
                    print(f"[{SERVICE_NAME}] Error processing DRS record: {os.path.join(table_folder.dir, file)}",
                          file=sys.stderr, flush=True)
                    continue

                try:
                    vcf_files.append(BaseVCFTableManager.get_vcf_file_record(drs_data["data"], drs_data["index"]))
                except ValueError as e:
                    print(f"[{SERVICE_NAME}] Could not load variant file '{os.path.join(table_folder.dir, file)}': "
                          f"encountered ValueError ({str(e)})", file=sys.stderr, flush=True)
                except TypeError as e:  # drs_vcf_to_internal_paths returned None
                    # TODO: This is a bad error handler since it also catches random other TypeErrors
                    #  Also, we should probably return if errors occur...
                    print(f"[{SERVICE_NAME}] No result from drs_vcf_to_internal_paths or encountered TypeError "
                          f"({str(e)})", file=sys.stderr, flush=True)

        return tuple(vcf_files)
