import os

from chord_variant_service.constants import SERVICE_NAME
from chord_variant_service.tables.vcf.base_manager import BaseVCFTableManager, VCFTableFolder


class VCFTableManager(BaseVCFTableManager):
    def _get_table_vcf_files(self, table_folder: VCFTableFolder):
        good_files = []

        for file in (f for f in os.listdir(table_folder.dir) if f.endswith(".vcf.gz")):
            try:
                good_files.append(BaseVCFTableManager.get_vcf_file_record(os.path.join(table_folder.dir, file)))
            except ValueError:
                print(f"[{SERVICE_NAME}] Could not load variant file '{os.path.join(table_folder.dir, file)}'")

        return tuple(good_files)
