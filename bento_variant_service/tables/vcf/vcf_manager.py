import os

from bento_variant_service.constants import SERVICE_NAME
from bento_variant_service.tables.vcf.base_manager import BaseVCFTableManager, VCFTableFolder


class VCFTableManager(BaseVCFTableManager):
    def _get_table_vcf_files(self, table_folder: VCFTableFolder):
        good_files = []

        for file in (f for f in os.listdir(table_folder.dir) if f.endswith(".vcf.gz")):
            try:
                good_files.append(BaseVCFTableManager.get_vcf_file_record(
                    f"file://{os.path.abspath(os.path.join(table_folder.dir, file))}"))
            except ValueError as e:
                print(f"[{SERVICE_NAME}] Could not load variant file '{os.path.join(table_folder.dir, file)}' "
                      f"(encountered error: {e})")

        return tuple(good_files)
