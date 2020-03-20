import os

from chord_variant_service.tables.vcf.base_manager import BaseVCFTableManager
from chord_variant_service.tables.vcf.table import VCFVariantTable


class VCFTableManager(BaseVCFTableManager):  # pragma: no cover
    def update_tables(self):
        for t in self.table_folders:
            vcf_files = tuple(BaseVCFTableManager.get_vcf_file_record(os.path.join(t.dir, file))
                              for file in os.listdir(t.dir) if file.endswith(".vcf.gz"))

            table = VCFVariantTable(
                table_id=t.id,
                name=t.name,
                metadata=t.metadata,
                files=vcf_files,
            )

            self._tables[t.id] = table
            for bd in table.beacon_datasets:
                self._beacon_datasets[bd.beacon_id_tuple] = bd
