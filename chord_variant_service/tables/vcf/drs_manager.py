import json
import os
import sys

from jsonschema import validate, ValidationError

from chord_variant_service.constants import SERVICE_NAME
from chord_variant_service.tables.vcf.base_manager import BaseVCFTableManager
from chord_variant_service.tables.vcf.drs_utils import DRS_DATA_SCHEMA, drs_vcf_to_internal_paths
from chord_variant_service.tables.vcf.table import VCFVariantTable


class DRSVCFTableManager(BaseVCFTableManager):  # pragma: no cover
    def _update_tables(self):
        for t in self.table_folders:
            vcf_files = []

            for file in os.listdir(t.dir):
                if not file.endswith(".drs.json"):
                    continue

                # We define a custom .drs.json schema for dealing with DRS records
                with open(os.path.join(t.dir, file)) as df:
                    drs_data = json.load(df)

                    try:
                        validate(drs_data, DRS_DATA_SCHEMA)
                    except ValidationError:
                        # TODO: Report this better
                        print(f"[{SERVICE_NAME}] Error processing DRS record: {os.path.join(t.dir, file)}",
                              file=sys.stderr, flush=True)
                        continue

                    vcf, idx, _vh, _ih = drs_vcf_to_internal_paths(drs_data["data"], drs_data)
                    vcf_files.append(BaseVCFTableManager.get_vcf_file_record(vcf, idx))

            table = VCFVariantTable(
                table_id=t.id,
                name=t.name,
                metadata=t.metadata,
                files=tuple(vcf_files),
            )

            self._tables[t.id] = table
            for bd in table.beacon_datasets:
                self._beacon_datasets[bd.beacon_id_tuple] = bd
