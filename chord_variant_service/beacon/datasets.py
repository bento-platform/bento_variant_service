import datetime
from typing import Tuple
from chord_variant_service.tables.vcf_file import VCFFile


__all__ = [
    "make_beacon_dataset_id",
    "BeaconDataset",
]


def make_beacon_dataset_id(tp: Tuple[str, str]) -> str:
    return f"{tp[0]}:{tp[1]}"


class BeaconDataset:
    def __init__(
        self,
        table_id: str,
        table_name: str,
        table_metadata: dict,
        assembly_id: str,
        files: Tuple[VCFFile] = ()
    ):
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
