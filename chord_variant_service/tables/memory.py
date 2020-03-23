from itertools import chain
from typing import Dict, Generator, List, Optional, Tuple

from chord_variant_service.beacon.datasets import BeaconDataset
from chord_variant_service.variants.models import Variant
from chord_variant_service.tables.base import VariantTable, TableManager
from chord_variant_service.tables.exceptions import IDGenerationFailure


__all__ = [
    "MemoryVariantTable",
    "MemoryTableManager",
]


class MemoryVariantTable(VariantTable):
    def __init__(self, table_id, name, metadata, assembly_ids=()):
        super().__init__(table_id, name, metadata, assembly_ids)
        self.variant_store: List[Variant] = []

    @property
    def n_of_variants(self) -> int:
        return len(self.variant_store)

    @property
    def n_of_samples(self) -> int:
        sample_set = set()
        for v in self.variants():
            for c in v.calls:
                sample_set.add(c.sample_id)
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
        offset: int = 0 if offset is None else offset
        if offset < 0 or offset >= len(self.variant_store):
            return

        count: int = len(self.variant_store) - offset if count is None else count
        if count <= 0:
            return

        for v in self.variant_store[offset:offset+count]:
            if chromosome is not None and v.chromosome != chromosome:
                continue

            if start_min is not None and v.start_pos < start_min:  # inclusive
                continue

            if start_max is not None and v.start_pos >= start_max:  # exclusive
                continue

            if assembly_id is not None and v.assembly_id != assembly_id:
                continue

            if only_interesting and next((c for c in v.calls if c.is_interesting), None) is None:
                # Uninteresting; skip this variant
                continue

            yield v

    def add_variant(self, variant: Variant):
        self.variant_store.append(variant)
        self._assembly_ids.add(variant.assembly_id)


class MemoryTableManager(TableManager):
    def __init__(self):
        self.tables = {}
        self.id_to_generate = "fixed_id"

    def get_table(self, table_id: str) -> Optional[dict]:
        return self.tables.get(table_id, None)

    def get_tables(self) -> dict:
        return self.tables

    def get_beacon_datasets(self) -> Dict[Tuple[str, str], BeaconDataset]:
        return {bd.beacon_id_tuple: bd for bd in chain.from_iterable(d.beacon_datasets for d in self.tables.values())}

    def update_tables(self):  # pragma: no cover
        pass

    def _generate_table_id(self) -> Optional[str]:
        return None if self.id_to_generate in self.tables else self.id_to_generate

    def create_table_and_update(self, name: str, metadata: dict) -> MemoryVariantTable:
        table_id = self._generate_table_id()
        if table_id is None:
            raise IDGenerationFailure()

        new_table = MemoryVariantTable(table_id=table_id, name=name, metadata=metadata, assembly_ids=("GRCh37",))
        self.tables[table_id] = new_table

        return new_table

    def delete_table_and_update(self, table_id: str):
        del self.tables[table_id]
