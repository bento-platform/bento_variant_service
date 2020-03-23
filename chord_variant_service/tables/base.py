from abc import ABC, abstractmethod
from typing import Dict, Generator, Optional, Sequence, Set, Tuple

from chord_variant_service.beacon.datasets import BeaconDataset
from chord_variant_service.variants.models import Variant
from chord_variant_service.variants.schemas import VARIANT_SCHEMA


__all__ = [
    "VariantTable",
    "TableManager",
]


class VariantTable(ABC):  # pragma: no cover
    def __init__(self, table_id: str, name: Optional[str], metadata: dict, assembly_ids: Sequence[str] = ()):
        self.table_id = table_id
        self.name = name
        self.metadata = metadata
        self._assembly_ids = set(assembly_ids)

    def as_table_response(self):
        # Don't leak sample IDs to the outside world
        return {
            "id": self.table_id,
            "name": self.name,
            "metadata": self.metadata,
            "assembly_ids": list(self._assembly_ids),
            "schema": VARIANT_SCHEMA
        }

    @property
    def beacon_datasets(self):
        return tuple(
            BeaconDataset(table_id=self.table_id, table_name=self.name, table_metadata=self.metadata, assembly_id=a)
            for a in sorted(self._assembly_ids)
        )

    # TODO: Breakdowns by reference genome!

    @property
    def assembly_ids(self) -> Set[str]:
        return self._assembly_ids.copy()

    @property
    @abstractmethod
    def n_of_variants(self) -> int:
        pass

    @property
    @abstractmethod
    def n_of_samples(self) -> int:
        pass

    @abstractmethod
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
        yield None


class TableManager(ABC):  # pragma: no cover
    # TODO: Rename
    @abstractmethod
    def get_table(self, table_id: str) -> Optional[VariantTable]:
        pass

    @abstractmethod
    def get_tables(self) -> Dict[str, VariantTable]:  # TODO: Rename get_tables
        return {}

    @abstractmethod
    def get_beacon_datasets(self) -> Dict[Tuple[str, str], BeaconDataset]:
        return {}

    @abstractmethod
    def update_tables(self):
        pass

    @abstractmethod
    def _generate_table_id(self) -> Optional[str]:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def create_table_and_update(self, name: str, metadata: dict) -> VariantTable:
        pass

    # TODO: Rename create_table_and_update, table_id
    @abstractmethod
    def delete_table_and_update(self, table_id: str):
        pass
