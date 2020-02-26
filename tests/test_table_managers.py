import pytest

from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.tables.memory import MemoryVariantTable, MemoryTableManager
from chord_variant_service.variants.models import Variant
from .shared_data import VARIANT_1


def test_memory_table_manager():
    mm = MemoryTableManager()

    assert mm.get_table("fixed_id") is None

    mm.create_table_and_update("test", {})

    assert isinstance(mm.get_table("fixed_id"), MemoryVariantTable)

    mm.get_table("fixed_id").variant_store.append(Variant(
        assembly_id="GRCh38",
        chromosome="1",
        start_pos=5000,
        ref_bases="C",
        alt_bases=("T",),
    ))

    mm.get_table("fixed_id").variant_store.append(Variant(
        assembly_id="GRCh37",
        chromosome="5",
        start_pos=5000,
        ref_bases="C",
        alt_bases=("T",),
    ))

    mm.get_table("fixed_id").variant_store.append(VARIANT_1)

    gen = mm.get_table("fixed_id").variants("GRCh37", "1")

    v = next(gen)

    assert v == VARIANT_1

    with pytest.raises(StopIteration):
        next(gen)

    gen2 = mm.get_table("fixed_id").variants("GRCh37", "1", 7000)
    with pytest.raises(StopIteration):
        next(gen2)

    gen3 = mm.get_table("fixed_id").variants("GRCh37", "1", None, 3000)
    with pytest.raises(StopIteration):
        next(gen3)

    with pytest.raises(IDGenerationFailure):
        mm.create_table_and_update("test", {})

    mm.delete_table_and_update("fixed_id")

    assert mm.get_table("fixed_id") is None
