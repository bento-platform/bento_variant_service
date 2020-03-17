import pytest

from flask import g

from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.tables.memory import MemoryVariantTable, MemoryTableManager
from chord_variant_service.tables.vcf.drs_manager import DRSVCFTableManager
from chord_variant_service.tables.vcf.vcf_manager import VCFTableManager
from chord_variant_service.table_manager import (
    MANAGER_TYPE_DRS,
    MANAGER_TYPE_MEMORY,
    MANAGER_TYPE_VCF,
    create_table_manager_of_type,
    clear_table_manager,
)
from chord_variant_service.variants.models import Variant
from .shared_data import VARIANT_1


def test_table_manage_creation():
    m = create_table_manager_of_type(MANAGER_TYPE_DRS)
    assert isinstance(m, DRSVCFTableManager)

    m = create_table_manager_of_type(MANAGER_TYPE_MEMORY)
    assert isinstance(m, MemoryTableManager)

    m = create_table_manager_of_type(MANAGER_TYPE_VCF)
    assert isinstance(m, VCFTableManager)

    m = create_table_manager_of_type("garbage")
    assert m is None


def test_table_manager_up_down(table_manager):
    assert isinstance(table_manager, MemoryTableManager)
    clear_table_manager(None)
    assert "table_manager" not in g


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

    gen = mm.get_table("fixed_id").variants("GRCh37", "1", 7000)
    with pytest.raises(StopIteration):
        next(gen)

    gen = mm.get_table("fixed_id").variants("GRCh37", "1", None, 3000)
    with pytest.raises(StopIteration):
        next(gen)

    gen = mm.get_table("fixed_id").variants(offset=-1)
    with pytest.raises(StopIteration):
        next(gen)

    gen = mm.get_table("fixed_id").variants(offset=4)
    with pytest.raises(StopIteration):
        next(gen)

    gen = mm.get_table("fixed_id").variants(count=0)
    with pytest.raises(StopIteration):
        next(gen)

    with pytest.raises(IDGenerationFailure):
        mm.create_table_and_update("test", {})

    mm.delete_table_and_update("fixed_id")

    assert mm.get_table("fixed_id") is None
