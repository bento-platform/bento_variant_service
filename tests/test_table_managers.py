import os
import pytest
import shutil

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
from .shared_data import VCF_FILE_PATH, VCF_INDEX_FILE_PATH, VARIANT_1


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

    tbl = mm.get_table("fixed_id")
    assert isinstance(tbl, MemoryVariantTable)

    ts = mm.get_tables()
    assert len(ts) == 1
    assert ts["fixed_id"] is tbl

    tbl.variant_store.append(Variant(
        assembly_id="GRCh38",
        chromosome="1",
        start_pos=5000,
        ref_bases="C",
        alt_bases=("T",),
    ))

    tbl.variant_store.append(Variant(
        assembly_id="GRCh37",
        chromosome="5",
        start_pos=5000,
        ref_bases="C",
        alt_bases=("T",),
    ))

    tbl.variant_store.append(VARIANT_1)

    gen = tbl.variants("GRCh37", "1")

    v = next(gen)

    assert v == VARIANT_1

    with pytest.raises(StopIteration):
        next(gen)

    gen = tbl.variants("GRCh37", "1", 7000)
    with pytest.raises(StopIteration):
        next(gen)

    gen = tbl.variants("GRCh37", "1", None, 3000)
    with pytest.raises(StopIteration):
        next(gen)

    gen = tbl.variants(offset=-1)
    with pytest.raises(StopIteration):
        next(gen)

    gen = tbl.variants(offset=4)
    with pytest.raises(StopIteration):
        next(gen)

    gen = tbl.variants(count=0)
    with pytest.raises(StopIteration):
        next(gen)

    with pytest.raises(IDGenerationFailure):
        mm.create_table_and_update("test", {})

    mm.delete_table_and_update("fixed_id")

    assert mm.get_table("fixed_id") is None


def test_vcf_table_manager(tmpdir):
    data_path = tmpdir / "data"
    data_path.mkdir()
    vm = VCFTableManager(data_path=str(data_path))

    t1 = vm.create_table_and_update("test", {})
    assert vm.get_table(t1.table_id) is not None
    ts = vm.get_tables()
    assert len(ts) == 1
    assert ts[t1.table_id] is t1

    shutil.copyfile(VCF_FILE_PATH, os.path.join(data_path, t1.table_id, "test.vcf.gz"))
    shutil.copyfile(VCF_INDEX_FILE_PATH, os.path.join(data_path, t1.table_id, "test.vcf.gz.tbi"))
    vm.update_tables()

    assert vm.get_table(t1.table_id).n_of_variants == 1
    assert vm.get_table(t1.table_id).n_of_samples == 835

    assert len(vm.get_table(t1.table_id).files) == 1

    assert len(tuple(vm.get_table(t1.table_id).variants())) == 1
    assert len(tuple(vm.get_table(t1.table_id).variants(chromosome="22"))) == 1
    assert len(tuple(vm.get_table(t1.table_id).variants(chromosome="21"))) == 0
    assert len(tuple(vm.get_table(t1.table_id).variants(offset=1))) == 0
    # TODO: Test that VCF is ingested better

    vm.delete_table_and_update(t1.table_id)
    assert vm.get_table(t1.table_id) is None
