import pytest

from chord_variant_service.datasets import MemoryVariantTable, IDGenerationFailure, MemoryTableManager


def test_memory_table_manager():
    mm = MemoryTableManager()

    assert mm.get_dataset("fixed_id") is None

    mm.create_dataset_and_update("test", {})

    assert isinstance(mm.get_dataset("fixed_id"), MemoryVariantTable)

    mm.get_dataset("fixed_id").variant_store.append({
        "assembly_id": "GRCh38",
        "chromosome": "1",
        "start": 5000,
        "end": 5001,
        "ref": "C",
        "alt": "T"
    })

    mm.get_dataset("fixed_id").variant_store.append({
        "assembly_id": "GRCh37",
        "chromosome": "5",
        "start": 5000,
        "end": 5001,
        "ref": "C",
        "alt": "T"
    })

    mm.get_dataset("fixed_id").variant_store.append({
        "assembly_id": "GRCh37",
        "chromosome": "1",
        "start": 5000,
        "end": 5001,
        "ref": "C",
        "alt": "T"
    })

    gen = mm.get_dataset("fixed_id").variants("GRCh37", "1")

    v = next(gen)
    assert isinstance(v, dict)
    assert v["assembly_id"] == "GRCh37"
    assert v["chromosome"] == "1"

    with pytest.raises(StopIteration):
        next(gen)

    gen2 = mm.get_dataset("fixed_id").variants("GRCh37", "1", 7000)
    with pytest.raises(StopIteration):
        next(gen2)

    gen3 = mm.get_dataset("fixed_id").variants("GRCh37", "1", None, 3000)
    with pytest.raises(StopIteration):
        next(gen3)

    with pytest.raises(IDGenerationFailure):
        mm.create_dataset_and_update("test", {})

    mm.delete_dataset_and_update("fixed_id")

    assert mm.get_dataset("fixed_id") is None
