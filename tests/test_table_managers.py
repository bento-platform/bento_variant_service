import pytest

from chord_variant_service.datasets import IDGenerationFailure, MemoryTableManager


def test_memory_table_manager():
    mm = MemoryTableManager()

    assert mm.get_dataset("fixed_id") is None

    mm.create_dataset_and_update("test", {})

    assert isinstance(mm.get_dataset("fixed_id"), dict)

    with pytest.raises(IDGenerationFailure):
        mm.create_dataset_and_update("test", {})

    mm.delete_dataset_and_update("fixed_id")

    assert mm.get_dataset("fixed_id") is None
