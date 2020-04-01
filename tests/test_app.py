from chord_variant_service.tables.memory import MemoryTableManager
from chord_variant_service import table_manager as tm


# noinspection PyProtectedMember
def test_post_hook(uninitialized_client):
    assert tm._table_manager is None

    r = uninitialized_client.get("/private/post-start-hook")
    assert r.status_code == 204

    # Table manager should be initialized now
    assert isinstance(tm._table_manager, MemoryTableManager)
