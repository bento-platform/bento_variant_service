# noinspection PyProtectedMember
from chord_variant_service.beacon import _make_beacon_dataset_id
from uuid import uuid4


def test_make_beacon_dataset_id():
    some_id = str(uuid4())
    assert _make_beacon_dataset_id((some_id, "GRCh37")) == f"{some_id}:GRCh37"
