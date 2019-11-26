from .shared_data import *


def test_variant_equality():
    assert VARIANT_1 == VARIANT_1
    assert VARIANT_2 == VARIANT_2
    assert VARIANT_3 == VARIANT_3
    assert VARIANT_1 != "test"
    assert VARIANT_2 != VARIANT_2.as_chord_representation()
