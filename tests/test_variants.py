from .shared_data import *


def test_variant_equality():
    assert VARIANT_1 == VARIANT_1
    assert VARIANT_2 == VARIANT_2
    assert VARIANT_3 == VARIANT_3
    assert VARIANT_1 != "test"
    assert VARIANT_2 != VARIANT_2.as_chord_representation()


def test_call_fake_equality():
    assert CALL_1.variant != CALL_2.variant
    assert CALL_1.eq_no_variant_check(CALL_2)
    assert not CALL_1.eq_no_variant_check("test")
