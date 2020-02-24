import pytest
from chord_variant_service.variants import *
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


def test_call_genotypes():
    with pytest.raises(ValueError):
        # Genotypes must be at least length 1
        Call(VARIANT_1, "S0001", ())

    # Haploid

    c = Call(VARIANT_1, "S0001", (0,))
    assert c.genotype_bases == ("C",)
    assert c.genotype_type == GT_REFERENCE

    c = Call(VARIANT_1, "S0001", (1,))
    assert c.genotype_bases == ("T",)
    assert c.genotype_type == GT_ALTERNATE

    # Diploid

    c = Call(VARIANT_1, "S0001", (0, 0))
    assert c.genotype_bases == ("C", "C")
    assert c.genotype_type == GT_HOMOZYGOUS_REFERENCE

    c = Call(VARIANT_1, "S0001", (0, 1))
    assert c.genotype_bases == ("C", "T")
    assert c.genotype_type == GT_HETEROZYGOUS

    c = Call(VARIANT_1, "S0001", (1, 1))
    assert c.genotype_bases == ("T", "T")
    assert c.genotype_type == GT_HOMOZYGOUS_ALTERNATE

    c = Call(VARIANT_1, "S0001", (None, None))
    assert c.genotype_bases[0] is None and c.genotype_bases[1] is None
    assert c.genotype_type == GT_UNCALLED
