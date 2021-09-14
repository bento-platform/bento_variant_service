import pytest
from bento_variant_service.variants import genotypes as gt
from bento_variant_service.variants.models import AlleleClass, Allele, ALLELE_MISSING, ALLELE_MISSING_UPSTREAM, Call
from .shared_data import T_ALLELE, C_ALLELE, VARIANT_1, VARIANT_2, VARIANT_3, VARIANT_6, CALL_1, CALL_2


def test_allele_bad_formatting():
    with pytest.raises(ValueError):
        Allele(AlleleClass.SEQUENCE, None)
    with pytest.raises(ValueError):
        Allele(AlleleClass.STRUCTURAL, None)


def test_allele_equality():
    assert T_ALLELE != 5
    assert T_ALLELE != "test"
    assert T_ALLELE != VARIANT_1
    assert T_ALLELE == T_ALLELE
    assert C_ALLELE == C_ALLELE
    assert T_ALLELE != C_ALLELE
    assert C_ALLELE != T_ALLELE

    assert hash(C_ALLELE) == hash("C")


def test_variant_equality():
    assert VARIANT_1 == VARIANT_1
    assert VARIANT_2 == VARIANT_2
    assert VARIANT_3 == VARIANT_3

    assert VARIANT_1 != "test"
    assert VARIANT_2 != VARIANT_2.as_chord_representation()

    assert VARIANT_6 != VARIANT_3
    assert VARIANT_3 != VARIANT_6


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
    assert c.genotype_alleles == (C_ALLELE,)
    assert c.genotype_type == gt.GT_REFERENCE
    assert not c.is_interesting

    c = Call(VARIANT_1, "S0001", (1,))
    assert c.genotype_alleles == (T_ALLELE,)
    assert c.genotype_type == gt.GT_ALTERNATE
    assert c.is_interesting

    # Diploid

    c = Call(VARIANT_1, "S0001", (0, 0))
    assert c.genotype_alleles == (C_ALLELE, C_ALLELE)
    assert c.genotype_type == gt.GT_HOMOZYGOUS_REFERENCE
    assert not c.is_interesting

    c = Call(VARIANT_1, "S0001", (0, 1))
    assert c.genotype_alleles == (C_ALLELE, T_ALLELE)
    assert c.genotype_type == gt.GT_HETEROZYGOUS
    assert c.is_interesting

    c = Call(VARIANT_1, "S0001", (1, 1))
    assert c.genotype_alleles == (T_ALLELE, T_ALLELE)
    assert c.genotype_type == gt.GT_HOMOZYGOUS_ALTERNATE
    assert c.is_interesting

    c = Call(VARIANT_1, "S0001", (".",))
    assert c.genotype_alleles[0] is ALLELE_MISSING
    assert c.genotype_type == gt.GT_MISSING
    assert not c.is_interesting

    c = Call(VARIANT_1, "S0001", ("*",))
    assert c.genotype_alleles[0] is ALLELE_MISSING_UPSTREAM
    assert c.genotype_type == gt.GT_MISSING_UPSTREAM_DELETION
    assert not c.is_interesting

    c = Call(VARIANT_6, "S0001", (1, 1))  # TODO: We need to decide if this is really HOMOZYGOUS_ALTERNATE or not
    assert c.genotype_alleles[0].allele_class == AlleleClass.STRUCTURAL
    assert c.genotype_type == gt.GT_HOMOZYGOUS_ALTERNATE
    assert c.is_interesting
