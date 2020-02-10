from chord_variant_service.variants import Variant, Call

__all__ = [
    "VARIANT_1",
    "VARIANT_2",
    "VARIANT_3",
]

VARIANT_1 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5000,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_1.calls = (Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phase_set=None),)

VARIANT_2 = Variant(
    assembly_id="GRCh38",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_2.calls = (Call(variant=VARIANT_2, sample_id="S0001", genotype=(0, 1), phase_set=None),)

VARIANT_3 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_3.calls = (Call(variant=VARIANT_3, sample_id="S0001", genotype=(0, 1), phase_set=None),)
