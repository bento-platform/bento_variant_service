from chord_variant_service.variants import Variant, Call

__all__ = [
    "VARIANT_1",
    "VARIANT_2",
    "VARIANT_3",
    "VARIANT_4",
    "VARIANT_5",

    "CALL_1",
    "CALL_2",
]

VARIANT_1 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5000,
    ref_bases="C",
    alt_bases=("T",),
)

CALL_1 = Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phase_set=None)
VARIANT_1.calls = (CALL_1,)

VARIANT_2 = Variant(
    assembly_id="GRCh38",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases=("T",),
)

CALL_2 = Call(variant=VARIANT_2, sample_id="S0001", genotype=(0, 1), phase_set=None)
VARIANT_2.calls = (CALL_2,)

VARIANT_3 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_3.calls = (Call(variant=VARIANT_3, sample_id="S0001", genotype=(0, 1), phase_set=None),)

VARIANT_4 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=7000,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_4.calls = (Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phase_set=None),)

VARIANT_5 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=7001,
    ref_bases="C",
    alt_bases=("T",),
)

VARIANT_5.calls = (Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phase_set=None),)
