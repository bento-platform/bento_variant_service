from chord_variant_service.variants import SampleVariant

__all__ = [
    "VARIANT_1",
    "VARIANT_2",
    "VARIANT_3",
]

VARIANT_1 = SampleVariant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5000,
    ref_bases="C",
    alt_bases="T",
    sample_id="S0001"
)

VARIANT_2 = SampleVariant(
    assembly_id="GRCh38",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases="T",
    sample_id="S0001"
)

VARIANT_3 = SampleVariant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases="T",
    sample_id="S0001"
)
