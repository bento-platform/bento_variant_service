import os
from bento_variant_service.variants.models import Variant, Call

__all__ = [
    "VCF_ONE_VAR_FILE_PATH",
    "VCF_ONE_VAR_INDEX_FILE_PATH",
    "VCF_ONE_VAR_FILE_URI",

    "VCF_TEN_VAR_FILE_PATH",
    "VCF_TEN_VAR_INDEX_FILE_PATH",

    "VCF_MISSING_9_FILE_PATH",
    "VCF_MISSING_9_INDEX_FILE_PATH",

    "VCF_NO_TBI_FILE_PATH",

    "VARIANT_1",
    "VARIANT_2",
    "VARIANT_3",
    "VARIANT_4",
    "VARIANT_5",

    "CALL_1",
    "CALL_2",
]

TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), "data")

VCF_ONE_VAR_FILE_PATH = os.path.join(TEST_DATA_PATH, "one_variant_22.vcf.gz")
VCF_ONE_VAR_INDEX_FILE_PATH = f"{VCF_ONE_VAR_FILE_PATH}.tbi"
VCF_ONE_VAR_FILE_URI = f"file://{VCF_ONE_VAR_FILE_PATH}"

VCF_TEN_VAR_FILE_PATH = os.path.join(TEST_DATA_PATH, "ten_variants_22.vcf.gz")
VCF_TEN_VAR_INDEX_FILE_PATH = f"{VCF_TEN_VAR_FILE_PATH}.tbi"

VCF_MISSING_9_FILE_PATH = os.path.join(TEST_DATA_PATH, "missing_9th_22.vcf.gz")
VCF_MISSING_9_INDEX_FILE_PATH = f"{VCF_MISSING_9_FILE_PATH}.tbi"

VCF_NO_TBI_FILE_PATH = os.path.join(TEST_DATA_PATH, "no_tbi_22.vcf.gz")

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
    alt_bases=("T", "A"),
)

CALL_2 = Call(variant=VARIANT_2, sample_id="S0001", genotype=(0, 1), phase_set=None)
VARIANT_2.calls = (CALL_2,)

VARIANT_3 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_bases=("T", "G"),
)

VARIANT_3.calls = (Call(variant=VARIANT_3, sample_id="S0001", genotype=(0, 0), phase_set=None),)

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
