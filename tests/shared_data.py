import os
from bento_variant_service.variants.models import AlleleClass, Allele, Variant, Call

__all__ = [
    "VCF_ONE_VAR_FILE_PATH",
    "VCF_ONE_VAR_INDEX_FILE_PATH",
    "VCF_ONE_VAR_FILE_URI",

    "VCF_TEN_VAR_FILE_PATH",
    "VCF_TEN_VAR_INDEX_FILE_PATH",

    "VCF_MISSING_9_FILE_PATH",
    "VCF_MISSING_9_INDEX_FILE_PATH",

    "VCF_NO_TBI_FILE_PATH",

    "DRS_RECORD_PATH",
    "DRS_VCF_ID",
    "DRS_IDX_ID",
    "DRS_VCF_RESPONSE",
    "DRS_IDX_RESPONSE",
    "DRS_404_RESPONSE",

    "T_ALLELE",
    "A_ALLELE",
    "G_ALLELE",
    "C_ALLELE",

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

DRS_RECORD_PATH = os.path.join(TEST_DATA_PATH, "test.drs.json")

DRS_VCF_ID = "e120a041-dd95-4d18-95cb-4ea31fd06a96"
DRS_IDX_ID = "33d4c424-8197-4835-be65-91f7d9d54fa1"

DRS_VCF_RESPONSE = {
    "id": DRS_VCF_ID,
    "access_methods": [
        {"type": "file", "access_url": {"url": f"file://{os.path.abspath(VCF_TEN_VAR_FILE_PATH)}"}},
    ],
    "checksums": [
        {"checksum": "067a0009ffc1b21e6850f711c8b67422d381405bc53c904a5c1f75ee93c665c4", "type": "sha-256"},
    ],
    "created_time": "2021-05-26T14:00:00Z",
    "size": os.path.getsize(VCF_TEN_VAR_FILE_PATH),
    "self_uri": f"drs://drs.local/{DRS_VCF_ID}",
}

DRS_IDX_RESPONSE = {
    "id": DRS_IDX_ID,
    "access_methods": [
        {"type": "file", "access_url": {"url": f"file://{os.path.abspath(VCF_TEN_VAR_INDEX_FILE_PATH)}"}},
    ],
    "checksums": [
        {"checksum": "9d3c5ba95b114d9617bf79cc347eb66fb98ad926eacb342c75a1f7ebf6814b0c", "type": "sha-256"},
    ],
    "created_time": "2021-05-26T14:00:00Z",
    "size": os.path.getsize(VCF_TEN_VAR_INDEX_FILE_PATH),
    "self_uri": f"drs://drs.local/{DRS_IDX_ID}",
}

DRS_404_RESPONSE = {
    "status_code": 404,
    "message": "Not found",
}

T_ALLELE = Allele(AlleleClass.SEQUENCE, "T")
A_ALLELE = Allele(AlleleClass.SEQUENCE, "A")
G_ALLELE = Allele(AlleleClass.SEQUENCE, "G")
C_ALLELE = Allele(AlleleClass.SEQUENCE, "C")

VARIANT_1 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5000,
    ref_bases="C",
    alt_alleles=(T_ALLELE,),
)

CALL_1 = Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phased=True, phase_set=None)
VARIANT_1.calls = (CALL_1,)

VARIANT_2 = Variant(
    assembly_id="GRCh38",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_alleles=(T_ALLELE, A_ALLELE),
)

CALL_2 = Call(variant=VARIANT_2, sample_id="S0001", genotype=(0, 1), phased=True, phase_set=None)
VARIANT_2.calls = (CALL_2,)

VARIANT_3 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=5003,
    ref_bases="C",
    alt_alleles=(T_ALLELE, G_ALLELE),
)

VARIANT_3.calls = (Call(variant=VARIANT_3, sample_id="S0001", genotype=(0, 0), phased=True, phase_set=None),)

VARIANT_4 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=7000,
    ref_bases="C",
    alt_alleles=(T_ALLELE,),
)

VARIANT_4.calls = (Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phased=True, phase_set=None),)

VARIANT_5 = Variant(
    assembly_id="GRCh37",
    chromosome="1",
    start_pos=7001,
    ref_bases="C",
    alt_alleles=(T_ALLELE,),
)

VARIANT_5.calls = (Call(variant=VARIANT_1, sample_id="S0001", genotype=(0, 1), phased=True, phase_set=None),)
