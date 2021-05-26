from bento_variant_service.tables.vcf.drs_manager import DRSVCFTableManager
from bento_variant_service.tables.vcf.drs_utils import drs_vcf_to_internal_paths
from .shared_data import DRS_VCF_ID, DRS_IDX_ID


# noinspection PyUnusedLocal
def test_drs_vcf_to_internal_paths(client_drs_mode, drs_table_manager: DRSVCFTableManager):
    assert drs_vcf_to_internal_paths(
        f"http://drs.local/objects/{DRS_VCF_ID}",
        f"http://drs.local/objects/{DRS_IDX_ID}",
    ) is None

    assert drs_vcf_to_internal_paths(
        f"drs://drs.local/{DRS_VCF_ID}",
        f"http://drs.local/objects/{DRS_IDX_ID}",
    ) is None

    # No responses set up
    assert drs_vcf_to_internal_paths(
        f"drs://drs.local/{DRS_VCF_ID}",
        f"drs://drs.local/{DRS_IDX_ID}",
    ) is None

    # TODO: VALID CASE
