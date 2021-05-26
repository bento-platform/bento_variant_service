import os
import responses

from bento_variant_service.tables.vcf.drs_manager import DRSVCFTableManager
from bento_variant_service.tables.vcf.drs_utils import drs_vcf_to_internal_paths

from .shared_data import (
    VCF_TEN_VAR_FILE_PATH,
    VCF_TEN_VAR_INDEX_FILE_PATH,

    DRS_VCF_ID,
    DRS_IDX_ID,
    DRS_VCF_RESPONSE,
    DRS_IDX_RESPONSE,
)


# noinspection PyUnusedLocal
def test_drs_vcf_to_internal_paths_errors(client_drs_mode, drs_table_manager: DRSVCFTableManager):
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


# noinspection PyUnusedLocal
@responses.activate
def test_drs_vcf_to_internal_paths_404(client_drs_mode, drs_table_manager: DRSVCFTableManager):
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_VCF_ID}", json={
        "status_code": 404,
        "message": "Not found",
    }, status=404)
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_IDX_ID}", json={
        "status_code": 404,
        "message": "Not found",
    }, status=404)

    assert drs_vcf_to_internal_paths(
        f"drs://drs.local/{DRS_VCF_ID}",
        f"drs://drs.local/{DRS_IDX_ID}",
    ) is None


# noinspection PyUnusedLocal
@responses.activate
def test_drs_vcf_to_internal_paths_valid(client_drs_mode, drs_table_manager: DRSVCFTableManager):
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_VCF_ID}", json=DRS_VCF_RESPONSE)
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_IDX_ID}", json=DRS_IDX_RESPONSE)

    r = drs_vcf_to_internal_paths(
        f"drs://drs.local/{DRS_VCF_ID}",
        f"drs://drs.local/{DRS_IDX_ID}",
    )

    assert r[:2] == (os.path.abspath(VCF_TEN_VAR_FILE_PATH), os.path.abspath(VCF_TEN_VAR_INDEX_FILE_PATH))
    assert r[2] is None
    assert r[3] is None
