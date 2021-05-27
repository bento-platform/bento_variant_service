import os
import shutil
import responses

from bento_variant_service.tables.vcf.vcf_manager import VCFTableManager
from bento_variant_service.tables.vcf.drs_manager import DRSVCFTableManager

from .shared_data import (
    VCF_TEN_VAR_FILE_PATH,
    VCF_TEN_VAR_INDEX_FILE_PATH,

    DRS_VCF_ID,
    DRS_IDX_ID,
    DRS_VCF_RESPONSE,
    DRS_IDX_RESPONSE,
    DRS_404_RESPONSE,
)


def make_ingest(dataset_id, workflow_id="", workflow_outputs=None, workflow_params=None):
    return {
        "table_id": dataset_id,
        "workflow_id": workflow_id,
        "workflow_outputs": workflow_outputs or {},
        "workflow_params": workflow_params or {}
    }


TEST_HEADERS = {"X-User": "test", "X-User-Role": "owner"}

# TODO: Test errors better


def test_ingest_no_body(client):
    # No ingest body
    rv = client.post("/private/ingest", json={}, headers=TEST_HEADERS)
    assert rv.status_code == 400


def test_ingest_bad_table(client):
    # Non-existant table
    rv = client.post("/private/ingest", json=make_ingest("invalid_table_id"), headers=TEST_HEADERS)
    assert rv.status_code == 400


def test_ingest_bad_requests(client):
    # Create a dummy table
    client.post("/tables?data-type=variant", json={"name": "test table", "metadata": {}})

    # Invalid workflow ID
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "invalid_workflow_id"), headers=TEST_HEADERS)
    assert rv.status_code == 400

    # Valid workflow ID, missing workflow output body
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "vcf_gz"), headers=TEST_HEADERS)
    assert rv.status_code == 400

    # Valid workflow ID, unsupported bad workflow output body
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "vcf_gz", {
        "vcf_files": [],  # Should be vcf_gz_files if corrected
        "tbi_files": []
    }), headers=TEST_HEADERS)
    assert rv.status_code == 400


# Force re-initialization of table_manager as a memory manager
# noinspection PyUnusedLocal
def test_ingest_memory(client):
    # Create a dummy table
    client.post("/tables?data-type=variant", json={"name": "test", "metadata": {}})

    # Valid workflow ID, unsupported table manager type for ingestion
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "vcf_gz", {
        "vcf_gz_files": [],
        "tbi_files": []
    }, {
        "vcf_gz.vcf_gz_files": [],
        "vcf_gz.assembly_id": "GRCh38"
    }), headers=TEST_HEADERS)
    assert rv.status_code == 400
    err = rv.get_json()
    assert err["errors"][0]["message"] == "Cannot ingest into a memory-based table manager"
    print(rv.get_json())

    # TODO: Test workflow ID validation - analysis vs ingestion
    # TODO: Test workflow parameters / outputs
    # TODO: Test functional ingestion


EMPTY_WORKFLOW_OUTPUTS = {
    "vcf_gz_files": [],
    "tbi_files": []
}


def _create_dummy_table(client):
    # Create a dummy table
    tr = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    return tr.get_json()


def test_ingest_vcf_missing_prefix(client_vcf_mode):
    t = _create_dummy_table(client_vcf_mode)

    # Valid workflow ID, invalid params (missing prefix)
    rv = client_vcf_mode.post("/private/ingest", json=make_ingest(
        t["id"],
        "vcf_gz",
        workflow_outputs=EMPTY_WORKFLOW_OUTPUTS,
        workflow_params={
            "vcf_gz_files": [],
            "assembly_id": "GRCh38"
        }
    ), headers=TEST_HEADERS)
    assert rv.status_code == 400


def test_ingest_vcf_missing_files(client_vcf_mode):
    t = _create_dummy_table(client_vcf_mode)

    # Valid workflow ID, empty file arrays
    rv = client_vcf_mode.post("/private/ingest", json=make_ingest(
        t["id"],
        "vcf_gz",
        workflow_outputs=EMPTY_WORKFLOW_OUTPUTS,
        workflow_params={
            "vcf_gz.vcf_gz_files": [],
            "vcf_gz.assembly_id": "GRCh38"
        }
    ), headers=TEST_HEADERS)
    assert rv.status_code == 204


def test_ingest_vcf_valid(tmpdir, client_vcf_mode, vcf_table_manager: VCFTableManager):
    t = _create_dummy_table(client_vcf_mode)

    data_path = tmpdir / "data_to_ingest"
    data_path.mkdir()
    data_path = str(data_path)

    # Copy variant files to use
    shutil.copyfile(VCF_TEN_VAR_FILE_PATH, os.path.join(data_path, "test.vcf.gz"))
    shutil.copyfile(VCF_TEN_VAR_INDEX_FILE_PATH, os.path.join(data_path, "test.vcf.gz.tbi"))

    # Valid workflow ID with a file
    rv = client_vcf_mode.post("/private/ingest", json=make_ingest(
        t["id"],
        "vcf_gz",
        workflow_outputs={
            "vcf_gz_files": [os.path.join(data_path, "test.vcf.gz")],
            "tbi_files": [os.path.join(data_path, "test.vcf.gz.tbi")],
        },
        workflow_params={
            "vcf_gz.vcf_gz_files": ["test.vcf.gz"],
            "vcf_gz.assembly_id": "GRCh37"
        }
    ), headers=TEST_HEADERS)
    assert rv.status_code == 204

    assert len(list(vcf_table_manager.get_table(t["id"]).variants())) == 10


@responses.activate
def test_ingest_vcf_valid_drs(client_drs_mode, drs_table_manager: DRSVCFTableManager):
    t = _create_dummy_table(client_drs_mode)

    responses.add(responses.GET, f"http://drs.local/objects/{DRS_VCF_ID}", json=DRS_VCF_RESPONSE)
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_IDX_ID}", json=DRS_IDX_RESPONSE)

    # Valid workflow ID with a file
    rv = client_drs_mode.post("/private/ingest", json=make_ingest(
        t["id"],
        "vcf_gz",
        workflow_outputs={
            "vcf_gz_files": [f"drs://drs.local/{DRS_VCF_ID}"],
            "tbi_files": [f"drs://drs.local/{DRS_IDX_ID}"],
        },
        workflow_params={
            "vcf_gz.vcf_gz_files": ["test.vcf.gz"],
            "vcf_gz.assembly_id": "GRCh37"
        }
    ), headers=TEST_HEADERS)
    assert rv.status_code == 204

    assert len(list(drs_table_manager.get_table(t["id"]).variants())) == 10


@responses.activate
def test_ingest_vcf_404(client_drs_mode, drs_table_manager: DRSVCFTableManager):
    # TODO: This funcionality is as expected but kind of weird, as no error is returned

    t = _create_dummy_table(client_drs_mode)

    responses.add(responses.GET, f"http://drs.local/objects/{DRS_VCF_ID}", json=DRS_404_RESPONSE, status=404)
    responses.add(responses.GET, f"http://drs.local/objects/{DRS_IDX_ID}", json=DRS_404_RESPONSE, status=404)

    # Valid workflow ID with a file
    rv = client_drs_mode.post("/private/ingest", json=make_ingest(
        t["id"],
        "vcf_gz",
        workflow_outputs={
            "vcf_gz_files": [f"drs://drs.local/{DRS_VCF_ID}"],
            "tbi_files": [f"drs://drs.local/{DRS_IDX_ID}"],
        },
        workflow_params={
            "vcf_gz.vcf_gz_files": ["test.vcf.gz"],
            "vcf_gz.assembly_id": "GRCh37"
        }
    ), headers=TEST_HEADERS)
    assert rv.status_code == 204

    # TODO: We expect 0 variants here because the VCFs aren't valid - this is expected, albeit probably not good.
    assert len(list(drs_table_manager.get_table(t["id"]).variants())) == 0
