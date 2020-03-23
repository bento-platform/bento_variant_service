def make_ingest(dataset_id, workflow_id="", workflow_outputs=None, workflow_params=None):
    return {
        "table_id": dataset_id,
        "workflow_id": workflow_id,
        "workflow_outputs": workflow_outputs or {},
        "workflow_params": workflow_params or {}
    }


TEST_HEADERS = {"X-User": "test", "X-User-Role": "owner"}


def test_ingest(client):
    # TODO: Test errors better

    # No ingest body
    rv = client.post("/private/ingest", json={}, headers=TEST_HEADERS)
    assert rv.status_code == 400

    # Non-existant table
    rv = client.post("/private/ingest", json=make_ingest("invalid_table_id"), headers=TEST_HEADERS)
    assert rv.status_code == 400

    # Create a dummy table
    client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })

    # Invalid workflow ID
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "invalid_workflow_id"), headers=TEST_HEADERS)
    assert rv.status_code == 400

    # Valid workflow ID, missing workflow output body
    rv = client.post("/private/ingest", json=make_ingest("fixed_id", "vcf_gz"), headers=TEST_HEADERS)
    assert rv.status_code == 400

    # TODO: Test workflow ID validation - analysis vs ingestion
    # TODO: Test workflow parameters / outputs
    # TODO: Test functional ingestion
