def make_ingest(dataset_id):
    return {
        "dataset_id": dataset_id,
        "workflow_id": "",  # TODO
        "workflow_metadata": {  # TODO
            "inputs": [],
            "outputs": []
        },
        "workflow_outputs": {  # TODO

        },
        "workflow_params": {  # TODO

        }
    }


def test_ingest(client):
    rv = client.post("/ingest", json={})
    assert rv.status_code == 400

    rv = client.post("/ingest", json=make_ingest("invalid_dataset_id"))
    assert rv.status_code == 400

    # TODO: Test workflow ID validation
    # TODO: Test workflow parameters / outputs
    # TODO: Test functional ingestion
