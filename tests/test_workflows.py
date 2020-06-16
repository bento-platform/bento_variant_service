import json

from bento_variant_service.workflows import WORKFLOWS


def test_workflows():
    # TODO: Schema
    assert "ingestion" in WORKFLOWS
    assert "analysis" in WORKFLOWS


def test_workflow_list(client):
    rv = client.get("/workflows")
    data = rv.get_json()

    assert json.dumps(data, sort_keys=True) == json.dumps(WORKFLOWS, sort_keys=True)


def test_workflow_detail(client):
    for wi, wv in WORKFLOWS["ingestion"].items():
        rv = client.get(f"/workflows/{wi}")
        data = rv.get_json()

        assert json.dumps(data, sort_keys=True) == json.dumps(WORKFLOWS["ingestion"][wi], sort_keys=True)

    for wa, wv in WORKFLOWS["analysis"].items():
        rv = client.get(f"/workflows/{wa}")
        data = rv.get_json()

        assert json.dumps(data, sort_keys=True) == json.dumps(WORKFLOWS["ingestion"][wa], sort_keys=True)

    rv_dne = client.get("/workflows/does_not_exist")
    assert rv_dne.status_code == 404


def test_workflow_wdl(client):
    for wi, wv in WORKFLOWS["ingestion"].items():
        rv = client.get(f"/workflows/{wi}.wdl")
        data = rv.data

        # TODO: Better test for WDL validity
        assert bytes(f"workflow {wi}", encoding="utf-8") in data

    for wa, wv in WORKFLOWS["analysis"].items():
        rv = client.get(f"/workflows/{wa}.wdl")
        data = rv.data

        # TODO: Better test for WDL validity
        assert bytes(f"workflow {wa}", encoding="utf-8") in data

    rv_dne = client.get("/workflows/does_not_exist.wdl")
    assert rv_dne.status_code == 404
