import json

from chord_variant_service.pool import get_pool, teardown_pool
from .shared_data import VARIANT_1


QUERY_1 = ["#eq", ["#resolve", "chromosome"], "1"]


def test_chord_variant_search(app, client):
    with app.app_context():
        pool = get_pool()

        try:

            mm = app.config["TABLE_MANAGER"]

            # Create a new dataset with ID fixed_id and name test
            ds = mm.create_dataset_and_update("test", {})

            ds.variant_store.append(VARIANT_1)

            rv = client.post("/search")
            rv2 = client.post("/private/search")
            assert rv.status_code == 400 and rv2.status_code == 400

            rv = client.post("/search", json={})
            rv2 = client.post("/private/search", json={})
            assert rv.status_code == 400 and rv2.status_code == 400

            rv = client.post("/search", json={"data_type": "variant"})
            rv2 = client.post("/search", json={"data_type": "variant"})
            assert rv.status_code == 400 and rv2.status_code == 400

            rv = client.post("/search", json={"query": QUERY_1})
            rv2 = client.post("/search", json={"query": QUERY_1})
            assert rv.status_code == 400 and rv2.status_code == 400

            # Test table search

            rv = client.post("/search", json={"data_type": "variant", "query": QUERY_1})
            assert rv.status_code == 200

            data = rv.get_json()
            assert "results" in data
            assert len(data["results"]) == 1
            assert data["results"][0]["data_type"] == "variant" and data["results"][0]["id"] == "fixed_id"

            # Test private search

            rv = client.post("/private/search", json={"data_type": "variant", "query": QUERY_1})
            assert rv.status_code == 200

            data = rv.get_json()
            assert "results" in data
            assert "fixed_id" in data["results"]
            assert len(data["results"]["fixed_id"]["matches"]) == 1
            assert json.dumps(data["results"]["fixed_id"]["matches"][0], sort_keys=True) == \
                json.dumps(VARIANT_1, sort_keys=True)

        finally:
            teardown_pool(None)
            pool.join()
