import json

from chord_variant_service.pool import get_pool, teardown_pool
from .shared_data import VARIANT_1


QUERY_FRAGMENT_1 = ["#ge", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_2 = ["#gt", ["#resolve", "start"], "4999"]
QUERY_FRAGMENT_3 = ["#gt", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_4 = ["#le", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_5 = ["#lt", ["#resolve", "start"], "5001"]
QUERY_FRAGMENT_6 = ["#lt", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_7 = ["#lt", ["#resolve", "end"], "5002"]
QUERY_FRAGMENT_8 = ["#le", ["#resolve", "end"], "5001"]

QUERY_1 = ["#eq", ["#resolve", "chromosome"], "1"]
QUERY_2 = ["#and", QUERY_1, QUERY_FRAGMENT_1]
QUERY_3 = ["#and", QUERY_1, QUERY_FRAGMENT_2]
QUERY_4 = ["#and", QUERY_1, QUERY_FRAGMENT_3]
QUERY_5 = ["#and", QUERY_1, QUERY_FRAGMENT_4]
QUERY_6 = ["#and", QUERY_1, QUERY_FRAGMENT_5]
QUERY_7 = ["#and", QUERY_1, QUERY_FRAGMENT_6]
QUERY_8 = ["#and", QUERY_1, ["#and", QUERY_FRAGMENT_1, QUERY_FRAGMENT_4]]
QUERY_9 = ["#and", QUERY_8, ["#eq", ["#resolve", "ref"], "C"]]
QUERY_10 = ["#and", QUERY_8, ["#eq", ["#resolve", "ref"], "T"]]
QUERY_11 = ["#and", QUERY_1, QUERY_FRAGMENT_7]
QUERY_12 = ["#and", QUERY_1, QUERY_FRAGMENT_8]

TEST_QUERIES = (
    (QUERY_1, True),
    (QUERY_2, True),
    (QUERY_3, True),
    (QUERY_4, False),
    (QUERY_5, True),
    (QUERY_6, True),
    (QUERY_7, False),
    (QUERY_8, True),
    (QUERY_9, True),
    (QUERY_10, False),
    (QUERY_11, True),
    (QUERY_12, True),
)


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

            # - Missing chromosome equality

            rv = client.post("/search", json={"data_type": "variant", "query": QUERY_FRAGMENT_1})
            assert rv.status_code == 200

            # TODO: This should be a valid query or an error

            data = rv.get_json()
            assert "results" in data
            assert len(data["results"]) == 0

            # - Invalid data type

            rv = client.post("/search", json={
                "data_type": "whatever",
                "query": ["#eq", ["#resolve", "chromosome"], "1"]
            })
            assert rv.status_code == 200

            # TODO: Error?

            data = rv.get_json()
            assert "results" in data
            assert len(data["results"]) == 0

            # - Invalid chromosome

            rv = client.post("/search", json={
                "data_type": "variant",
                "query": ["#eq", ["#resolve", "chromosome"], "  :invalid:  "]
            })
            assert rv.status_code == 200

            # TODO: Error?

            data = rv.get_json()
            assert "results" in data
            assert len(data["results"]) == 0

            # - Good queries

            for q, r in TEST_QUERIES:
                rv = client.post("/search", json={"data_type": "variant", "query": q})
                assert rv.status_code == 200

                data = rv.get_json()
                assert "results" in data

                if r:
                    assert len(data["results"]) == 1
                    assert data["results"][0]["data_type"] == "variant" and data["results"][0]["id"] == "fixed_id"
                else:
                    assert len(data["results"]) == 0

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
