import json

from chord_variant_service.pool import get_pool, teardown_pool
from chord_variant_service.tables.memory import MemoryTableManager

from .shared_data import VARIANT_1, VARIANT_4, VARIANT_5


QUERY_FRAGMENT_1 = ["#ge", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_2 = ["#gt", ["#resolve", "start"], "4999"]
QUERY_FRAGMENT_3 = ["#gt", ["#resolve", "start"], "7001"]
QUERY_FRAGMENT_4 = ["#le", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_5 = ["#lt", ["#resolve", "start"], "5001"]
QUERY_FRAGMENT_6 = ["#lt", ["#resolve", "start"], "5000"]
QUERY_FRAGMENT_7 = ["#lt", ["#resolve", "end"], "5002"]
QUERY_FRAGMENT_8 = ["#le", ["#resolve", "end"], "5001"]
QUERY_FRAGMENT_9 = ["#eq", ["#resolve", "start"], "7000"]
QUERY_FRAGMENT_10 = ["#eq", ["#resolve", "start"], "7001"]

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
QUERY_13 = ["#and", QUERY_1, QUERY_FRAGMENT_9]
QUERY_14 = ["#and", QUERY_1, QUERY_FRAGMENT_10]

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
    (QUERY_13, True),
    (QUERY_14, True),
)

TEST_PRIVATE_QUERIES = (
    (QUERY_1, 3),
    (QUERY_2, 3),
    (QUERY_3, 3),
    (QUERY_4, 0),
    (QUERY_5, 1),
    (QUERY_6, 1),
    (QUERY_7, 0),
    (QUERY_8, 1),
    (QUERY_9, 1),
    (QUERY_10, 0),
    (QUERY_11, 1),
    (QUERY_12, 1),
    (QUERY_13, 1),
    (QUERY_14, 1),
)


def test_chord_variant_search(app, client, table_manager):
    with app.app_context():
        pool = get_pool()

        try:
            mm: MemoryTableManager = table_manager

            # Create a new table with ID fixed_id and name test
            table = mm.create_table_and_update("test", {})

            table.variant_store.append(VARIANT_1)
            table.variant_store.append(VARIANT_4)
            table.variant_store.append(VARIANT_5)

            for rv in (client.post("/search"),
                       client.get("/search"),
                       client.post("/private/search"),
                       client.get("/private/search")):
                assert rv.status_code == 400

            rv = client.post("/search", json={})
            rv2 = client.post("/private/search", json={})
            assert rv.status_code == 400 and rv2.status_code == 400

            for rv in (client.post("/search", json=["data_type", "query"]),
                       client.post("/private/search", json=["data_type", "query"])):
                assert rv.status_code == 400

            for rv in (client.post("/search", json={"data_type": "variant"}),
                       client.get("/search", query_string=json.dumps({"data_type": "variant"})),
                       client.post("/private/search", json={"data_type": "variant"}),
                       client.get("/private/search", query_string={"data_type": "variant"})):
                assert rv.status_code == 400

            for rv in (client.post("/search", json={"query": QUERY_1}),
                       client.get("/search", query_string={"query": QUERY_1}),
                       client.post("/private/search", json={"query": QUERY_1}),
                       client.get("/private/search", query_string={"query": QUERY_1})):
                assert rv.status_code == 400

            for rv in (client.get("/search", query_string={"data_type": "variant", "query": "[5, 6, 7"}),
                       client.get("/private/search", query_string={"data_type": "variant", "query": "[5, 6, 7"})):
                assert rv.status_code == 400

            # Test table search

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

            rv = client.post("/search", json={"data_type": "variant", "query": QUERY_FRAGMENT_1})
            assert rv.status_code == 200
            data = rv.get_json()
            assert "results" in data
            assert len(data["results"]) == 1

            for q, r in TEST_QUERIES:
                for rv in (client.post("/search", json={"data_type": "variant", "query": q}),
                           client.get("/search", query_string={"data_type": "variant", "query": json.dumps(q)})):
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
            assert len(data["results"]["fixed_id"]["matches"]) == 3
            assert json.dumps(data["results"]["fixed_id"]["matches"][0], sort_keys=True) == \
                json.dumps(VARIANT_1.as_chord_representation(), sort_keys=True)

            # Test private table search

            rv = client.post("/private/tables/dne/search", json={"query": QUERY_1})
            assert rv.status_code == 404

            rv = client.post("/private/tables/fixed_id/search")
            assert rv.status_code == 400

            rv = client.post("/private/tables/fixed_id/search", json={})
            assert rv.status_code == 400

            rv = client.post("/private/tables/fixed_id/search", json=["query"])
            assert rv.status_code == 400

            rv = client.post("/private/tables/fixed_id/search", json={"query": QUERY_1})
            assert rv.status_code == 200

            data = rv.get_json()
            assert "results" in data

            assert len(data["results"]) == 3
            assert json.dumps(data["results"][0], sort_keys=True) == json.dumps(VARIANT_1.as_chord_representation(),
                                                                                sort_keys=True)

            for q, r in TEST_PRIVATE_QUERIES:
                qj = {"query": q}

                rv = client.post("/tables/fixed_id/search", json=qj)
                assert rv.status_code == 200
                data = rv.get_json()
                assert data == (r > 0)

                rv = client.post("/private/tables/fixed_id/search", json=qj)
                assert rv.status_code == 200
                data = rv.get_json()
                assert "results" in data
                assert len(data["results"]) == r

        finally:
            teardown_pool(None)
            pool.join()
