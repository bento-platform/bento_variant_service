import json
import os
import shutil

from jsonschema import validate
from typing import Optional, Tuple

from bento_variant_service.tables.memory import MemoryTableManager
from bento_variant_service.tables.vcf.vcf_manager import VCFTableManager
from bento_variant_service.variants.schemas import VARIANT_TABLE_METADATA_SCHEMA, VARIANT_SCHEMA

from .shared_data import (
    VCF_ONE_VAR_FILE_PATH,
    VCF_ONE_VAR_INDEX_FILE_PATH,

    VCF_TEN_VAR_FILE_PATH,
    VCF_TEN_VAR_INDEX_FILE_PATH,

    VCF_MISSING_9_FILE_PATH,
    VCF_MISSING_9_INDEX_FILE_PATH,

    VCF_NO_TBI_FILE_PATH,

    VARIANT_1,
    VARIANT_2,
    VARIANT_3,
)

DATASET_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "metadata": VARIANT_TABLE_METADATA_SCHEMA,
        "schema": {"$ref": "http://json-schema.org/draft-07/schema#"}
    },
    "required": ["id", "schema"]
}


def test_tables(client):
    rv = client.get("/tables")
    assert rv.status_code == 400

    rv = client.get("/tables?data-type=variant")
    data = rv.get_json()

    assert isinstance(data, list) and len(data) == 0

    rv = client.post("/tables?data-type=variant")
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=invalid_data_type")
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    data = rv.get_json()

    assert rv.status_code == 201
    validate(data, DATASET_SCHEMA)

    rv = client.post("/tables?data-type=variant", json=["name", "metadata"])
    assert rv.status_code == 400  # Not an object

    rv = client.post("/tables?data-type=variant", json={"metadata": {}})
    assert rv.status_code == 400  # No name

    rv = client.post("/tables?data-type=variant", json={"name": "test table"})
    assert rv.status_code == 400  # No metadata

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": None
    })
    assert rv.status_code == 400

    rv = client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })
    assert rv.status_code == 500  # No IDs left


def test_table_detail(client):
    client.post("/tables?data-type=variant", json={
        "name": "test table",
        "metadata": {}
    })

    rv = client.get("/tables/none")
    assert rv.status_code == 404

    rv = client.get("/tables/fixed_id")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data, sort_keys=True) == json.dumps({
        "id": "fixed_id",
        "name": "test table",
        "metadata": {},
        "data_type": "variant",
        "schema": VARIANT_SCHEMA,
        "assembly_ids": ["GRCh37"],
    }, sort_keys=True)

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 204

    rv = client.delete("/tables/fixed_id")
    assert rv.status_code == 404


def test_table_summary(client, table_manager):
    mm: MemoryTableManager = table_manager

    # Create a new table with ID fixed_id and name test
    table = mm.create_table_and_update("test", {})
    table.variant_store.append(VARIANT_1)
    table.variant_store.append(VARIANT_2)
    table.variant_store.append(VARIANT_3)

    rv = client.get("/tables/none/summary")
    assert rv.status_code == 404

    rv = client.get("/tables/fixed_id/summary")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data, sort_keys=True) == json.dumps({
        "count": 3,
        "data_type_specific": {
            "samples": 1,
        }
    }, sort_keys=True)


def test_table_data(client, table_manager):
    mm: MemoryTableManager = table_manager

    # Create a new table with ID fixed_id and name test
    table = mm.create_table_and_update("test", {})
    table.variant_store.append(VARIANT_1)
    table.variant_store.append(VARIANT_2)
    table.variant_store.append(VARIANT_3)

    rv = client.get("/private/tables/none/data")
    assert rv.status_code == 404

    rv = client.get("/private/tables/fixed_id/data")
    assert rv.status_code == 200
    data = rv.get_json()
    # TODO: Test schema
    assert json.dumps(data["schema"], sort_keys=True) == json.dumps(VARIANT_SCHEMA, sort_keys=True)
    assert data["pagination"]["previous_page_url"] is None
    assert data["pagination"]["next_page_url"] is None
    assert json.dumps(data["data"], sort_keys=True) == json.dumps([
        VARIANT_1.as_chord_representation(),
        VARIANT_2.as_chord_representation(),
        VARIANT_3.as_chord_representation(),
    ], sort_keys=True)


SHARED_QUERY_STRINGS_AND_RESULTS = (
    ({"offset": "not_an_int"}, 400, None),
    ({"count": "not_an_int"}, 400, None),
    ({"count": -1}, 400, None),
    ({"offset": -1}, 400, None),
    ({"offset": 3, "count": 0}, 400, None),
)

MEMORY_QUERY_STRINGS_AND_RESULTS: Tuple[Tuple[dict, int, Optional[Tuple[bool, bool, list]]], ...] = (
    *SHARED_QUERY_STRINGS_AND_RESULTS,
    ({"offset": 1}, 200, (True, False, [
        VARIANT_2.as_chord_representation(),
        VARIANT_3.as_chord_representation(),
    ])),
    ({"offset": 1, "count": 1}, 200, (True, True, [VARIANT_2.as_chord_representation()])),
    ({"count": 1}, 200, (False, True, [VARIANT_1.as_chord_representation()])),
    ({"offset": 2, "count": 1}, 200, (True, False, [VARIANT_3.as_chord_representation()])),
    ({"offset": 3, "count": 1}, 200, (True, False, [])),
    ({"only_interesting": "false"}, 200, (False, False, [
        VARIANT_1.as_chord_representation(),
        VARIANT_2.as_chord_representation(),
        VARIANT_3.as_chord_representation(),
    ])),
    ({"only_interesting": "true"}, 200, (False, False, [
        VARIANT_1.as_chord_representation(),
        VARIANT_2.as_chord_representation(),
    ])),
)


VCF_NUM_INTERESTING_CALLS = (1, 12, 9, 2, 1, 240, 9, 11, 5, 2)
VCF_QUERY_STRINGS_AND_RESULTS = (
    *SHARED_QUERY_STRINGS_AND_RESULTS,
    ({"only_interesting": "false"}, 200, (False, False, 10, (835,) * 10)),
    ({"only_interesting": "true"}, 200, (False, False, 10, VCF_NUM_INTERESTING_CALLS)),
    ({"offset": 1}, 200, (True, False, 9, (835,) * 9)),
    ({"offset": 1, "only_interesting": "true"}, 200, (True, False, 9, VCF_NUM_INTERESTING_CALLS[1:])),
    ({"offset": 0, "count": 3}, 200, (False, True, 3, (835,) * 3)),
    ({"offset": 1, "count": 3,  "only_interesting": "true"}, 200, (True, True, 3, VCF_NUM_INTERESTING_CALLS[1:4])),
)


def _check_pagination(prev_page: bool, next_page: bool, data: dict):
    if prev_page:
        assert data["pagination"]["previous_page_url"] is not None
    else:
        assert data["pagination"]["previous_page_url"] is None

    if next_page:
        assert data["pagination"]["next_page_url"] is not None
    else:
        assert data["pagination"]["next_page_url"] is None


def test_memory_table_data_pagination(client, table_manager):
    mm: MemoryTableManager = table_manager

    # Create a new table with ID fixed_id and name test
    table = mm.create_table_and_update("test", {})
    table.variant_store.append(VARIANT_1)
    table.variant_store.append(VARIANT_2)
    table.variant_store.append(VARIANT_3)

    for q, sc, r in MEMORY_QUERY_STRINGS_AND_RESULTS:
        rv = client.get("/private/tables/fixed_id/variants", query_string=q)
        assert rv.status_code == sc

        rv = client.get("/private/tables/fixed_id/data", query_string=q)
        assert rv.status_code == sc

        if r is not None:
            data = rv.get_json()
            prev_page, next_page, r_data = r
            _check_pagination(prev_page, next_page, data)
            assert json.dumps(data["data"], sort_keys=True) == json.dumps(r_data, sort_keys=True)


def test_vcf_table_error_handling(vcf_table_manager):
    vm: VCFTableManager = vcf_table_manager

    # Create a new table named test
    t = vm.create_table_and_update("test", {})

    shutil.copyfile(VCF_ONE_VAR_FILE_PATH, os.path.join(vm.data_path, t.table_id, "test.vcf.gz"))
    shutil.copyfile(VCF_ONE_VAR_INDEX_FILE_PATH, os.path.join(vm.data_path, t.table_id, "test.vcf.gz.tbi"))

    shutil.copyfile(VCF_MISSING_9_FILE_PATH, os.path.join(vm.data_path, t.table_id, "missing_9.vcf.gz"))
    shutil.copyfile(VCF_MISSING_9_INDEX_FILE_PATH, os.path.join(vm.data_path, t.table_id, "missing_9.vcf.gz.tbi"))

    shutil.copyfile(VCF_NO_TBI_FILE_PATH, os.path.join(vm.data_path, t.table_id, "no_tbi.vcf.gz"))

    # Update to register new files
    vm.update_tables()

    assert len(t.files) == 1
    assert t.n_of_variants == 1
    assert t.n_of_samples == 835

    assert len(tuple(t.variants())) == 1
    assert len(tuple(t.variants(chromosome="22"))) == 1
    assert len(tuple(t.variants(chromosome="21"))) == 0
    assert len(tuple(t.variants(offset=1))) == 0


# noinspection DuplicatedCode
def test_vcf_table_pagination(vcf_client, vcf_table_manager):
    vm: VCFTableManager = vcf_table_manager

    # Create a new table named test
    t = vm.create_table_and_update("test", {})

    # Add ten_variants_22.vcf.gz
    shutil.copyfile(VCF_TEN_VAR_FILE_PATH, os.path.join(vm.data_path, t.table_id, "test.vcf.gz"))
    shutil.copyfile(VCF_TEN_VAR_INDEX_FILE_PATH, os.path.join(vm.data_path, t.table_id, "test.vcf.gz.tbi"))

    # Update to register new files
    vm.update_tables()

    for q, sc, r in VCF_QUERY_STRINGS_AND_RESULTS:
        rv = vcf_client.get(f"/private/tables/{t.table_id}/variants", query_string=q)
        assert rv.status_code == sc

        rv = vcf_client.get(f"/private/tables/{t.table_id}/data", query_string=q)
        assert rv.status_code == sc

        if r is not None:
            data = rv.get_json()
            prev_page, next_page, r_len, c_lens = r
            _check_pagination(prev_page, next_page, data)
            assert len(data["data"]) == r_len
            assert tuple(len(d["calls"]) for d in data["data"]) == c_lens

    assert len(tuple(t.variants(start_min=16050607))) == 7  # inclusive min
    assert len(tuple(t.variants(start_max=16050627))) == 4  # exclusive max
    assert len(tuple(t.variants(start_min=16050607, start_max=16050627))) == 1  # "
