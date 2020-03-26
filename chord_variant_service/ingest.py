import json
import os
import shutil
import sys

from base64 import urlsafe_b64encode
from chord_lib.ingestion import (
    WORKFLOW_TYPE_FILE,
    WORKFLOW_TYPE_FILE_ARRAY,

    find_common_prefix,
    file_with_prefix,
    formatted_output,
    make_output_params
)
from chord_lib.responses import flask_errors
from chord_lib.schemas.chord import CHORD_INGEST_SCHEMA
from chord_lib.workflows import get_workflow, workflow_exists
from flask import Blueprint, current_app, request
from jsonschema import validate, ValidationError
from typing import List, Tuple
from urllib.parse import urlparse

from chord_variant_service.constants import SERVICE_NAME
from chord_variant_service.table_manager import (
    MANAGER_TYPE_DRS,
    MANAGER_TYPE_MEMORY,
    get_table_manager,
)
from chord_variant_service.workflows import WORKFLOWS


bp_ingest = Blueprint("ingest", __name__)


WORKFLOW_OUTPUT_VCF_GZ_FILES = "vcf_gz_files"
WORKFLOW_OUTPUT_TBI_FILES = "tbi_files"


MEMORY_CANNOT_INGEST_ERROR = "Cannot ingest into a memory-based table manager"


def get_ingest_metadata_from_request(request_data):
    workflow_id = request_data["workflow_id"].strip()
    workflow_outputs = request_data["workflow_outputs"]
    workflow_params = request_data["workflow_params"]
    return workflow_id, workflow_outputs, workflow_params


def write_drs_object_files(table_id: str, request_data: dict):  # pragma: no cover
    workflow_outputs = get_ingest_metadata_from_request(request_data)[2]
    table_path = os.path.join(current_app.config["DATA_PATH"], table_id)

    # Fetch the relevant files from the workflows (these should be DRS URLs...)
    vcfs: List[str] = workflow_outputs.get(WORKFLOW_OUTPUT_VCF_GZ_FILES, [])
    tbis: List[str] = workflow_outputs.get(WORKFLOW_OUTPUT_TBI_FILES, [])

    if len(vcfs) != len(tbis):
        print(f"[{SERVICE_NAME}] Mismatched VCF GZ and TBI array lengths: {len(vcfs)}, {len(tbis)}", file=sys.stderr,
              flush=True)
        return

    pairs: Tuple[Tuple[str, str], ...] = tuple(zip(vcfs, tbis))

    for vcf_url, idx_url in pairs:
        v = urlparse(vcf_url)
        i = urlparse(idx_url)
        if v.scheme != "drs" or i.scheme != "drs":
            print(f"[{SERVICE_NAME}] Invalid DRS URL: {vcf_url} or {idx_url}", file=sys.stderr, flush=True)
            continue

        # TODO: Check hashes for duplicate files

        drs_object_file_path = os.path.join(table_path, f"{urlsafe_b64encode(vcf_url)}.drs.json")
        if os.path.exists(drs_object_file_path):
            print(f"[{SERVICE_NAME}] DRS object already exists in table {table_id}: {drs_object_file_path}",
                  file=sys.stderr, flush=True)
            continue

        with open(drs_object_file_path, "w") as df:
            print(f"[{SERVICE_NAME}] Writing DRS object to {drs_object_file_path}", flush=True)
            json.dump({"data": vcf_url, "index": idx_url}, df)


def move_ingest_files(table_id: str, request_data: dict):
    workflow_id, workflow_outputs, workflow_params = get_ingest_metadata_from_request(request_data)
    workflow_metadata = get_workflow(workflow_id, WORKFLOWS)

    table_path = os.path.join(current_app.config["DATA_PATH"], table_id)
    output_params = make_output_params(workflow_id, workflow_params, workflow_metadata["inputs"])
    prefix = find_common_prefix(table_path, workflow_metadata, output_params)

    def ingest_file_path(f):
        # Full path to to-be-newly-ingested file
        #  - Rename file if a duplicate name exists (ex. dup.vcf.gz becomes 1_dup.vcf.gz)
        #  - If prefix is None, it will not be added
        return os.path.join(table_path, file_with_prefix(f, prefix))

    files_to_move = []

    for output in workflow_metadata["outputs"]:
        if output["type"] == WORKFLOW_TYPE_FILE:
            files_to_move.append((workflow_outputs[output["id"]],
                                  ingest_file_path(formatted_output(output, output_params))))

        elif output["type"] == WORKFLOW_TYPE_FILE_ARRAY:
            files_to_move.extend(zip(
                workflow_outputs[output["id"]],
                map(ingest_file_path, formatted_output(output, output_params))
            ))

    for tmp_file_path, file_path in files_to_move:
        # Move the file from its temporary location to its location in the service's data folder.
        shutil.move(tmp_file_path, file_path)


# Ingest files into tables
# Ingestion doesn't allow uploading files directly, it simply moves them from a different location on the filesystem.
@bp_ingest.route("/private/ingest", methods=["POST"])
def ingest():
    try:
        validate(request.json, CHORD_INGEST_SCHEMA)

        table_id = request.json["table_id"]

        if table_id not in get_table_manager().tables:
            return flask_errors.flask_bad_request_error(f"No table with ID: {table_id}")

        workflow_id, workflow_outputs, _ = get_ingest_metadata_from_request(request.json)

        if not workflow_exists(workflow_id, WORKFLOWS):
            # Check that the workflow exists here...
            return flask_errors.flask_bad_request_error(f"No workflow with ID: {workflow_id}")

        workflow_metadata = get_workflow(workflow_id, WORKFLOWS)

        # TODO: Customize to table manager specifics properly
        # TODO: Make sure DRS support works
        # TODO: Support memory variant loading (from VCFs?)

        # TODO: More extensive, standardized, chord_lib-based validation of workflow ingestion data

        if len(workflow_metadata["outputs"]) != len(workflow_outputs):
            return flask_errors.flask_bad_request_error("Output length mismatch")

        for output in workflow_metadata["outputs"]:
            if output["id"] not in workflow_outputs:
                # Missing output
                err = f"Missing output {output['id']} in {workflow_id} (Outputs: {workflow_outputs})"
                print(f"[{SERVICE_NAME}] {err}", file=sys.stderr, flush=True)
                return flask_errors.flask_bad_request_error(err)

        # Check manager type to determine how the ingestion will be handled
        manager_type = current_app.config["TABLE_MANAGER"]
        if manager_type == MANAGER_TYPE_DRS:  # pragma: no cover
            write_drs_object_files(table_id, request.json)
        elif manager_type == MANAGER_TYPE_MEMORY:
            print(f"[{SERVICE_NAME}] Unsupported: {MEMORY_CANNOT_INGEST_ERROR}", file=sys.stderr, flush=True)
            return flask_errors.flask_bad_request_error(MEMORY_CANNOT_INGEST_ERROR)
        else:  # MANAGER_TYPE_VCF
            try:
                move_ingest_files(table_id, request.json)
            except KeyError:  # From make_output_params; TODO: In future may change to custom exception
                return flask_errors.flask_bad_request_error("Bad workflow parameter")

        # After files have been handled, refresh the tables in the manager
        get_table_manager().update_tables()

        return current_app.response_class(status=204)

    except (ValidationError, ValueError):  # UUID, or JSON schema failure TODO: More detailed error messages
        return flask_errors.flask_bad_request_error("Validation error")
