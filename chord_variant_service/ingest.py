import os
import shutil

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
from chord_lib.workflows import workflow_exists
from flask import Blueprint, current_app, request
from jsonschema import validate, ValidationError

from chord_variant_service.constants import DATA_PATH
from chord_variant_service.table_manager import MANAGER_TYPE_VCF, get_table_manager
from chord_variant_service.workflows import WORKFLOWS


bp_ingest = Blueprint("ingest", __name__)


def get_ingest_metadata_from_request():
    workflow_id = request.json["workflow_id"].strip()
    workflow_metadata = request.json["workflow_metadata"]
    workflow_outputs = request.json["workflow_outputs"]
    workflow_params = request.json["workflow_params"]
    return workflow_id, workflow_metadata, workflow_outputs, workflow_params


def move_ingest_files(table_id: str):
    workflow_id, workflow_metadata, workflow_outputs, workflow_params = get_ingest_metadata_from_request()

    output_params = make_output_params(workflow_id, workflow_params, workflow_metadata["inputs"])
    prefix = find_common_prefix(os.path.join(DATA_PATH, table_id), workflow_metadata, output_params)

    def ingest_file_path(f):
        # Full path to to-be-newly-ingested file
        #  - Rename file if a duplicate name exists (ex. dup.vcf.gz becomes 1_dup.vcf.gz)
        #  - If prefix is None, it will not be added
        return os.path.join(DATA_PATH, table_id, file_with_prefix(f, prefix))

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

        if table_id not in get_table_manager().get_tables():
            return flask_errors.flask_bad_request_error(f"No table with ID: {table_id}")

        workflow_id, workflow_metadata, workflow_outputs, _ = get_ingest_metadata_from_request()

        if not workflow_exists(workflow_id, WORKFLOWS):
            # Check that the workflow exists here...
            return flask_errors.flask_bad_request_error(f"No workflow with ID: {workflow_id}")

        # TODO: Customize to table manager specifics properly
        # TODO: Make sure DRS support works
        # TODO: Support memory variant loading (from VCFs?)

        for output in workflow_metadata["outputs"]:
            if output["id"] not in workflow_outputs:
                # Missing output
                print("Missing {} in {}".format(output["id"], workflow_outputs))
                return current_app.response_class(status=400)

        if current_app.config["TABLE_MANAGER"] == MANAGER_TYPE_VCF:
            move_ingest_files(table_id)

        get_table_manager().update_tables()

        return current_app.response_class(status=204)

    except (ValidationError, ValueError):  # UUID, or JSON schema failure TODO: More detailed error messages
        return flask_errors.flask_bad_request_error("Validation error")
