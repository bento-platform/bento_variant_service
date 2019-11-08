import chord_lib
import os
import shutil
import uuid

from flask import Blueprint, current_app, request
from jsonschema import validate, ValidationError

from .datasets import DATA_PATH, table_manager
from .workflows import WORKFLOWS


bp_ingest = Blueprint("ingest", __name__)


# Ingest files into datasets
# Ingestion doesn't allow uploading files directly, it simply moves them from a different location on the filesystem.
@bp_ingest.route("/ingest", methods=["POST"])
def ingest():
    try:
        validate(request.json, chord_lib.schemas.chord.CHORD_INGEST_SCHEMA)

        dataset_id = request.json["dataset_id"]

        assert dataset_id in table_manager.get_datasets()
        dataset_id = str(uuid.UUID(dataset_id))  # Check that it's a valid UUID and normalize it to UUID's str format.

        workflow_id = request.json["workflow_id"].strip()
        workflow_metadata = request.json["workflow_metadata"]
        workflow_outputs = request.json["workflow_outputs"]
        workflow_params = request.json["workflow_params"]

        assert chord_lib.workflows.workflow_exists(workflow_id, WORKFLOWS)  # Check that the workflow exists here...

        output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                               workflow_metadata["inputs"])

        prefix = chord_lib.ingestion.find_common_prefix(os.path.join(DATA_PATH, dataset_id), workflow_metadata,
                                                        output_params)

        # TODO: Customize to table manager specifics

        for output in workflow_metadata["outputs"]:
            if output["id"] not in workflow_outputs:
                # Missing output
                print("Missing {} in {}".format(output["id"], workflow_outputs))
                return current_app.response_class(status=400)

            if output["type"] == chord_lib.ingestion.WORKFLOW_TYPE_FILE:
                # Full path to to-be-newly-ingested file
                file_path = os.path.join(DATA_PATH, dataset_id,
                                         chord_lib.ingestion.formatted_output(output, output_params))

                # Rename file if a duplicate name exists (ex. dup.vcf.gz becomes 1_dup.vcf.gz)
                if prefix is not None:
                    file_path = os.path.join(DATA_PATH, dataset_id, chord_lib.ingestion.file_with_prefix(
                        chord_lib.ingestion.formatted_output(output, output_params), prefix))

                # Move the file from its temporary location to its location in the service's data folder.
                shutil.move(workflow_outputs[output["id"]], file_path)

        table_manager.update_datasets()

        return current_app.response_class(status=204)

    except (AssertionError, ValidationError, ValueError):  # assertion, UUID, or JSON schema failure
        # TODO: Better errors
        print("Assertion / validation error")
        return current_app.response_class(status=400)
