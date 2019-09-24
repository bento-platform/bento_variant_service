import chord_lib
import os
import shutil
import uuid

from flask import Blueprint, current_app, json, request

from .datasets import DATA_PATH, datasets, update_datasets


bp_ingest = Blueprint("ingest", __name__)


# Ingest files into datasets
# Ingestion doesn't allow uploading files directly, it simply moves them from a different location on the filesystem.
@bp_ingest.route("/ingest", methods=["POST"])
def ingest():
    try:
        assert "dataset_id" in request.form
        assert "workflow_id" in request.form
        assert "workflow_metadata" in request.form
        assert "workflow_outputs" in request.form
        assert "workflow_params" in request.form

        dataset_id = request.form["dataset_id"]  # TODO: WES needs to be able to forward this on...

        assert dataset_id in datasets
        dataset_id = str(uuid.UUID(dataset_id))  # Check that it's a valid UUID and normalize it to UUID's str format.

        workflow_id = request.form["workflow_id"].strip()
        workflow_metadata = json.loads(request.form["workflow_metadata"])
        workflow_outputs = json.loads(request.form["workflow_outputs"])
        workflow_params = json.loads(request.form["workflow_params"])

        output_params = chord_lib.ingestion.make_output_params(workflow_id, workflow_params,
                                                               workflow_metadata["inputs"])

        prefix = chord_lib.ingestion.find_common_prefix(os.path.join(DATA_PATH, dataset_id), workflow_metadata,
                                                        output_params)

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

        update_datasets()

        return current_app.response_class(status=204)

    except (AssertionError, ValueError):  # assertion or JSON conversion failure
        # TODO: Better errors
        print("Assertion or value error")
        return current_app.response_class(status=400)
