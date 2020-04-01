from chord_lib.responses import flask_errors
from chord_lib.workflows import get_workflow, get_workflow_resource, workflow_exists
from flask import Blueprint, current_app, json, jsonify

bp_workflows = Blueprint("workflows", __name__)

# TODO: Add validation and then move to chord_lib.ingestion
with bp_workflows.open_resource("workflows/chord_workflows.json") as wf:
    # TODO: Schema
    WORKFLOWS = json.loads(wf.read())


@bp_workflows.route("/workflows", methods=["GET"])
def workflow_list():
    return jsonify(WORKFLOWS)


@bp_workflows.route("/workflows/<string:workflow_id>", methods=["GET"])
def workflow_detail(workflow_id):
    if not workflow_exists(workflow_id, WORKFLOWS):
        return flask_errors.flask_not_found_error(f"No workflow with id {workflow_id}")

    return jsonify(get_workflow(workflow_id, WORKFLOWS))


@bp_workflows.route("/workflows/<workflow_id>.wdl", methods=["GET"])
def workflow_wdl(workflow_id):
    if not workflow_exists(workflow_id, WORKFLOWS):
        return flask_errors.flask_not_found_error(f"No workflow with id {workflow_id}")

    with current_app.open_resource("workflows/{}".format(get_workflow_resource(workflow_id, WORKFLOWS))) as wfh:
        return current_app.response_class(response=wfh.read(), mimetype="text/plain", status=200)
