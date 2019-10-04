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


@bp_workflows.route("/workflows/<string:workflow_name>", methods=["GET"])
def workflow_detail(workflow_name):
    # TODO: Better errors
    if not workflow_exists(workflow_name, WORKFLOWS):
        return current_app.response_class(status=404)

    return jsonify(get_workflow(workflow_name, WORKFLOWS))


@bp_workflows.route("/workflows/<string:workflow_name>.wdl", methods=["GET"])
def workflow_wdl(workflow_name):
    # TODO: Better errors
    if not workflow_exists(workflow_name, WORKFLOWS):
        return current_app.response_class(status=404)

    with current_app.open_resource("workflows/{}".format(get_workflow_resource(workflow_name, WORKFLOWS))) as wfh:
        return current_app.response_class(response=wfh.read(), mimetype="text/plain", status=200)
