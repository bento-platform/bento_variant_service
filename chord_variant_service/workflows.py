from flask import Blueprint, current_app, json, jsonify
from werkzeug.utils import secure_filename

bp_workflows = Blueprint("workflows", __name__)

# TODO: Add validation and then move to chord_lib.ingestion
with bp_workflows.open_resource("workflows/chord_workflows.json") as wf:
    # TODO: Schema
    WORKFLOWS = json.loads(wf.read())


@bp_workflows.route("/workflows", methods=["GET"])
def workflow_list():
    return jsonify(WORKFLOWS)


def workflow_exists(workflow_name):
    return workflow_name in WORKFLOWS["ingestion"] or workflow_name in WORKFLOWS["analysis"]


def get_workflow(workflow_name):
    return (WORKFLOWS["ingestion"][workflow_name] if workflow_name in WORKFLOWS["ingestion"]
            else WORKFLOWS["analysis"][workflow_name])


def get_workflow_resource(workflow_name):
    return "workflows/{}".format(secure_filename(get_workflow(workflow_name)["file"]))


@bp_workflows.route("/workflows/<string:workflow_name>", methods=["GET"])
def workflow_detail(workflow_name):
    # TODO: Better errors
    if not workflow_exists(workflow_name):
        return current_app.response_class(status=404)

    return jsonify(get_workflow(workflow_name))


@bp_workflows.route("/workflows/<string:workflow_name>.wdl", methods=["GET"])
def workflow_wdl(workflow_name):
    # TODO: Better errors
    if not workflow_exists(workflow_name):
        return current_app.response_class(status=404)

    with current_app.open_resource(get_workflow_resource(workflow_name)) as wfh:
        return current_app.response_class(response=wfh.read(), mimetype="text/plain", status=200)
