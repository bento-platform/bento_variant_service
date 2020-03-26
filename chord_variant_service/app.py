import os
import subprocess
import sys

from chord_lib.responses import flask_errors
from flask import Flask, jsonify
from werkzeug.exceptions import BadRequest, NotFound

from chord_variant_service import __version__
from chord_variant_service.beacon.routes import bp_beacon
from chord_variant_service.constants import SERVICE_NAME, SERVICE_TYPE, SERVICE_ID
from chord_variant_service.ingest import bp_ingest
from chord_variant_service.pool import teardown_pool
from chord_variant_service.search import bp_chord_search
from chord_variant_service.tables.routes import bp_tables
from chord_variant_service.table_manager import (
    MANAGER_TYPE_DRS,
    MANAGER_TYPE_MEMORY,
    MANAGER_TYPE_VCF,
    get_table_manager,
    clear_table_manager,
)
from chord_variant_service.workflows import bp_workflows


application = Flask(__name__)
application.config.from_mapping(
    DATA_PATH=os.environ.get("DATA", "data/"),
    INITIALIZE_IMMEDIATELY=os.environ.get("INITIALIZE_IMMEDIATELY", "true").strip().lower() == "true",
    TABLE_MANAGER=os.environ.get("TABLE_MANAGER", MANAGER_TYPE_VCF),  # Options: drs, memory, vcf
)


# Check if we have the required BCFtools if we're starting in DRS or VCF mode
with application.app_context():  # pragma: no cover
    if application.config["TABLE_MANAGER"] not in (MANAGER_TYPE_DRS, MANAGER_TYPE_MEMORY, MANAGER_TYPE_VCF):
        print(f"[{SERVICE_NAME}] Invalid table manager type: {application.config['TABLE_MANAGER']}", file=sys.stderr,
              flush=True)
        exit(1)

    if application.config["TABLE_MANAGER"] != MANAGER_TYPE_MEMORY:
        try:
            subprocess.run(("bcftools", "--version"))
        except FileNotFoundError:
            print(f"[{SERVICE_NAME}] Missing required dependency: bcftools", file=sys.stderr, flush=True)
            exit(1)

    print(f"[{SERVICE_NAME}] Started with table manager mode: {application.config['TABLE_MANAGER']}", flush=True)


application.register_blueprint(bp_beacon)
application.register_blueprint(bp_chord_search)
application.register_blueprint(bp_ingest)
application.register_blueprint(bp_tables)
application.register_blueprint(bp_workflows)


# Generic catch-all
application.register_error_handler(
    Exception,
    flask_errors.flask_error_wrap_with_traceback(
        flask_errors.flask_internal_server_error,
        service_name=SERVICE_NAME
    )
)
application.register_error_handler(BadRequest, flask_errors.flask_error_wrap(flask_errors.flask_bad_request_error))
application.register_error_handler(NotFound, flask_errors.flask_error_wrap(flask_errors.flask_not_found_error))


@application.teardown_appcontext
def app_teardown(err):
    teardown_pool(err)
    clear_table_manager(err)


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info
    return jsonify({
        "id": SERVICE_ID,
        "name": SERVICE_NAME,
        "type": SERVICE_TYPE,
        "description": "Variant service for a CHORD application.",
        "organization": {
            "name": "C3G",
            "url": "http://c3g.ca"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": __version__
    })


def post_start_hook():
    # Force initialization of table manager
    get_table_manager()


with application.app_context():
    if application.config["INITIALIZE_IMMEDIATELY"]:
        print(f"[{SERVICE_NAME}] Post-start hook invoked automatically at startup", flush=True)
        post_start_hook()


# Register a post-start hook to initialize the table manager
# This cannot always happen immediately upon startup, because the DRS service may not have started yet.
@application.route("/private/post-start-hook", methods=["GET"])
def post_start_hook_route():
    print(f"[{SERVICE_NAME}] Post-start hook invoked via URL request", flush=True)
    post_start_hook()
    return application.response_class(status=204)
