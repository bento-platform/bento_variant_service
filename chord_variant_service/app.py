import chord_variant_service
import os
import sys
import traceback

from chord_lib.responses.flask_errors import *
from flask import Flask, jsonify
from werkzeug.exceptions import BadRequest, NotFound

from .beacon import bp_beacon
from .tables import DATA_PATH, VCFTableManager, bp_tables
from .ingest import bp_ingest
from .pool import teardown_pool
from .search import bp_chord_search
from .workflows import bp_workflows


application = Flask(__name__)

# TODO: How to share this across processes?
table_manager = VCFTableManager(DATA_PATH)
application.config["TABLE_MANAGER"] = table_manager
table_manager.update_tables()

application.register_blueprint(bp_beacon)
application.register_blueprint(bp_chord_search)
application.register_blueprint(bp_ingest)
application.register_blueprint(bp_tables)
application.register_blueprint(bp_workflows)


# TODO: Figure out common pattern and move to chord_lib

def _wrap_tb(func):  # pragma: no cover
    # TODO: pass exception?
    def handle_error(_e):
        print("[CHORD Variant Service] Encountered error:", file=sys.stderr)
        traceback.print_exc()
        return func()
    return handle_error


def _wrap(func):  # pragma: no cover
    return lambda _e: func()


application.register_error_handler(Exception, _wrap_tb(flask_internal_server_error))  # Generic catch-all
application.register_error_handler(BadRequest, _wrap(flask_bad_request_error))
application.register_error_handler(NotFound, _wrap(flask_not_found_error))


SERVICE_TYPE = "ca.c3g.chord:variant:{}".format(chord_variant_service.__version__)
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)


@application.teardown_appcontext
def app_teardown_pool(err):
    teardown_pool(err)


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

    return jsonify({
        "id": SERVICE_ID,
        "name": "CHORD Variant Service",  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Variant service for a CHORD application.",
        "organization": {
            "name": "C3G",
            "url": "http://c3g.ca"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__
    })
