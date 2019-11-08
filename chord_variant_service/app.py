import chord_variant_service
import os

from flask import Flask, jsonify

from .beacon import bp_beacon
from .datasets import DATA_PATH, VCFTableManager, bp_datasets, download_example_datasets
from .ingest import bp_ingest
from .pool import teardown_pool
from .search import bp_chord_search
from .workflows import bp_workflows


application = Flask(__name__)

# TODO: How to share this across processes?
table_manager = VCFTableManager(DATA_PATH)
application.config["TABLE_MANAGER"] = table_manager
table_manager.update_datasets()
if len(table_manager.datasets.keys()) == 0 and os.environ.get("DEMO_DATA", "") != "":  # pragma: no cover
    download_example_datasets(table_manager)

application.register_blueprint(bp_beacon)
application.register_blueprint(bp_chord_search)
application.register_blueprint(bp_datasets)
application.register_blueprint(bp_ingest)
application.register_blueprint(bp_workflows)


@application.teardown_appcontext
def app_teardown_pool(err):
    teardown_pool(err)


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

    return jsonify({
        "id": "ca.distributedgenomics.chord_variant_service",  # TODO: Should be globally unique?
        "name": "CHORD Variant Service",                       # TODO: Should be globally unique?
        "type": "ca.distributedgenomics:chord_variant_service:{}".format(chord_variant_service.__version__),  # TODO
        "description": "Variant service for a CHORD application.",
        "organization": {
            "name": "GenAP",
            "url": "https://genap.ca/"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__
    })
