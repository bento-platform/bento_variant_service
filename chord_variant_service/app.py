import chord_variant_service
import datetime
import os
import pysam

from flask import Flask, json, jsonify, request

VARIANT_SCHEMA = {
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
    "type": "object",
    "required": ["chromosome", "start", "end", "ref", "alt"],
    "properties": {
        "chromosome": {
            "type": "string"
        },
        "start": {
            "type": "integer"
        },
        "end": {
            "type": "integer"
        },
        "ref": {
            "type": "string"
        },
        "alt": {
            "type": "string"
        }
    }
}


application = Flask(__name__)
application.config.from_mapping(
    DATABASE=os.environ.get("DATABASE", "chord_example_service.db")
)

data_path = os.environ.get("DATA", "data/")
datasets = {d: [os.listdir(os.path.join(data_path, d))] for d in os.listdir(data_path)
            if os.path.isdir(os.path.join(data_path, d))}


def data_type_404(data_type_id):
    return json.dumps({
        "code": 404,
        "message": "Data type not found",
        "timestamp": datetime.datetime.utcnow().isoformat("T") + "Z",
        "errors": [{"code": "not_found", "message": f"Data type with ID {data_type_id} was not found"}]
    })


@application.route("/data-types", methods=["GET"])
def data_type_list():
    # Data types are basically stand-ins for schema blocks

    return jsonify([{"id": "variants", "schema": VARIANT_SCHEMA}])


@application.route("/data-types/variant", methods=["GET"])
def data_type_detail():
    return jsonify({
        "id": "variant",
        "schema": VARIANT_SCHEMA
    })


@application.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)


@application.route("/datasets", methods=["GET"])
def dataset_list():
    dt = request.args.get("data-type", default="")

    if dt != "variant":
        return data_type_404("variant")

    return jsonify([{
        "id": d,
        "schema": VARIANT_SCHEMA
    } for d in datasets.keys()])


@application.route("/datasets/<uuid:dataset_id>", methods=["GET"])
def dataset_detail(dataset_id):
    # Not implementing this
    pass


SEARCH_NEGATION = ("pos", "neg")
SEARCH_CONDITIONS = ("eq", "lt", "le", "gt", "ge", "co")
SQL_SEARCH_CONDITIONS = {
    "eq": "=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "co": "LIKE"
}


@application.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    # dt = request.json["dataTypeID"]
    # conditions = request.json["conditions"]
    # conditions_filtered = [c for c in conditions if c["searchField"].split(".")[-1] in ("id", "content") and
    #                        c["negation"] in SEARCH_NEGATION and c["condition"] in SEARCH_CONDITIONS]
    # query = ("SELECT * FROM datasets AS d WHERE d.data_type = ? AND d.id IN ("
    #          "SELECT dataset FROM entries WHERE {})".format(
    #              " AND ".join(["{}({} {} ?)".format("NOT " if c["negation"] == "neg" else "",
    #                                                 c["searchField"].split(".")[-1],
    #                                                 SQL_SEARCH_CONDITIONS[c["condition"]])
    #                            for c in conditions_filtered])))
    #
    # db = get_db()
    # c = db.cursor()
    #
    # c.execute(query, (dt,) + tuple([f"%{c['searchValue']}%" if c["condition"] == "co" else c["searchValue"]
    #                                 for c in conditions_filtered]))
    #
    return jsonify({"results": []})


@application.route("/service-info", methods=["GET"])
def service_info():
    # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

    return jsonify({
        "id": "ca.distributedgenomics.chord_variant_service",  # TODO: Should be globally unique?
        "name": "CHORD Variant Service",                       # TODO: Should be globally unique?
        "type": "urn:ga4gh:search",                            # TODO
        "description": "Example service for a CHORD application.",
        "organization": "GenAP",
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_variant_service.__version__,
        "extension": {}
    })
