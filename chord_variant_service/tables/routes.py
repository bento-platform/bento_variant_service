import sys

from chord_lib.auth.flask_decorators import flask_permissions
from chord_lib.responses.flask_errors import *
from flask import Blueprint, current_app, json, jsonify, request, url_for
from jsonschema import validate, ValidationError

from chord_variant_service.tables.base import TableManager
from chord_variant_service.tables.exceptions import IDGenerationFailure
from chord_variant_service.variants.schemas import VARIANT_SCHEMA, VARIANT_TABLE_METADATA_SCHEMA


__all__ = [
    "MIME_TYPE",
    "bp_tables",
]


MIME_TYPE = "application/json"


bp_tables = Blueprint("tables", __name__)


# Fetch or create tables
@bp_tables.route("/tables", methods=["GET", "POST"])
@flask_permissions({"POST": {"owner"}})
def table_list():
    dt = request.args.getlist("data-type")

    if "variant" not in dt or len(dt) != 1:
        return flask_bad_request_error(f"Invalid or missing data type (specified ID: {dt})")

    # TODO: This POST stuff is not compliant with the GA4GH Search API
    if request.method == "POST":
        if request.json is None:
            return flask_bad_request_error("Missing or invalid request body")

        # TODO: Query schema

        if not isinstance(request.json, dict):
            return flask_bad_request_error("Request body is not an object")

        if "metadata" not in request.json:
            return flask_bad_request_error("Missing metadata field")

        name = request.json.get("name", "").strip()
        metadata = request.json["metadata"]

        if name == "":
            return flask_bad_request_error("Missing or blank name field")

        try:
            validate(metadata, VARIANT_TABLE_METADATA_SCHEMA)
        except ValidationError:
            return flask_bad_request_error("Invalid metadata format (validation failed)")  # TODO: Validation message

        try:
            new_table = current_app.config["TABLE_MANAGER"].create_table_and_update(name, metadata)
            return current_app.response_class(response=json.dumps(new_table.as_table_response()),
                                              mimetype=MIME_TYPE, status=201)

        except IDGenerationFailure:
            print("[CHORD Variant Service] Couldn't generate new ID", file=sys.stderr)
            return flask_internal_server_error("Could not generate new ID for table")

    return jsonify([d.as_table_response() for d in current_app.config["TABLE_MANAGER"].get_tables().values()])


# TODO: Implement POST (separate permissions)
@bp_tables.route("/tables/<string:table_id>", methods=["GET", "DELETE"])
@flask_permissions({"DELETE": {"owner"}})
def table_detail(table_id):
    if current_app.config["TABLE_MANAGER"].get_table(table_id) is None:
        # TODO: Refresh cache if needed?
        return flask_not_found_error(f"No table with ID {table_id}")

    if request.method == "DELETE":
        current_app.config["TABLE_MANAGER"].delete_table_and_update(table_id)

        # TODO: More complete response?
        return current_app.response_class(status=204)

    return jsonify(current_app.config["TABLE_MANAGER"].get_table(table_id).as_table_response())


@bp_tables.route("/private/tables/<string:table_id>/data", methods=["GET"])
def table_data(table_id):
    table_manager: TableManager = current_app.config["TABLE_MANAGER"]

    if table_manager.get_table(table_id) is None:
        # TODO: Refresh cache if needed?
        return flask_not_found_error(f"No table with ID {table_id}")

    try:
        offset = int(request.args.get("offset", "0"))
        count = int(request.args.get("count", "100").strip())  # TODO: Constant-ify default count
    except ValueError:
        return flask_bad_request_error("Invalid offset or count provided")

    if offset < 0:
        return flask_bad_request_error("Offset must be non-negative")

    if count <= 0:
        return flask_bad_request_error("Count must be positive")

    total_variants = table_manager.get_table(table_id).n_of_variants

    # TODO: Pagination
    # TODO: Filtering?
    # TODO: Make consistent with search results?

    # TODO: What should be done when offset sends us off the end? 404?

    return jsonify({
        "schema": VARIANT_SCHEMA,
        "data": list(v.as_chord_representation()
                     for v in current_app.config["TABLE_MANAGER"].get_table(table_id).variants(offset=offset,
                                                                                               count=count)),
        # TODO: Need to calculate nulls based on total variants in a table
        "pagination": {  # TODO: CHORD_URL
            "previous_page_url": (
                url_for("tables.table_data", table_id=table_id) +
                f"?offset={max(0, offset - count)}&count={count + min(0, offset - count)}"
            ) if offset > 0 else None,
            "next_page_url": (
                url_for("tables.table_data", table_id=table_id) +
                f"?offset={offset + count}&count={count}"
            ) if offset + count < total_variants else None,
        }
    })


@bp_tables.route("/data-types", methods=["GET"])
def data_type_list():
    # Data types are basically stand-ins for schema blocks

    return jsonify([{
        "id": "variant",
        "schema": VARIANT_SCHEMA,
        "metadata_schema": VARIANT_TABLE_METADATA_SCHEMA
    }])


@bp_tables.route("/data-types/variant", methods=["GET"])
def data_type_detail():
    return jsonify({
        "id": "variant",
        "schema": VARIANT_SCHEMA,
        "metadata_schema": VARIANT_TABLE_METADATA_SCHEMA
    })


@bp_tables.route("/data-types/variant/schema", methods=["GET"])
def data_type_schema():
    return jsonify(VARIANT_SCHEMA)


# TODO: Consistent snake or kebab
@bp_tables.route("/data-types/variant/metadata_schema", methods=["GET"])
def data_type_metadata_schema():
    return jsonify(VARIANT_TABLE_METADATA_SCHEMA)
