import sys

from chord_lib.auth.flask_decorators import flask_permissions, flask_permissions_owner
from chord_lib.responses import flask_errors
from flask import Blueprint, current_app, json, jsonify, request, url_for
from jsonschema import validate, ValidationError

from chord_variant_service.tables.base import TableManager
from chord_variant_service.tables.vcf.table import VCFVariantTable
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
        return flask_errors.flask_bad_request_error(f"Invalid or missing data type (specified ID: {dt})")

    table_manager: TableManager = current_app.config["TABLE_MANAGER"]

    # TODO: This POST stuff is not compliant with the GA4GH Search API
    if request.method == "POST":
        if request.json is None:
            return flask_errors.flask_bad_request_error("Missing or invalid request body")

        # TODO: Query schema

        if not isinstance(request.json, dict):
            return flask_errors.flask_bad_request_error("Request body is not an object")

        if "metadata" not in request.json:
            return flask_errors.flask_bad_request_error("Missing metadata field")

        name = request.json.get("name", "").strip()
        metadata = request.json["metadata"]

        if name == "":
            return flask_errors.flask_bad_request_error("Missing or blank name field")

        try:
            validate(metadata, VARIANT_TABLE_METADATA_SCHEMA)
        except ValidationError:
            # TODO: Validation message
            return flask_errors.flask_bad_request_error("Invalid metadata format (validation failed)")

        try:
            new_table = table_manager.create_table_and_update(name, metadata)
            return current_app.response_class(response=json.dumps(new_table.as_table_response()),
                                              mimetype=MIME_TYPE, status=201)

        except IDGenerationFailure:
            print("[CHORD Variant Service] Couldn't generate new ID", file=sys.stderr)
            return flask_errors.flask_internal_server_error("Could not generate new ID for table")

    return jsonify([t.as_table_response() for t in table_manager.get_tables().values()])


# TODO: Implement POST (separate permissions)
@bp_tables.route("/tables/<string:table_id>", methods=["GET", "DELETE"])
@flask_permissions({"DELETE": {"owner"}})
def table_detail(table_id):
    table_manager: TableManager = current_app.config["TABLE_MANAGER"]
    table = table_manager.get_table(table_id)

    if table is None:
        # TODO: Refresh cache if needed?
        return flask_errors.flask_not_found_error(f"No table with ID {table_id}")

    if request.method == "DELETE":
        table_manager.delete_table_and_update(table_id)
        # TODO: More complete response?
        return current_app.response_class(status=204)

    return jsonify(table.as_table_response())


@bp_tables.route("/tables/<string:table_id>/summary", methods=["GET"])
@flask_permissions_owner
def table_summary(table_id):
    table_manager: TableManager = current_app.config["TABLE_MANAGER"]
    table = table_manager.get_table(table_id)

    if table is None:
        # TODO: Refresh cache if needed?
        return flask_errors.flask_not_found_error(f"No table with ID {table_id}")

    return {
        "count": table.n_of_variants,
        "data_type_specific": {
            "samples": table.n_of_samples,
            **({"vcf_files": len(table.files)} if isinstance(table, VCFVariantTable) else {}),
            # TODO: Average minor allele frequency? other cool variant statistics?
            # TODO: Number of known "rare" variants? based on rs... null if not provided
        }
    }


@bp_tables.route("/private/tables/<string:table_id>/data", methods=["GET"])
@bp_tables.route("/private/tables/<string:table_id>/variants", methods=["GET"])
def table_data(table_id):
    table_manager: TableManager = current_app.config["TABLE_MANAGER"]
    table = table_manager.get_table(table_id)

    if table is None:
        # TODO: Refresh cache if needed?
        return flask_errors.flask_not_found_error(f"No table with ID {table_id}")

    try:
        offset = int(request.args.get("offset", "0"))
        count = int(request.args.get("count", "100").strip())  # TODO: Constant-ify default count
    except ValueError:
        return flask_errors.flask_bad_request_error("Invalid offset or count provided")

    if offset < 0:
        return flask_errors.flask_bad_request_error("Offset must be non-negative")

    if count <= 0:
        return flask_errors.flask_bad_request_error("Count must be positive")

    # TODO: Assembly ID?

    # TODO: Move this to search?
    only_interesting = request.args.get("only_interesting", "false").strip().lower() == "true"

    # TODO: Filtering?
    # TODO: Make consistent with search results?

    # TODO: What should be done when offset sends us off the end? 404?

    data = [v.as_chord_representation()
            for v in table.variants(offset=offset, count=count, only_interesting=only_interesting)]

    next_page = next(table.variants(offset=offset + count, count=count, only_interesting=only_interesting),
                     None) is not None  # Check if there's at least one next result

    return jsonify({
        "schema": VARIANT_SCHEMA,
        "data": data,
        # TODO: Need to calculate nulls based on total variants in a table
        "pagination": {  # TODO: CHORD_URL
            "previous_page_url": (
                url_for("tables.table_data", table_id=table_id) +
                f"?offset={max(0, offset - count)}&count={count + min(0, offset - count)}"
            ) if offset > 0 else None,
            "next_page_url": (
                url_for("tables.table_data", table_id=table_id) +
                f"?offset={offset + count}&count={count}"
            ) if next_page else None,
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
