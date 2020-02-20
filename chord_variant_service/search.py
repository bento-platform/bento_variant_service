import re

from chord_lib.responses.flask_errors import *
from chord_lib.search.data_structure import check_ast_against_data_structure
from chord_lib.search.queries import (
    convert_query_to_ast_and_preprocess,
    ast_to_and_asts,
    and_asts_to_ast,

    AST,
    Expression,
    Literal,

    FUNCTION_EQ,
    FUNCTION_LT,
    FUNCTION_LE,
    FUNCTION_GT,
    FUNCTION_GE,
    FUNCTION_RESOLVE
)
from flask import Blueprint, current_app, jsonify, request

from typing import Any, Callable, List, Iterable, Optional, Tuple

from .tables import VariantTable, TableManager
from .pool import get_pool
from .variants import VARIANT_SCHEMA


CHROMOSOME_REGEX = re.compile(r"([^\s:.]{1,100}|\.|<[^\s;]+>)")
BASE_REGEX = re.compile(r"([acgtnACGTN]+|\.|<[^\s;]+>)")


def search_worker_prime(
    dataset: VariantTable,
    chromosome: Optional[str],
    start_min: Optional[int],
    start_max: Optional[int],
    rest_of_query: Optional[AST],
    internal_data: bool,
    assembly_id: Optional[str],
) -> Tuple[Optional[VariantTable], List[dict]]:
    found = False
    matches = []

    possible_matches = dataset.variants(assembly_id, chromosome, start_min, start_max)

    while not found or internal_data:
        try:
            variant = next(possible_matches)

            match = rest_of_query is None or check_ast_against_data_structure(
                rest_of_query, variant.as_chord_representation(), VARIANT_SCHEMA)
            found = found or match

            if not internal_data and found:
                break

            if match:  # implicitly internal_data is True here as well
                matches.append(variant.as_chord_representation())

        except StopIteration:
            break

        except ValueError as e:  # pragma: no cover
            # int casts from VCF
            # TODO
            print(str(e))
            break

    return (dataset if found else None), matches


def search_worker(args):
    return search_worker_prime(*args)


def generic_variant_search(
    table_manager: TableManager,
    chromosome: Optional[str],
    start_min: Optional[int],
    start_max: Optional[int],
    rest_of_query: Optional[AST] = None,
    internal_data=False,
    assembly_id: Optional[str] = None,
    dataset_ids: Optional[List[str]] = None
) -> Iterable[Tuple[VariantTable, List[dict]]]:
    # TODO: Sane defaults
    # TODO: Figure out inclusion/exclusion with start_min/end_max

    ds = set(dataset_ids) if dataset_ids is not None else None

    pool = get_pool()
    pool_map: Iterable[Tuple[Optional[VariantTable], List[dict]]] = pool.imap_unordered(
        search_worker,
        ((dataset, chromosome, start_min, start_max, rest_of_query, internal_data, assembly_id)
         for dataset in table_manager.get_tables().values()
         if (ds is None or dataset.table_id in ds) and (assembly_id is None or assembly_id in dataset.assembly_ids))
    )

    return ((d, m) for d, m in pool_map if len(m) > 0 or (not internal_data and d is not None))


def query_key_op_value(query_item: AST, field: str, op: str) -> Optional[Literal]:
    # checks format of query_item is [#op [#resolve field] "value"] and yields "value" if so

    if (isinstance(query_item, Expression) and
            query_item.fn == op and
            isinstance(query_item.args[0], Expression) and
            query_item.args[0].fn == FUNCTION_RESOLVE and
            isinstance(query_item.args[0].args[0], Literal) and
            query_item.args[0].args[0].value == field and
            isinstance(query_item.args[1], Literal)):
        return query_item.args[1].value

    return None


def query_key_op_value_or_pass(query_item: AST, field: str, value: Optional[Literal], state_changed: bool, op: str,
                               transform: Callable = lambda x: x) -> Tuple[Optional[Any], bool]:
    if value is None:  # No value has been parsed from the query yet
        value = query_key_op_value(query_item, field, op)  # May yield a value
        if value is not None:  # New value derived from query_key_op_value call
            value = transform(value)  # Transform the value to standardize it in the context of the query
            return value, True  # Something got transformed, so return True for state_changed

    return value, state_changed


class InvalidQuery(Exception):
    pass


def pipeline_value_extraction(query_item: AST, initial_value: Optional[Literal], initial_state_changed: bool,
                              extractors: Tuple[Tuple[str, str, Callable], ...]) -> Tuple[Optional[Any], bool]:
    field, op, transform = extractors[0]
    v, s = query_key_op_value_or_pass(query_item, field, initial_value, initial_state_changed, op, transform)
    for field, op, transform in extractors[1:]:
        v, s = query_key_op_value_or_pass(query_item, field, v, s, op, transform)
    return v, s


def parse_query_for_tabix(query: AST) -> Tuple[Optional[str], Optional[int], Optional[int], AST]:
    query_items = ast_to_and_asts(query)

    chromosome = None
    start_min = None
    start_max = None

    other_query_items = []

    for q in query_items:
        # format: [#eq [#resolve chromosome] "chr1"]

        state_changed = False

        chromosome, state_changed = query_key_op_value_or_pass(q, "chromosome", chromosome, state_changed, FUNCTION_EQ)

        # Reminder: start_min is inclusive (start_min = X is the same as start >= X)
        start_min, state_changed = pipeline_value_extraction(q, start_min, state_changed, (
            ("start", FUNCTION_EQ, int),  # Convert start = X to start_min = X (start_max handled below)
            ("start", FUNCTION_GE, int),  # Extract start >= X into start_min = X
            ("start", FUNCTION_GT, lambda x: int(x) + 1),  # Convert start > X to start_min = X + 1
        ))

        # Reminder: start_max is exclusive (start_max = X is the same as start < X); variants are at least 1 nucleotide
        #   long, meaning that if end is limited in some way we can sometimes derive a start_max.
        start_max, state_changed = pipeline_value_extraction(q, start_max, state_changed, (
            ("start", FUNCTION_EQ, lambda x: int(x) + 1),  # Convert start = X to start_max = X + 1
            ("start", FUNCTION_LE, lambda x: int(x) + 1),  # Extract start <= X to start_max = X + 1
            ("start", FUNCTION_LT, int),  # Convert start < X to start_max = X
            ("end", FUNCTION_LE, int),  # Convert end <= X to start_max = X
            ("end", FUNCTION_LT, lambda x: int(x) - 1),  # Convert end < X to start_max = X - 2
        ))

        if not state_changed:
            other_query_items.append(q)

    return chromosome, start_min, start_max, and_asts_to_ast(tuple(other_query_items))


def chord_search(table_manager: TableManager, dt: str, query: List, internal_data: bool = False):
    null_result = {} if internal_data else []

    if dt != "variant":
        # TODO: Don't silently ignore errors
        return null_result

    # TODO: Parser error handling
    query_ast = convert_query_to_ast_and_preprocess(query)

    try:
        chromosome, start_min, start_max, rest_of_query = parse_query_for_tabix(query_ast)
    except InvalidQuery:
        # TODO: Don't silently ignore errors
        return null_result

    dataset_results = {} if internal_data else []

    # TODO: What coordinate system do we want?

    try:
        # Check validity of VCF chromosome
        assert chromosome is None or re.match(CHROMOSOME_REGEX, chromosome) is not None

        search_results = generic_variant_search(
            table_manager=table_manager,
            chromosome=chromosome,
            start_min=start_min,
            start_max=start_max,
            rest_of_query=rest_of_query,
            internal_data=internal_data
        )

        if internal_data:
            return {d.table_id: {"data_type": "variant", "matches": e} for d, e in search_results if e is not None}

        return [{"id": d.table_id, "data_type": "variant"} for d, _ in search_results]

    except (ValueError, AssertionError) as e:
        # TODO
        print(str(e))

    return dataset_results


bp_chord_search = Blueprint("chord_search", __name__)


def _search_endpoint(internal_data=False):
    # TODO: Request validation schema

    if request.json is None:
        return flask_bad_request_error("Missing request body")

    if not isinstance(request.json, dict):
        return flask_bad_request_error("Request body is not an object")

    if "data_type" not in request.json:
        return flask_bad_request_error("Missing data type in request body")

    if "query" not in request.json:
        return flask_bad_request_error("Missing data type in request body")

    return jsonify({"results": chord_search(current_app.config["TABLE_MANAGER"],
                                            request.json["data_type"],
                                            request.json["query"],
                                            internal_data=internal_data)})


@bp_chord_search.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY
    return _search_endpoint()


@bp_chord_search.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly
    return _search_endpoint(internal_data=True)


@bp_chord_search.route("/private/tables/<string:table_id>/search", methods=["POST"])
def table_search(table_id):
    table = current_app.config["TABLE_MANAGER"].get_table(table_id)

    if table is None:
        # TODO: Refresh cache if needed?
        return flask_not_found_error(f"No table with ID {table_id}")

    if request.json is None:
        return flask_bad_request_error("Missing search body")

    # TODO: Schema for request body

    if not isinstance(request.json, dict):
        return flask_bad_request_error("Search body must be an object")

    if "query" not in request.json:
        return flask_bad_request_error("Query not included in search body")

    # If it exists in the variant table manager, it's of data type 'variant'

    search = chord_search(current_app.config["TABLE_MANAGER"], "variant", request.json["query"], internal_data=True)
    return jsonify({"results": search.get(table_id, {}).get("matches", [])})
