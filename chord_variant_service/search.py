import re
import tabix

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

from typing import Callable, List, Iterable, Optional, Tuple

from .datasets import VariantTable, TableManager
from .pool import get_pool
from .variants import VARIANT_SCHEMA


CHROMOSOME_REGEX = re.compile(r"([^\s:.]{1,100}|\.|<[^\s;]+>)")
BASE_REGEX = re.compile(r"([acgtnACGTN]+|\.|<[^\s;]+>)")


def search_worker_prime(
    dataset: VariantTable,
    chromosome: str,
    start_min: Optional[int],
    start_max: Optional[int],
    rest_of_query: Optional[AST],
    internal_data: bool,
    assembly_id: Optional[str],
    table_manager: TableManager
) -> Tuple[Optional[VariantTable], List[dict]]:
    refresh_at_end = False

    found = False
    matches = []

    possible_matches = dataset.variants(assembly_id, chromosome, start_min, start_max)

    while not found:
        try:
            variant = next(possible_matches)

            # TODO: Deal with sample homozygous / heterozygous

            match = rest_of_query is None or check_ast_against_data_structure(rest_of_query, variant, VARIANT_SCHEMA)
            found = found or match

            if not internal_data and found:
                break

            if match:  # implicitly internal_data is True here as well
                matches.append(variant)

        except StopIteration:
            break

        except tabix.TabixError:  # pragma: no cover
            # Dataset might be removed or corrupt, skip it and refresh datasets at the end
            print("Error processing a tabix file in dataset {}".format(dataset.table_id))
            refresh_at_end = True

        except ValueError as e:  # pragma: no cover
            # int casts from VCF
            # TODO
            print(str(e))
            break

    if refresh_at_end:  # pragma: no cover
        table_manager.update_datasets()

    return (dataset if found else None), matches


def search_worker(args):
    return search_worker_prime(*args)


def generic_variant_search(
    table_manager: TableManager,
    chromosome: str,
    start_min: int,
    start_max: int,
    rest_of_query: Optional[AST] = None,
    internal_data=False,
    assembly_id: Optional[str] = None,
    dataset_ids: Optional[List[str]] = None
) -> Iterable[Tuple[VariantTable, List[dict]]]:

    # TODO: Sane defaults
    # TODO: Figure out inclusion/exclusion with start_min/end_max

    dataset_results = ()
    ds = set(dataset_ids) if dataset_ids is not None else None

    try:
        pool = get_pool()
        pool_map: Iterable[Tuple[Optional[VariantTable], List[dict]]] = pool.imap_unordered(
            search_worker,
            ((dataset, chromosome, start_min, start_max, rest_of_query, internal_data,
              assembly_id, table_manager) for dataset in table_manager.get_datasets().values()
             if (ds is None or dataset.table_id in ds) and (assembly_id is None or assembly_id in dataset.assembly_ids))
        )

        dataset_results = ((d, m) for d, m in pool_map if len(m) > 0 or (not internal_data and d is not None))

    except ValueError as e:
        print(str(e))

    return dataset_results


def query_key_op_value(query_item: AST, field: str, op: str):
    # format: [#op [#resolve field] "value"]

    if (isinstance(query_item, Expression) and
            query_item.fn == op and
            isinstance(query_item.args[0], Expression) and
            query_item.args[0].fn == FUNCTION_RESOLVE and
            isinstance(query_item.args[0].args[0], Literal) and
            query_item.args[0].args[0].value == field and
            isinstance(query_item.args[1], Literal)):
        return query_item.args[1].value

    return None


def query_key_op_value_or_pass(value, state_changed: bool, query_item: AST, field: str, op: str,
                               transform: Callable = lambda x: x):
    if value is None:
        value = query_key_op_value(query_item, field, op)
        if value is not None:
            value = transform(value)
            return value, True

    return value, state_changed or False


class InvalidQuery(Exception):
    pass


def parse_query_for_tabix(query: AST) -> Tuple[Optional[str], Optional[int], Optional[int], AST]:
    query_items = ast_to_and_asts(query)

    chromosome = None
    start_min = None
    start_max = None

    other_query_items = []

    for q in query_items:
        # format: [#eq [#resolve chromosome] "chr1"]

        state_changed = False

        chromosome, state_changed = query_key_op_value_or_pass(chromosome, state_changed, q, "chromosome", FUNCTION_EQ)

        start_min, state_changed = query_key_op_value_or_pass(start_min, state_changed, q, "start", FUNCTION_GE, int)
        start_min, state_changed = query_key_op_value_or_pass(start_min, state_changed, q, "start", FUNCTION_GT,
                                                              lambda x: int(x) + 1)  # Convert > to >=

        start_max, state_changed = query_key_op_value_or_pass(start_max, state_changed, q, "start", FUNCTION_LE, int)
        start_max, state_changed = query_key_op_value_or_pass(start_max, state_changed, q, "start", FUNCTION_LT,
                                                              lambda x: int(x) - 1)  # Convert < to <=

        start_max, state_changed = query_key_op_value_or_pass(
            start_max, state_changed, q, "end", FUNCTION_LE, lambda x: int(x) - 1)  # Convert end <= X to start <= X - 1
        start_max, state_changed = query_key_op_value_or_pass(
            start_max, state_changed, q, "end", FUNCTION_LT, lambda x: int(x) - 2)  # Convert end < X to start <= X - 2

        if not state_changed:
            other_query_items.append(q)

    if chromosome is None:
        raise InvalidQuery

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
        assert re.match(CHROMOSOME_REGEX, chromosome) is not None  # Check validity of VCF chromosome

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


@bp_chord_search.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    if request.json is None or "data_type" not in request.json or "query" not in request.json:
        return current_app.response_class(status=400)

    return jsonify({"results": chord_search(current_app.config["TABLE_MANAGER"],
                                            request.json["data_type"],
                                            request.json["query"],
                                            internal_data=False)})


@bp_chord_search.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly

    if request.json is None or "data_type" not in request.json or "query" not in request.json:
        return current_app.response_class(status=400)

    return jsonify({"results": chord_search(current_app.config["TABLE_MANAGER"],
                                            request.json["data_type"],
                                            request.json["query"],
                                            internal_data=True)})


@bp_chord_search.route("/private/tables/<string:table_id>/search", methods=["POST"])
def table_search(table_id):
    table = current_app.config["TABLE_MANAGER"].get_dataset(table_id)

    if table is None:
        # TODO: More standardized error
        # TODO: Refresh cache if needed?
        return current_app.response_class(status=404)

    if request.json is None or "query" not in request.json:
        return current_app.response_class(status=400)

    # If it exists in the variant table manager, it's of data type 'variant'

    search = chord_search(current_app.config["TABLE_MANAGER"], "variant", request.json["query"], internal_data=True)
    return jsonify({"results": search[table_id]["matches"] if len(search) > 0 else []})
