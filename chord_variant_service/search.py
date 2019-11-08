import chord_lib.search
import re
import tabix

from flask import Blueprint, current_app, jsonify, request
from operator import eq, ne

from typing import Callable, List, Iterable, Optional, Tuple

from .datasets import VariantTable, TableManager
from .pool import get_pool
from .variants import VARIANT_SCHEMA


CHROMOSOME_REGEX = re.compile(r"([^\s:.]{1,100}|\.|<[^\s;]+>)")
BASE_REGEX = re.compile(r"([acgtnACGTN]+|\.|<[^\s;]+>)")


def search_worker_prime(
    dataset: VariantTable,
    chromosome: str,
    start_min: int,
    start_max: Optional[int],
    end_min: Optional[int],
    end_max: int,
    ref: Optional[str],
    alt: Optional[str],
    ref_op: Callable[[str, str], bool],
    alt_op: Callable[[str, str], bool],
    internal_data: bool,
    assembly_id: Optional[str],
    table_manager: TableManager
) -> Tuple[Optional[VariantTable], List[dict]]:
    refresh_at_end = False

    found = False
    matches = []
    for vcf in (vf for vf, a_id in dataset.file_assembly_ids.items() if assembly_id is None or a_id == assembly_id):
        try:
            tbx = tabix.open(vcf)

            # TODO: Security of passing this? Verify values in non-Beacon searches
            for row in tbx.query(chromosome, start_min, end_max):
                if (start_max is not None and row[1] > start_max) or (end_min is not None and row[2] < end_min):
                    # TODO: Are start_max and end_min both inclusive for sure? Pretty sure but unclear
                    continue

                match = ((ref_op(row[3].upper(), ref.upper()) if ref is not None else True) and
                         (alt_op(row[4].upper(), alt.upper()) if alt is not None else True))

                found = found or match

                if not internal_data and found:
                    break

                if match:  # implicitly internal_data is True here as well
                    matches.append({
                        "chromosome": row[0],
                        "start": row[1],
                        "end": row[2],
                        "ref": row[3],
                        "alt": row[4]
                    })

            if not internal_data and found:
                break

        except tabix.TabixError:
            # Dataset might be removed or corrupt, skip it and refresh datasets at the end
            print("Error processing tabix file: {}".format(vcf))
            refresh_at_end = True

        except ValueError as e:
            # TODO
            print(str(e))
            break

    if refresh_at_end:
        table_manager.update_datasets()

    return (dataset if found else None), matches


def search_worker(args):
    return search_worker_prime(*args)


def generic_variant_search(
    table_manager: TableManager,
    chromosome: str,
    start_min: int,
    start_max: Optional[int] = None,
    end_min: Optional[int] = None,
    end_max=None,
    ref: Optional[str] = None,
    alt: Optional[str] = None,
    ref_op: Callable[[str, str], bool] = eq,
    alt_op: Callable[[str, str], bool] = eq,
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
            ((dataset, chromosome, start_min, start_max, end_min, end_max, ref, alt, ref_op, alt_op, internal_data,
              assembly_id, table_manager) for dataset in table_manager.get_datasets().values()
             if (ds is None or dataset.table_id in ds) and (assembly_id is None or assembly_id in dataset.assembly_ids))
        )

        dataset_results = ((d, m) for d, m in pool_map if len(m) > 0 or (not internal_data and d is not None))

    except ValueError as e:
        print(str(e))

    return dataset_results


# TODO: To chord_lib? Maybe conditions_dict should be a class or something...
def parse_conditions(conditions: List) -> dict:
    return {
        c["field"].split(".")[-1]: c
        for c in conditions
        if (c["field"].split(".")[-1] in VARIANT_SCHEMA["properties"] and
            isinstance(c["negated"], bool) and c["operation"] in chord_lib.search.SEARCH_OPERATIONS)
    }


def chord_search(table_manager: TableManager, dt: str, conditions: List, internal_data: bool = False):
    null_result = {} if internal_data else []

    if dt != "variant":
        return null_result

    condition_dict = parse_conditions(conditions)

    if "chromosome" not in condition_dict or "start" not in condition_dict or "end" not in condition_dict:
        # TODO: Error
        # TODO: Not hardcoded?
        # TODO: More conditions
        return null_result

    dataset_results = {} if internal_data else []

    # TODO: What coordinate system do we want?

    try:
        chromosome = condition_dict["chromosome"]["searchValue"]
        assert re.match(CHROMOSOME_REGEX, chromosome) is not None  # Check validity of VCF chromosome

        start_pos = int(condition_dict["start"]["searchValue"])
        end_pos = int(condition_dict["end"]["searchValue"])

        ref_cond = condition_dict.get("ref", None)
        alt_cond = condition_dict.get("alt", None)

        assert (re.match(ref_cond["searchValue"], BASE_REGEX) is not None if ref_cond else True)
        assert (re.match(alt_cond["searchValue"], BASE_REGEX) is not None if alt_cond else True)

        ref_op = ne if ref_cond is not None and ref_cond["negated"] else eq
        alt_op = ne if alt_cond is not None and alt_cond["negated"] else eq

        search_results = generic_variant_search(
            table_manager=table_manager,
            chromosome=chromosome,
            start_min=start_pos,
            end_max=end_pos,
            ref=ref_cond["searchValue"] if ref_cond is not None else None,
            alt=alt_cond["searchValue"] if alt_cond is not None else None,
            ref_op=ref_op,
            alt_op=alt_op,
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

    return jsonify({"results": chord_search(current_app.config["TABLE_MANAGER"],
                                            request.json["dataTypeID"],
                                            request.json["conditions"],
                                            internal_data=False)})


@bp_chord_search.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly

    return jsonify({"results": chord_search(current_app.config["TABLE_MANAGER"],
                                            request.json["dataTypeID"],
                                            request.json["conditions"],
                                            internal_data=True)})
