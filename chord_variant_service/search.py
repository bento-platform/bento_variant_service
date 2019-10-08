import chord_lib.search
import re
import tabix

from flask import Blueprint, jsonify, request
from operator import eq, ne

from .datasets import datasets, update_datasets
from .pool import get_pool
from .variants import VARIANT_SCHEMA


CHROMOSOME_REGEX = re.compile(r"([^\s:.]{1,100}|\.|<[^\s;]+>)")
BASE_REGEX = re.compile(r"([acgtnACGTN]+|\.|<[^\s;]+>)")


def search_worker_prime(d, chromosome, start_min, start_max, end_min, end_max, ref, alt, ref_op, alt_op, internal_data,
                        assembly_id):
    refresh_at_end = False

    found = False
    matches = []
    for vcf in (vf for vf, a_id in zip(datasets[d]["files"], datasets[d]["assembly_ids"]) if a_id == assembly_id):
        try:
            tbx = tabix.open(vcf)

            # TODO: Security of passing this? Verify values in non-Beacon searches
            for row in tbx.query(chromosome, start_min, end_max):
                if (start_max is not None and row[1] > start_max) or (end_min is not None and row[2] < end_min):
                    # TODO: Are start_max and end_min both inclusive for sure? Pretty sure but unclear
                    continue

                if ref is not None and alt is None:
                    match = ref_op(row[3].upper(), ref.upper())
                elif ref is None and alt is not None:
                    match = alt_op(row[4].upper(), alt.upper())
                elif ref is not None and alt is not None:
                    match = (ref_op(row[3].upper(), ref.upper()) and
                             alt_op(row[4].upper(), alt.upper()))
                else:
                    match = True

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

            if found:
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
        update_datasets()

    if internal_data:
        return d, {"data_type": "variant", "matches": matches} if found else None

    return {"id": d, "data_type": "variant"} if found else None


def search_worker(args):
    return search_worker_prime(*args)


def generic_variant_search(chromosome, start_min, start_max=None, end_min=None, end_max=None, ref=None, alt=None,
                           ref_op=eq, alt_op=eq, internal_data=False, assembly_id=None, dataset_ids=None):

    # TODO: Sane defaults
    # TODO: Figure out inclusion/exclusion with start_min/end_max

    dataset_results = {} if internal_data else []
    ds = set(dataset_ids) if dataset_ids is not None else None

    try:
        pool = get_pool()
        pool_map = pool.imap_unordered(
            search_worker,
            ((d, chromosome, start_min, start_max, end_min, end_max, ref, alt, ref_op, alt_op, internal_data,
              assembly_id) for d in datasets
             if (ds is None or d in ds) and (assembly_id is None or assembly_id in datasets[d]["assembly_ids"]))
        )

        if internal_data:
            dataset_results = {d: e for d, e in pool_map if e is not None}
        else:
            dataset_results = [d for d in pool_map if d is not None]

    except ValueError as e:
        print(str(e))

    return dataset_results


# TODO: To chord_lib? Maybe conditions_dict should be a class or something...
def parse_conditions(conditions):
    return {
        c["field"].split(".")[-1]: c
        for c in conditions
        if (c["field"].split(".")[-1] in VARIANT_SCHEMA["properties"] and
            isinstance(c["negated"], bool) and c["operation"] in chord_lib.search.SEARCH_OPERATIONS)
    }


def chord_search(dt, conditions, internal_data=False):
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

        return generic_variant_search(chromosome=chromosome, start_min=start_pos, end_max=end_pos,
                                      ref=ref_cond["searchValue"] if ref_cond is not None else None,
                                      alt=alt_cond["searchValue"] if alt_cond is not None else None,
                                      ref_op=ref_op, alt_op=alt_op, internal_data=internal_data)

    except (ValueError, AssertionError) as e:
        # TODO
        print(str(e))

    return dataset_results


bp_chord_search = Blueprint("chord_search", __name__)


@bp_chord_search.route("/search", methods=["POST"])
def search_endpoint():
    # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP
    # TODO: PROBABLY VULNERABLE IN SOME WAY

    return jsonify({"results": chord_search(request.json["dataTypeID"], request.json["conditions"], False)})


@bp_chord_search.route("/private/search", methods=["POST"])
def private_search_endpoint():
    # Proxy should ensure that non-services cannot access this
    # TODO: Figure out security properly

    return jsonify({"results": chord_search(request.json["dataTypeID"], request.json["conditions"], True)})
