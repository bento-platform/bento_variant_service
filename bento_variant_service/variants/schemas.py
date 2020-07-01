# Possible operations: eq, lt, gt, le, ge, co
# TODO: Regex verification with schema, to front end

from bento_lib.search import operations as op
from . import genotypes as gt


__all__ = [
    "VARIANT_CALL_SCHEMA",
    "VARIANT_SCHEMA",
    "VARIANT_TABLE_METADATA_SCHEMA",
]


VARIANT_CALL_SCHEMA = {
    "id": "variant:variant_call",  # TODO: Real ID
    "type": "object",
    "description": "An object representing a called instance of a variant.",
    "required": ["sample_id", "genotype_bases"],
    "properties": {
        "sample_id": {
            "type": "string",
            "description": "Variant call sample ID.",  # TODO: More detailed?
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "internal",
                "canNegate": True,
                "required": False,
                "type": "single",
                "order": 0
            }
        },
        "genotype_bases": {
            "type": "array",
            "description": "Variant call genotype.",
            "items": {
                "type": ["string", "null"],
                "description": "Variant call bases on a chromosome.",
                "search": {
                    "operations": [op.SEARCH_OP_EQ],
                    "queryable": "all",
                    "canNegate": True,
                    "required": False,
                    "type": "single",
                    "order": 0
                }
            },
            # TODO: Is this search block really needed
            "search": {
                "required": False,
                "type": "unlimited",
                "order": 1,
            }
        },
        "genotype_type": {
            "type": "string",
            "description": "Variant call genotype type.",
            "enum": [
                # No call
                gt.GT_UNCALLED,

                # Haploid
                gt.GT_REFERENCE,
                gt.GT_ALTERNATE,

                # Diploid or higher
                gt.GT_HOMOZYGOUS_REFERENCE,
                gt.GT_HETEROZYGOUS,
                gt.GT_HOMOZYGOUS_ALTERNATE,
            ],
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": True,
                "required": True,  # TODO: Shouldn't be "required" here; but should show up by default anyway
                "type": "single",
                "order": 2
            }
        },
        "phase_set": {
            "type": ["number", "null"],
            "description": "Genotype phase set, if any.",
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "internal",
                "canNegate": True,
                "required": False,
                "type": "single",
                "order": 3
            }
        }
    }
}


VARIANT_SCHEMA = {
    "$id": "variant:variant",  # TODO: Real ID
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "Bento variant data type",
    "type": "object",
    "required": ["assembly_id", "chromosome", "start", "end", "ref", "alt", "calls"],
    "search": {
        "operations": [],
    },
    "properties": {
        "assembly_id": {
            "type": "string",
            "enum": ["GRCh38", "GRCh37", "NCBI36", "Other"],
            "description": "Reference genome assembly ID.",
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": False,
                "required": True,
                "type": "single",
                "order": 0,
            },
        },
        "chromosome": {
            "type": "string",
            # TODO: Choices
            "description": "Reference genome chromosome identifier (e.g. 17 or X)",
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 1,
            },
        },
        "start": {
            "type": "integer",
            "description": "1-indexed start location of the variant on the chromosome.",
            "search": {
                "operations": [op.SEARCH_OP_EQ, op.SEARCH_OP_LT, op.SEARCH_OP_LE, op.SEARCH_OP_GT, op.SEARCH_OP_GE],
                "queryable": "all",
                "canNegate": False,
                "required": True,  # TODO: Shouldn't be "required" here; but should show up by default anyway
                "type": "unlimited",  # single / unlimited
                "order": 2,
            },
        },
        "end": {
            "type": "integer",
            "description": ("1-indexed end location (exclusive) of the variant on the chromosome, in terms of the "
                            "number of bases in the reference sequence for the variant."),
            "search": {
                "operations": [op.SEARCH_OP_EQ, op.SEARCH_OP_LT, op.SEARCH_OP_LE, op.SEARCH_OP_GT, op.SEARCH_OP_GE],
                "queryable": "all",
                "canNegate": True,
                "required": False,
                "type": "unlimited",  # single / unlimited
                "order": 3,
            },
        },
        "ref": {
            "type": "string",
            "description": "Reference base sequence for the variant.",
            "search": {
                "operations": [op.SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 4,
            },
        },
        "alt": {
            "type": "array",
            "description": "Alternate (non-reference) base sequences for the variant.",
            "items": {
                "type": "string",
                "description": "Alternate base sequence for the variant.",
                "search": {
                    "operations": [op.SEARCH_OP_EQ],
                    "queryable": "all",
                    "canNegate": True,
                    "required": False,
                    "type": "single",  # single / unlimited
                    "order": 0
                }
            },
            "search": {
                "order": 5
            },
        },
        "calls": {
            "type": "array",
            "description": "Called instances of this variant on samples.",
            "items": VARIANT_CALL_SCHEMA,
            "search": {
                "required": False,
                "type": "unlimited",
                "order": 6,
            }
        }
    }
}

VARIANT_TABLE_METADATA_SCHEMA = {
    "$id": "variant:table_metadata",  # TODO: Real ID
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "Bento variant data type metadata schema",
    "type": "object",
    "required": [],
    "properties": {
        "created": {
            "type": "string",
            "chord_autogenerated": True  # TODO: Extend schema
        },
        "updated": {
            "type": "string",
            "chord_autogenerated": True  # TODO: Extend schema
        }
    }
}
