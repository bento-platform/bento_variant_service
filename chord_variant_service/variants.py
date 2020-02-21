# Possible operations: eq, lt, gt, le, ge, co
# TODO: Regex verification with schema, to front end

import json
from chord_lib.search.operations import *
from typing import Optional, Tuple


__all__ = [
    "VARIANT_SCHEMA",
    "VARIANT_TABLE_METADATA_SCHEMA",
    "Variant",
    "Call",
]


# Haploid
GT_REFERENCE = "REFERENCE"
GT_ALTERNATE = "ALTERNATE"

# Diploid or higher
GT_HOMOZYGOUS_REFERENCE = "HOMOZYGOUS_REFERENCE"
GT_HETEROZYGOUS = "HETEROZYGOUS"
GT_HOMOZYGOUS_ALTERNATE = "HOMOZYGOUS_ALTERNATE"


VARIANT_CALL_SCHEMA = {
    "type": "object",
    "description": "An object representing a called instance of a variant.",
    "required": ["sample_id", "genotype_bases"],
    "properties": {
        "sample_id": {
            "type": "string",
            "description": "Variant call sample ID.",  # TODO: More detailed?
            "search": {
                "operations": [SEARCH_OP_EQ],
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
                "type": "string",
                "description": "Variant call bases on a chromosome.",
                "search": {
                    "operations": [SEARCH_OP_EQ],
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
                "",

                # Haploid
                GT_REFERENCE,
                GT_ALTERNATE,

                # Diploid or higher
                GT_HOMOZYGOUS_REFERENCE,
                GT_HETEROZYGOUS,
                GT_HOMOZYGOUS_ALTERNATE,
            ],
            "search": {
                "operations": [SEARCH_OP_EQ],
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
                "operations": [SEARCH_OP_EQ],
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
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
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
                "operations": [SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": False,
                "required": True,
                "type": "single",
                "order": 0
            }
        },
        "chromosome": {
            "type": "string",
            # TODO: Choices
            "description": "Reference genome chromosome identifier (e.g. 17 or X)",
            "search": {
                "operations": [SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 1
            }
        },
        "start": {
            "type": "integer",
            "description": "1-indexed start location of the variant on the chromosome.",
            "search": {
                "operations": [SEARCH_OP_EQ, SEARCH_OP_LT, SEARCH_OP_LE, SEARCH_OP_GT, SEARCH_OP_GE],
                "queryable": "all",
                "canNegate": False,
                "required": True,  # TODO: Shouldn't be "required" here; but should show up by default anyway
                "type": "unlimited",  # single / unlimited
                "order": 2
            }
        },
        "end": {
            "type": "integer",
            "description": ("1-indexed end location (exclusive) of the variant on the chromosome, in terms of the "
                            "number of bases in the reference sequence for the variant."),
            "search": {
                "operations": [SEARCH_OP_EQ, SEARCH_OP_LT, SEARCH_OP_LE, SEARCH_OP_GT, SEARCH_OP_GE],
                "queryable": "all",
                "canNegate": True,
                "required": False,
                "type": "unlimited",  # single / unlimited
                "order": 3
            }
        },
        "ref": {
            "type": "string",
            "description": "Reference base sequence for the variant.",
            "search": {
                "operations": [SEARCH_OP_EQ],
                "queryable": "all",
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 4
            }
        },
        "alt": {
            "type": "array",
            "description": "Alternate (non-reference) base sequences for the variant.",
            "items": {
                "type": "string",
                "description": "Alternate base sequence for the variant.",
                "search": {
                    "operations": [SEARCH_OP_EQ],
                    "queryable": "all",
                    "canNegate": True,
                    "required": False,
                    "type": "single",  # single / unlimited
                    "order": 0
                }
            },
            "search": {
                "order": 5
            }
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
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type metadata schema",
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


class Variant:
    """
    Instance of a particular variant and all calls made.
    """

    def __init__(self, assembly_id: str, chromosome: str, ref_bases: str, alt_bases: Tuple[str, ...], start_pos: int,
                 calls: Tuple["Call"] = ()):
        self.assembly_id: str = assembly_id  # Assembly ID for context
        self.chromosome: str = chromosome  # Chromosome where the variant occurs
        self.ref_bases: str = ref_bases  # Reference bases
        self.alt_bases: Tuple[str] = alt_bases  # Alternate bases  TODO: Structural variants
        self.start_pos: int = start_pos  # Starting position on the chromosome w/r/t the reference, 0-indexed
        self.calls: Tuple["Call"] = calls  # Variant calls, per sample  TODO: Make this a dict?

    @property
    def end_pos(self) -> int:
        """
        End position of the reference sequence, 0-indexed.
        """
        return self.start_pos + len(self.ref_bases)

    def as_chord_representation(self):
        return {
            "assembly_id": self.assembly_id,
            "chromosome": self.chromosome,
            "start": self.start_pos,  # 1-based, inclusive
            "end": self.end_pos,  # 1-based, exclusive  TODO: Convention here? exclusive or inclusive?
            "ref": self.ref_bases,
            "alt": list(self.alt_bases),  # TODO: Change property name?
            "calls": [c.as_chord_representation() for c in self.calls],
        }

    def __eq__(self, other):
        if not isinstance(other, Variant):
            return False

        return all((
            (self.assembly_id is None and other.assembly_id is None) or self.assembly_id == other.assembly_id,
            self.chromosome == other.chromosome,
            self.ref_bases == other.ref_bases,
            self.alt_bases == other.alt_bases,
            self.start_pos == other.start_pos,
            len(self.calls) == len(other.calls),
            all((c1.eq_no_variant_check(c2) for c1, c2 in zip(self.calls, other.calls))),
        ))


class Call:
    """
    Instance of a called variant on a particular sample.
    """

    def __init__(self, variant: Variant, sample_id: str, genotype: Tuple[int, ...], phase_set: Optional[int] = None):
        self.variant: Variant = variant
        self.sample_id: str = sample_id
        self.genotype: Tuple[int, ...] = genotype
        self.genotype_bases: Tuple[Optional[str], ...] = tuple(  # TODO: Structural variants
            None if g is None else (self.variant.ref_bases if g == 0 else self.variant.alt_bases[g-1])
            for g in genotype)
        self.phase_set: Optional[int] = phase_set

        genotype_type = ""
        if len(self.genotype) == 1:
            genotype_type = GT_REFERENCE if self.genotype[0] == 0 else GT_ALTERNATE
        elif len(self.genotype) > 1:
            genotype_type = GT_HOMOZYGOUS_ALTERNATE
            if len(set(self.genotype)) > 1:
                genotype_type = GT_HETEROZYGOUS
            elif self.genotype[0] == 0:
                # all elements are 0 if 0 is the first element and the length of the set is 1
                genotype_type = GT_HOMOZYGOUS_REFERENCE
        self.genotype_type = genotype_type

    def as_chord_representation(self, include_variant: bool = False):
        return {
            "sample_id": self.sample_id,
            "genotype_bases": list(self.genotype_bases),  # TODO: Structural variants
            "genotype_type": self.genotype_type,
            "phase_set": self.phase_set,
            **(self.variant.as_chord_representation() if include_variant else {}),
        }

    def eq_no_variant_check(self, other):
        if not isinstance(other, Call):
            return False

        return all((
            self.sample_id == other.sample_id,
            json.dumps(self.genotype) == json.dumps(other.genotype),
            (self.phase_set is None and other.phase_set is None) or self.phase_set == other.phase_set
        ))
