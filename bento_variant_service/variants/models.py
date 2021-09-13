from enum import Enum
from typing import Optional, Tuple, Union
from . import genotypes as gt


__all__ = [
    "AlleleClass",
    "Allele",
    "Variant",
    "Call",

    "VCF_MISSING_VAL",
    "VCF_MISSING_UPSTREAM_VAL",
    "ALLELE_MISSING",
    "ALLELE_MISSING_UPSTREAM",
]


# SEQUENCE: Sequence of base pairs e.g. CAG
# STRUCTURAL: <ID>; need to check it's valid (out of scope for the enum)
# MISSING_UPSTREAM: *
# MISSING: .
AlleleClass = Enum("AlleleClass", ("SEQUENCE", "STRUCTURAL", "MISSING_UPSTREAM", "MISSING"))


VCF_MISSING_VAL = "."
VCF_MISSING_UPSTREAM_VAL = "*"


class Allele:
    def __init__(self, allele_class: AlleleClass, value: Optional[str]):
        self.allele_class: AlleleClass = allele_class
        self.value: Optional[str] = value  # Include angle brackets here for extra clarity / unambiguity

        if self.value is None and self.allele_class not in (AlleleClass.MISSING, AlleleClass.MISSING_UPSTREAM):
            raise ValueError(f"Invalid None value for allele of class {self.allele_class}")

    def __eq__(self, other):
        if not isinstance(other, Allele):
            return False

        return self.allele_class == other.allele_class \
            and ((self.value is None and other.value is None) or self.value == other.value)

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        if self.allele_class == AlleleClass.MISSING:
            return VCF_MISSING_VAL
        elif self.allele_class == AlleleClass.MISSING_UPSTREAM:
            return VCF_MISSING_UPSTREAM_VAL
        else:
            return self.value

    @classmethod
    def class_from_vcf(cls, allele: str):
        if allele == VCF_MISSING_VAL:
            return AlleleClass.MISSING
        elif allele == VCF_MISSING_UPSTREAM_VAL:
            return AlleleClass.MISSING_UPSTREAM
        elif allele[0] == "<" and allele[-1] == ">":  # Should be of length 1 at least due to VCF spec
            return AlleleClass.STRUCTURAL
        else:
            return AlleleClass.SEQUENCE


class Variant:
    """
    Instance of a particular variant and all calls made.
    """

    def __init__(self, assembly_id: str, chromosome: str, ref_bases: str, alt_alleles: Tuple[Allele, ...],
                 start_pos: int, qual: Optional[float] = None, calls: Tuple["Call"] = (),
                 file_uri: Optional[str] = None):
        self.assembly_id: str = assembly_id  # Assembly ID for context
        self.chromosome: str = chromosome  # Chromosome where the variant occurs
        self.ref_bases: str = ref_bases  # Reference bases
        self.alt_alleles: Tuple[Allele, ...] = alt_alleles  # Alternate alleles - tuple makes them comparable
        self.start_pos: int = start_pos  # Starting position on the chromosome w/r/t the reference, 0-indexed
        self.qual: Optional[float] = qual  # Quality score for "assertion made by alt"
        self.calls: Tuple["Call"] = calls  # Variant calls, per sample  TODO: Make this a dict?

        self.file_uri: Optional[str] = file_uri  # File URI, "

    @property
    def ref_allele(self) -> Allele:
        # Ref alleles have to be bases to my knowledge
        return Allele(AlleleClass.SEQUENCE, self.ref_bases)

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
            "alt": [a.value for a in self.alt_alleles],  # TODO: Include both value and class here?
            "qual": self.qual,
            "calls": [c.as_chord_representation() for c in self.calls],
        }

    def as_augmented_chord_representation(self):
        return {
            **self.as_chord_representation(),
            # _ prefix is context dependent -> immune from equality, used by Bento in weird contexts
            "_extra": {
                "file_uri": self.file_uri,
            },
        }

    def __eq__(self, other):
        # Use and shortcutting to return False early if the other instance isn't a Variant
        return isinstance(other, Variant) and all((
            (self.assembly_id is None and other.assembly_id is None) or self.assembly_id == other.assembly_id,
            self.chromosome == other.chromosome,
            self.ref_bases == other.ref_bases,
            self.alt_alleles == other.alt_alleles,
            self.start_pos == other.start_pos,
            (self.qual is None and other.qual is None) or self.qual == other.qual,
            len(self.calls) == len(other.calls),
            all((c1.eq_no_variant_check(c2) for c1, c2 in zip(self.calls, other.calls))),
        ))


ALLELE_MISSING = Allele(AlleleClass.MISSING, ".")
ALLELE_MISSING_UPSTREAM = Allele(AlleleClass.MISSING_UPSTREAM, "*")


class Call:
    """
    Instance of a called variant on a particular sample.
    """

    def _genotype_to_allele(self, g) -> Allele:
        if g == VCF_MISSING_VAL:
            return ALLELE_MISSING
        elif g == VCF_MISSING_UPSTREAM_VAL:
            return ALLELE_MISSING_UPSTREAM
        elif g == 0:
            return self.variant.ref_allele
        else:
            return self.variant.alt_alleles[g - 1]

    # TODO: py3.8: More refined typing for genotype: Tuple[Union[int, Literal["*"], Literal["."]], ...]

    def __init__(self, variant: Variant, sample_id: str, genotype: Tuple[Union[int, str], ...],
                 phased: bool = False, phase_set: Optional[int] = None, read_depth: Optional[int] = None):
        self.variant: Variant = variant
        self.sample_id: str = sample_id
        self.genotype: Tuple[Union[int, str], ...] = genotype
        self.genotype_alleles: Tuple[Allele, ...] = tuple(map(self._genotype_to_allele, genotype))
        self.phased: bool = phased
        self.phase_set: Optional[int] = phase_set if phased else None  # Should be ignored if phased
        self.read_depth: Optional[int] = read_depth

        if len(genotype) == 0:
            raise ValueError("Calls must have a genotype length of 1 or more")

        if genotype[0] == VCF_MISSING_VAL:
            # Missing call
            genotype_type = gt.GT_MISSING
        elif genotype[0] == VCF_MISSING_UPSTREAM_VAL:
            # Missing call due to an upstream deletion
            genotype_type = gt.GT_MISSING_UPSTREAM_DELETION
        elif len(self.genotype) == 1:
            genotype_type = gt.GT_REFERENCE if self.genotype[0] == 0 else gt.GT_ALTERNATE
        else:  # len(self.genotype) > 1; not haploid
            genotype_type = gt.GT_HOMOZYGOUS_ALTERNATE
            if len(set(self.genotype)) > 1:
                genotype_type = gt.GT_HETEROZYGOUS
            elif self.genotype[0] == 0:
                # all elements are 0 if 0 is the first element and the length of the set is 1
                genotype_type = gt.GT_HOMOZYGOUS_REFERENCE

        self.genotype_type = genotype_type

    @property
    def is_interesting(self):
        return self.genotype_type not in gt.GT_UNINTERESTING_CALLS

    def as_chord_representation(self, include_variant: bool = False):
        return {
            "sample_id": self.sample_id,
            "genotype_alleles": [a.value for a in self.genotype_alleles],  # TODO: Include allele class?
            "genotype_type": self.genotype_type,
            "phased": self.phased,
            "phase_set": self.phase_set,
            **(self.variant.as_chord_representation() if include_variant else {}),
        }

    def eq_no_variant_check(self, other):
        # Use and shortcutting to return False early if the other instance isn't a Call
        return isinstance(other, Call) and all((
            self.sample_id == other.sample_id,
            self.phased == other.phased,
            (self.phase_set is None and other.phase_set is None) or self.phase_set == other.phase_set,

            # Tuples are comparable via ==
            # If unphased, genotype order doesn't matter, so we compare using sets
            self.genotype == other.genotype if self.phased else set(self.genotype) == set(other.genotype),

            (self.read_depth is None and other.read_depth is None) or self.read_depth == other.read_depth,
        ))
