from typing import Optional, Tuple
from . import genotypes as gt


__all__ = [
    "Variant",
    "Call",
]


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

    def __init__(self, variant: Variant, sample_id: str, genotype: Tuple[Optional[int], ...],
                 phase_set: Optional[int] = None):
        self.variant: Variant = variant
        self.sample_id: str = sample_id
        self.genotype: Tuple[int, ...] = genotype
        self.genotype_bases: Tuple[Optional[str], ...] = tuple(  # TODO: Structural variants
            None if g is None else (self.variant.ref_bases if g == 0 else self.variant.alt_bases[g-1])
            for g in genotype)
        self.phase_set: Optional[int] = phase_set

        if len(genotype) == 0:
            raise ValueError("Calls must have a genotype length of 1 or more")

        if genotype[0] is None:
            # Cannot make a call
            genotype_type = gt.GT_UNCALLED
        elif len(self.genotype) == 1:
            genotype_type = gt.GT_REFERENCE if self.genotype[0] == 0 else gt.GT_ALTERNATE
        else:  # len(self.genotype) > 1:
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
            "genotype_bases": list(self.genotype_bases),  # TODO: Structural variants
            "genotype_type": self.genotype_type,
            "phase_set": self.phase_set,
            **(self.variant.as_chord_representation() if include_variant else {}),
        }

    def eq_no_variant_check(self, other):
        # Use and shortcutting to return False early if the other instance isn't a Call
        return isinstance(other, Call) and all((
            self.sample_id == other.sample_id,
            self.genotype == other.genotype,  # Tuples are comparable via ==
            (self.phase_set is None and other.phase_set is None) or self.phase_set == other.phase_set
        ))
