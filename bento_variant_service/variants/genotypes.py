__all__ = [
    "GT_UNCALLED",

    "GT_REFERENCE",
    "GT_ALTERNATE",

    "GT_HOMOZYGOUS_REFERENCE",
    "GT_HETEROZYGOUS",
    "GT_HOMOZYGOUS_ALTERNATE",

    "GT_UNINTERESTING_CALLS"
]


GT_UNCALLED = ""

# Haploid
GT_REFERENCE = "REFERENCE"
GT_ALTERNATE = "ALTERNATE"

# Diploid or higher
GT_HOMOZYGOUS_REFERENCE = "HOMOZYGOUS_REFERENCE"
GT_HETEROZYGOUS = "HETEROZYGOUS"
GT_HOMOZYGOUS_ALTERNATE = "HOMOZYGOUS_ALTERNATE"

GT_UNINTERESTING_CALLS = frozenset({GT_UNCALLED, GT_REFERENCE, GT_HOMOZYGOUS_REFERENCE})