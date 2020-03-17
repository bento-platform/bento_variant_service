import subprocess
from pysam import VariantFile
from typing import Optional, Tuple


__all__ = [
    "ASSEMBLY_ID_VCF_HEADER",
    "VCFFile",
]


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"


class VCFFile:
    def __init__(self, vcf_path: str, index_path: Optional[str] = None):
        vcf = VariantFile(vcf_path, index_path=index_path)

        # Store path for later opening
        self._path: str = vcf_path

        # Store index path for later opening - if it's None, _path + ".tbi" will be assumed by pysam.
        self._index_path: str = index_path

        # Find assembly ID
        self._assembly_id: str = "Other"
        for h in vcf.header.records:
            if h.key == ASSEMBLY_ID_VCF_HEADER:
                self._assembly_id = h.value

        # Find sample IDs
        self._sample_ids: Tuple[str] = tuple(vcf.header.samples)

        # Find row count
        p = subprocess.Popen(("bcftools", "index", "--nrecords", vcf_path), stdout=subprocess.PIPE)
        self._n_of_variants: int = int(p.stdout.read().strip())  # TODO: Handle error

        vcf.close()

    @property
    def path(self) -> str:
        return self._path

    @property
    def index_path(self) -> str:
        return self._index_path

    @property
    def assembly_id(self) -> str:
        return self._assembly_id

    @property
    def sample_ids(self) -> Tuple[str]:
        return self._sample_ids

    @property
    def n_of_variants(self) -> int:
        return self._n_of_variants
