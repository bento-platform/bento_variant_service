import os
import pysam
import subprocess
from pysam import VariantFile
from typing import Optional, Sequence, Tuple

from chord_variant_service.pool import WORKERS


__all__ = [
    "ASSEMBLY_ID_VCF_HEADER",
    "VCFFile",
]


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"


class VCFFile:
    def __init__(self, vcf_path: str, index_path: Optional[str] = None):
        vcf = VariantFile(vcf_path, index_filename=index_path)

        # Store path for later opening
        self._path: str = os.path.realpath(vcf_path)

        # Store index path for later opening - if it's None, _path + ".tbi" will be assumed by pysam.
        self._index_path: str = os.path.realpath(index_path) if index_path else None

        # Find assembly ID
        self._assembly_id: str = "Other"
        for h in vcf.header.records:
            if h.key == ASSEMBLY_ID_VCF_HEADER:
                self._assembly_id = h.value

        # Find sample IDs
        self._sample_ids: Tuple[str] = tuple(vcf.header.samples)

        # Find row count
        try:
            p = subprocess.Popen(("bcftools", "index", "--nrecords", vcf_path), stdout=subprocess.PIPE)
            # f = pysam.TabixFile(self.path, index=self.index_path, parser=pysam.asTuple(), threads=WORKERS)
            # self._n_of_variants: int = sum(1 for _ in f.fetch())  # TODO: Make this more efficient
            # f.close()
            self._n_of_variants: int = int(p.stdout.read().strip())  # TODO: Handle error
        except (subprocess.CalledProcessError, ValueError, OSError):
            # bcftools returned 1, or couldn't find number of records, or couldn't find index
            raise ValueError  # Consolidate to one exception type
        finally:
            vcf.close()

    @property
    def path(self) -> str:
        return self._path

    @property
    def index_path(self) -> Optional[str]:
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

    def fetch(self, *args) -> Sequence[tuple]:
        # Takes pysam coordinates rather than CHORD coordinates
        # Parse as a Tabix file instead of a Variant file for performance reasons, and to get rows as tuples.
        f = pysam.TabixFile(self.path, index=self.index_path, parser=pysam.asTuple(), threads=WORKERS)
        yield from f.fetch(*args)
        f.close()
