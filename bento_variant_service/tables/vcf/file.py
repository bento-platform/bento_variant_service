import os
import pysam
import subprocess
import traceback

from pysam import VariantFile
from typing import Optional, Set, Sequence, Tuple
from urllib.parse import urlparse

from bento_variant_service.constants import SERVICE_NAME
from bento_variant_service.pool import WORKERS
from .drs_utils import DRS_URI_SCHEME, drs_vcf_to_internal_paths


__all__ = [
    "ASSEMBLY_ID_VCF_HEADER",
    "VCFFile",
]


ASSEMBLY_ID_VCF_HEADER = "chord_assembly_id"
CONTIG_VCF_HEADER = "contig"

CHR_PREFIX = "chr"
STANDARD_CHROMOSOMES = [
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14",
    "15", "16", "17", "18", "19", "20", "21", "22", "X", "Y", "MT",
]


class VCFFile:
    def __init__(self, vcf_uri: str, index_uri: Optional[str] = None):
        self._original_uri: str = vcf_uri
        self._original_index_uri: Optional[str] = index_uri

        # Turn the URIs into filesystem paths in order to open them quickly

        parsed_vcf_uri = urlparse(vcf_uri)
        parsed_index_uri = urlparse(index_uri or "")

        vcf_path = parsed_vcf_uri.path
        index_path = parsed_index_uri.path or None

        schemes = (parsed_vcf_uri.scheme, parsed_index_uri.scheme)
        same_schemes = len(set(schemes)) == 1

        # - If both are DRS, transform them into filesystem paths
        if DRS_URI_SCHEME in schemes:
            if not same_schemes:
                raise ValueError(f"Mismatched schemes: '{parsed_vcf_uri.scheme}' or '{parsed_index_uri.scheme}'")
            vcf_path, index_path, _vh, _ih = drs_vcf_to_internal_paths(vcf_uri, index_uri)

        # - Otherwise, if the scheme is not DRS, assume file://
        # TODO: Add HTTP support

        # - Store path for later opening
        self._path: str = os.path.realpath(vcf_path)

        # - Store index path for later opening - if it's None, _path + ".tbi" will be assumed by pysam.
        self._index_path: str = os.path.realpath(index_path) if index_path else None

        # Load some information from the VCF file

        vcf = VariantFile(vcf_path, index_filename=index_path)

        # - Find assembly ID
        self._assembly_id: str = "Other"
        for h in vcf.header.records:
            if h.key == ASSEMBLY_ID_VCF_HEADER:
                self._assembly_id = h.value
                break

        # - Find contigs and detect contig format (e.g. chr1 vs 1)
        self._contigs: Set[str] = set(vcf.header.contigs)

        #    - This will still work for an VCF that is on a non-standard contig, since it won't be picked up as a
        #      standard chromosome. We mostly don't want to prepend this, so erring on the side of not is a good move.
        self._use_chr_prefix: bool = any(
            c.startswith(CHR_PREFIX) and c.lstrip(CHR_PREFIX) in STANDARD_CHROMOSOMES for c in self._contigs)

        # - Find sample IDs
        self._sample_ids: Tuple[str] = tuple(vcf.header.samples)

        # - Find row count
        try:
            p = subprocess.Popen((
                "bcftools",
                "index",
                "--nrecords",
                f"{self._path}{f'##idx##{self._index_path}' if self._index_path else ''}"
            ), stdout=subprocess.PIPE)
            self._n_of_variants: int = int(p.stdout.read().strip())  # TODO: Handle error
        except (subprocess.CalledProcessError, ValueError) as e:
            # bcftools returned 1, or couldn't find number of records, or couldn't find index
            print(f"[{SERVICE_NAME}] [DEBUG] Consolidating bcftools call error {str(e)} to ValueError", flush=True)
            traceback.print_exc()
            raise ValueError(str(e))  # Consolidate to one exception type
        finally:
            vcf.close()

        print(f"[{SERVICE_NAME}] [DEBUG] Loaded VCF file from path {self._path} with:")
        print(f"[{SERVICE_NAME}] [DEBUG]   chr prefixes = {self._use_chr_prefix}")
        print(f"[{SERVICE_NAME}] [DEBUG]    assembly id = {self._assembly_id}")
        print(f"[{SERVICE_NAME}] [DEBUG]      # samples = {len(self._sample_ids)}")
        print(f"[{SERVICE_NAME}] [DEBUG]         # rows = {self._n_of_variants}", flush=True)

    @property
    def original_uri(self) -> str:
        return self._original_uri

    @property
    def original_index_uri(self) -> Optional[str]:
        return self._original_index_uri

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
        if args:
            # If we need to prepend a chr prefix, do so here
            contig = f"{CHR_PREFIX}{str(args[0]).lstrip(CHR_PREFIX)}" if self._use_chr_prefix else args[0]
            args = (contig, *args[1:])

            if contig not in self._contigs:
                # If the contig is not present in this file, skip it to not trigger a ValueError later (which is a bit
                # confusing for anyone reading the logs.)
                print(f"[{SERVICE_NAME}] [DEBUG] Skipping fetching from VCF {self._path}:")
                print(f"[{SERVICE_NAME}] [DEBUG]     Queried contig '{contig}' not in available contigs: "
                      f"{self._contigs}", flush=True)
                return ()

        # Takes pysam coordinates rather than CHORD coordinates
        # Parse as a Tabix file instead of a Variant file for performance reasons, and to get rows as tuples.
        f = pysam.TabixFile(self.path, index=self.index_path, parser=pysam.asTuple(), threads=WORKERS)

        try:
            yield from f.fetch(*args)
        finally:
            f.close()

    def __repr__(self):
        return f"<VCFFile {self._path}>"
