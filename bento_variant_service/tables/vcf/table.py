import re

from typing import Generator, List, Optional, Tuple

from bento_variant_service.beacon.datasets import BeaconDataset
from bento_variant_service.tables.base import VariantTable
from bento_variant_service.tables.vcf.file import VCFFile
from bento_variant_service.variants.models import Variant, Call


MAX_SIGNED_INT_32 = 2 ** 31 - 1

REGEX_GENOTYPE_SPLIT = re.compile(r"[|/]")
VCF_GENOTYPE = "GT"
VCF_READ_DEPTH = "DP"
VCF_PHASE_SET = "PS"


class VCFVariantTable(VariantTable):
    def __init__(
        self,
        table_id: str,
        name: Optional[str],
        metadata: dict,
        files: Tuple[VCFFile, ...] = (),
    ):
        self._files: Tuple[VCFFile, ...] = ()
        super().__init__(table_id, name, metadata)
        self.update_with_files(name, metadata, files)

    def update_with_files(self, name: Optional[str], metadata: dict, files: Tuple[VCFFile]):
        # Check passed files for well-formattedness, skip otherwise
        good_files: List[VCFFile] = []
        for file in files:
            fg = next(file.fetch(), ())
            if len(fg) >= 9:  # Need 9th column of VCF to deal with genotypes, samples, etc.
                good_files.append(file)

        self.update(name, metadata, tuple(vf.assembly_id for vf in good_files))
        self._files: Tuple[VCFFile] = tuple(good_files)

    @property
    def beacon_datasets(self):
        return tuple(
            BeaconDataset(
                table_id=self.table_id,
                table_name=self.name,
                table_metadata=self.metadata,
                assembly_id=a,
                files=tuple(vf for vf in self._files if vf.assembly_id == a)
            ) for a in sorted(self._assembly_ids)
        )

    @staticmethod
    def _int_or_none_from_vcf(val):
        return None if val == "." else int(val)

    @staticmethod
    def _variant_calls(variant: Variant, sample_ids: tuple, row: tuple, only_interesting: bool = False):
        for sample_id, row_data in zip(sample_ids, row[9:]):
            row_info = {k: v for k, v in zip(row[8].split(":"), row_data.split(":"))}

            if VCF_GENOTYPE not in row_info:
                # Only include samples which have genotypes
                continue

            gt = row_info[VCF_GENOTYPE]
            call = Call(
                variant=variant,
                genotype=tuple(
                    VCFVariantTable._int_or_none_from_vcf(g)
                    for g in re.split(REGEX_GENOTYPE_SPLIT, gt)),
                phased="/" in gt,
                phase_set=VCFVariantTable._int_or_none_from_vcf(row_info.get(VCF_PHASE_SET, ".")),
                sample_id=sample_id,
                read_depth=VCFVariantTable._int_or_none_from_vcf(row_info.get(VCF_READ_DEPTH, ".")),
            )

            if only_interesting and not call.is_interesting:
                # Uninteresting, not present on sample
                continue

            yield call

    @property
    def files(self) -> Tuple[VCFFile]:
        return self._files

    @property
    def n_of_variants(self) -> int:
        return sum(vf.n_of_variants for vf in self._files)

    @property
    def n_of_samples(self) -> int:
        sample_set = set()
        for vcf in self._files:
            sample_set.update(vcf.sample_ids)
        return len(sample_set)

    def _variants(
        self,
        assembly_id: Optional[str] = None,
        chromosome: Optional[str] = None,
        start_min: Optional[int] = None,
        start_max: Optional[int] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        only_interesting: bool = False,
    ) -> Generator[Variant, None, None]:
        # If offset isn't specified, set it to 0 (the very start)
        offset: int = 0 if offset is None else offset

        variants_passed = 0
        variants_seen = 0

        # TODO: Optimize offset/count
        #  e.g. by skipping entire VCFs if we know their row counts a-priori

        for vcf in filter(lambda vf: assembly_id is None or vf.assembly_id == assembly_id, self._files):
            if (
                chromosome is None and  # No filters (otherwise we wouldn't be able to assume we're skipping the VCF)
                start_min is None and  # "
                start_max is None and  # "
                not only_interesting and  # "
                vcf.n_of_variants <= offset - variants_seen
            ):
                # If the entire file is covered by the remaining offset, skip it. This saves time crawling through an
                # entire VCF if we cannot use any of them.
                variants_seen += vcf.n_of_variants
                continue

            try:
                # TODO: Security of passing this? Verify values in non-Beacon searches
                # TODO: What if the VCF includes telomeres (off the end)?]

                # TODO: pysam uses 0-based indexing, double-check

                query = ()
                if chromosome is not None:
                    query = (
                        chromosome,
                        start_min - 1 if start_min is not None else 0,
                        start_max - 1 if start_max is not None else MAX_SIGNED_INT_32,
                    )

                for row in vcf.fetch(*query):
                    variants_passed += 1

                    if variants_passed <= offset:
                        continue

                    if count is not None and variants_seen >= count:
                        return

                    if chromosome is None:
                        # Didn't index in, so check start_min / start_max by hand
                        if start_min is not None and int(row[1]) < start_min:
                            continue
                        elif start_max is not None and int(row[1]) >= start_max:
                            continue

                    variant = Variant(
                        assembly_id=vcf.assembly_id,
                        chromosome=row[0],
                        start_pos=int(row[1]),
                        ref_bases=row[3],
                        alt_bases=tuple(row[4].split(",")),
                        qual=int(row[5]) if row[5] != "." else None,
                        file_uri=vcf.original_index_uri,
                    )

                    variant.calls = tuple(VCFVariantTable._variant_calls(variant, vcf.sample_ids, row,
                                                                         only_interesting=only_interesting))

                    if only_interesting and len(variant.calls) == 0:
                        # Uninteresting; no calls of note on the variant
                        continue

                    yield variant

                    variants_seen += 1

            except ValueError as e:
                # Sometimes this can occur if a region not found in Tabix file, so continue searching but log it
                print(f"[Bento Variant Service] Encountered ValueError: {e}")
                continue
