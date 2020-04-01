import os
from chord_variant_service.tables.vcf.file import VCFFile

from .shared_data import VCF_ONE_VAR_FILE_PATH


def test_vcf_file():
    file = VCFFile(VCF_ONE_VAR_FILE_PATH)
    assert file.path == os.path.realpath(VCF_ONE_VAR_FILE_PATH)
    assert file.index_path is None
    assert file.assembly_id == "GRCh37"
    assert len(file.sample_ids) == 835
    assert file.n_of_variants == 1
    assert len(tuple(file.fetch())) == 1
