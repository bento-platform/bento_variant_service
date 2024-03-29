import os
import pytest

from bento_variant_service.tables.vcf.file import VCFFile

from .shared_data import VCF_ONE_VAR_FILE_PATH, VCF_ONE_VAR_FILE_URI, DRS_VCF_ID


def test_vcf_file():
    file = VCFFile(VCF_ONE_VAR_FILE_URI)
    assert file.path == os.path.realpath(VCF_ONE_VAR_FILE_PATH)
    assert file.original_uri == VCF_ONE_VAR_FILE_URI
    assert file.index_path is None
    assert file.assembly_id == "GRCh37"
    assert len(file.sample_ids) == 835
    assert file.n_of_variants == 1
    assert len(tuple(file.fetch())) == 1
    assert repr(file) == f"<VCFFile {file.path}>"


def test_vcf_file_no_contig():
    file = VCFFile(VCF_ONE_VAR_FILE_URI)
    assert len(tuple(file.fetch("chr22", "1", "1000"))) == 0


def test_vcf_file_error():
    with pytest.raises(ValueError):
        VCFFile(VCF_ONE_VAR_FILE_URI, f"drs://drs.local/{DRS_VCF_ID}")
