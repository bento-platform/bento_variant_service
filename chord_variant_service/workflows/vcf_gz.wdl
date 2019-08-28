workflow vcf_gz {
    File vcf_gz_file
    call rename_vcf_gz {
        input: vcf_gz_file=vcf_gz_file
    }
    call generate_tbi
}

task generate_tbi {
  command {
    tabix -p vcf out.vcf.gz
  }
  output {
    File tbi_file = "out.vcf.gz.tbi"
  }
}

task rename_vcf_gz {
  File vcf_gz_file
  command {
    mv ${vcf_gz_file} out.vcf.gz
  }
}
