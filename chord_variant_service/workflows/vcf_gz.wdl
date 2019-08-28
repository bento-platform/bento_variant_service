workflow vcf_gz {
    File vcf_gz_file
    call generate_tbi {
        input: vcf_gz_file=vcf_gz_file
    }
}

task generate_tbi {
  File vcf_gz_file
  command {
    tabix -p vcf ${vcf_gz_file}
  }
  output {
    File tbi_file = "out.vcf.gz.tbi"
  }
}
