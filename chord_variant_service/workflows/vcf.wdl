workflow vcf {
    File vcf_file
    call vcf_compress {
        input: vcf_file=vcf_file
    }
    call generate_tbi
}

task generate_tbi {
  File vcf_gz_file
  command {
    tabix -p vcf ${vcf_gz_file}
  }
}

task vcf_compress {
    File vcf_file
    command {
        bgzip < ${vcf_file} > out.vcf.gz
    }
}
