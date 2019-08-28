workflow vcf {
    File vcf_file
    call vcf_compress {
        input: vcf_file=vcf_file
    }
    call generate_tbi {
        input: vcf_gz_file=vcf_compress.vcf_gz_file
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

task vcf_compress {
    File vcf_file
    command {
        bgzip ${vcf_file}
    }
    output {
        File vcf_gz_file = "out.vcf.gz"
    }
}
