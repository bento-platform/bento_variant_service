workflow vcf {
    Array[File] vcf_files
    String assembly_id

    scatter(file in vcf_files) {
        call vcf_annotate_compress {
            input: vcf_file = file,
                   assembly_id = assembly_id
        }
    }

    scatter(file in vcf_annotate_compress.annotated_vcf_gz_file) {
        call generate_tbi {
            input: vcf_gz_file = file
        }
    }
}

task vcf_annotate_compress {
    File vcf_file
    String assembly_id
    command {
        echo "##chord_assembly_id=${assembly_id}" > chord_assembly_id &&
        bcftools annotate --no-version -h chord_assembly_id ${vcf_file} -o ${vcf_file}.gz -Oz &&
        rm chord_assembly_id ${vcf_file}
    }
    output {
        File annotated_vcf_gz_file = "${vcf_file}.gz"
    }
}

task generate_tbi {
    File vcf_gz_file
    command {
        rm -f ${vcf_gz_file}.tbi &&
        tabix -p vcf ${vcf_gz_file}
    }
    output {
        File tbi_file = "${vcf_gz_file}.tbi"
    }
}
