workflow vcf_gz {
    Array[File] vcf_gz_files
    String assembly_id

    scatter(file in vcf_gz_files) {
        call generate_tbi as generate_tbi_1 {
            input: vcf_gz_file=file
        }

        # Need to pass TBI file in here, otherwise execution occurs out-of-order
        call vcf_annotate {
            input: vcf_gz_file = file,
                   tbi_file = generate_tbi_1.tbi_file,
                   assembly_id = assembly_id
        }

        call generate_tbi as generate_tbi_2 {
            input: vcf_gz_file=vcf_annotate.annotated_vcf_gz_file
        }
    }
}

task vcf_annotate {
    File vcf_gz_file
    File tbi_file
    String assembly_id
    command {
        echo "##chord_assembly_id=${assembly_id}" > chord_assembly_id &&
        bcftools annotate --no-version -h chord_assembly_id ${vcf_gz_file} -o ${vcf_gz_file}_temp.vcf.gz -Oz &&
        rm chord_assembly_id ${vcf_gz_file} &&
        mv ${vcf_gz_file}_temp.vcf.gz ${vcf_gz_file}
    }
    output {
        File annotated_vcf_gz_file = "${vcf_gz_file}"
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
