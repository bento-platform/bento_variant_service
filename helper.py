#!/usr/bin/env python3

"""
Helper script for ingesting a .vcf.gz and .tbi file.
"""

import os
import requests
import sys


def usage():
    print("Usage: ./helper.py ingest table_id assembly_id file.vcf.gz \n"
          "  [or] ./helper.py new_table 'table name' \n"
          "  [or] ./helper.py search 1 0 10000")
    exit(1)


def ingest(args):
    if len(args) != 3:
        usage()

    table_id, assembly_id, vcf_file = args

    vcf_file = os.path.abspath(vcf_file)
    tbi_file = f"{vcf_file}.tbi"

    if not os.path.isfile(vcf_file) or not os.path.isfile(tbi_file):
        print("Missing .vcf.gz or .tbi file")
        exit(1)

    r = requests.post("http://localhost:5000/private/ingest", json={
        "table_id": table_id,
        "workflow_id": "vcf_gz",
        "workflow_params": {
            "vcf_gz.vcf_gz_files": [os.path.abspath(args[2])],
            "vcf_gz.assembly_id": assembly_id
        },
        "workflow_outputs": {
            "vcf_gz_files": [vcf_file],
            "tbi_files": [tbi_file]
        }
    })

    print(f"Finished with HTTP status {r.status_code}")
    exit(0 if r.status_code < 400 else 1)


def new_table(args):
    if len(args) != 1:
        usage()

    r = requests.post("http://localhost:5000/tables", json={
        "name": args[0],
        "data_type": "variant",
        "metadata": {}
    })

    if r.status_code != 201:
        print("Error creating table")
        print(r.json())
        exit(1)

    print(r.json())


def variant_search(args):
    if len(args) != 3:
        usage()

    chromosome, start_ge, start_le = args

    r = requests.post("http://localhost:5000/private/search", json={
        "data_type": "variant",
        "query": [
            "#and",
            ["#eq", ["#resolve", "chromosome"], chromosome],
            ["#and",
             ["#ge", ["#resolve", "start"], int(start_ge)],
             ["#le", ["#resolve", "start"], int(start_le)]]]
    })

    if r.status_code > 399:
        print(f"Error: {r.status_code}")
        exit(1)

    print(r.json())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()

    usage_mode = sys.argv[1]

    if usage_mode == "ingest":
        ingest(sys.argv[2:])

    elif usage_mode == "new_table":
        new_table(sys.argv[2:])

    elif usage_mode == "search":
        variant_search(sys.argv[2:])

    else:
        usage()
