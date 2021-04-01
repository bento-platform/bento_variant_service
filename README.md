# Bento Variant Service

![Test Status](https://github.com/bento-platform/bento_variant_service/workflows/Test/badge.svg)
![Lint Status](https://github.com/bento-platform/bento_variant_service/workflows/Lint/badge.svg)
[![codecov](https://codecov.io/gh/bento-platform/bento_variant_service/branch/master/graph/badge.svg)](https://codecov.io/gh/bento-platform/bento_variant_service)

Proposed quality control pipeline:

* Standardize chromosome names (TODO: Only for humans? Maybe just remove `chr`)
* Verify positions are positive
* Investigate other error conditions for pytabix and check them in QC

The workflows exposed by this service currently depend on:

* HTSlib
* `bcftools`

The service itself depends on the following non-Python utilities:

* `bcftools`


## Copyright Notice

The Bento Variant Service is copyright &copy; 2019-2020 the Canadian Centre for
Computational Genomics, McGill University.

Portions of this codebase (namely, test VCF data) comes from the 1000 Genomes
Project, and is thus subject to their copyright and 
[terms of use](https://www.internationalgenome.org/IGSR_disclaimer).


## On Coordinates

VCFs, per the [spec](https://samtools.github.io/hts-specs/VCFv4.2.pdf), use
1-based coordinates:

> POS - position:  The reference position, with the 1st base having position 1.
> Positions are sorted numerically,in increasing order, within each reference
> sequence CHROM. It is permitted to have multiple records with the same POS.
> Telomeres are indicated by using positions 0 or N+1, where N is the length of
> the corresponding chromosome or contig.  (Integer, Required)

Beacon, on the other hand,
[specifies](https://github.com/ga4gh-beacon/specification/blob/v1.0.1/beacon.yaml#L41)
that 0-based coordinates should be used:

> ... Precise start coordinate position, allele locus (0-based, inclusive).

All **Beacon** endpoints use **0-based** coordinates with **closed** ranges.

All **other** endpoints use **1-based** coordinates with **half-open** ranges.


## Environment Variables

Default values for environment variables are listed on the right-hand side.

```bash
TABLE_MANAGER=drs
DRS_URL_BASE_PATH=/api/drs
SERVICE_ID=ca.c3g.bento:variant:VERSION
INITIALIZE_IMMEDIATELY=true
DATA=/path/to/data/directory
CHORD_URL=http://localhost/  # URL for the Bento node or standalone service
```

### Notes

  * `TABLE_MANAGER` is used to specify how data will be stored in the service
    instance, and what types of data files the service will look for in the
    `DATA` folder. The available options are:
       * `drs`: expects data as Data Repository Service (DRS) object links to
         `.vcf.gz` and `.vcf.gz.tbi` files
       * `memory`: stores data in memory for the duration of the service's
         process uptime
       * `vcf`: expects data as `.vcf.gz` and `.vcf.gz.tbi` files directly
       
  * `INITIALIZE_IMMEDIATELY` is used to specify whether to wait for a `GET` 
    request to /private/post-start-hook to initialize the service table manager
    
  * `DRS_URL_BASE_PATH` is used to specify the Bento container-internal base
    path to the container's DRS instance.

  * If left unset, `SERVICE_ID` will default to `ca.c3g.bento:variant:VERSION`,
    where `VERSION` is the current version of the service package.

  * `CHORD_URL` is used to construct the reverse domain-name notation identifier
    for the GA4GH Beacon endpoints.


## Running in Development

Development dependencies are described in `requirements.txt` and can be
installed using the following command:

```bash
pip install -r requirements.txt
```

The Flask development server can be run with the following command:

```bash
FLASK_APP=bento_variant_service.app FLASK_DEBUG=True flask run
```


## Running Tests

To run all tests and calculate coverage, including branch coverage, run the
following command:

```bash
python3 -m tox
```


## Deploying

In production, the service should be deployed using a WSGI service like
[uWSGI](https://uwsgi-docs.readthedocs.io/en/latest/) or
[Gunicorn](https://gunicorn.org/).


## Docker

The Dockerfile configures the service with Gunicorn. It is thus strongly
recommended that a reverse proxy such as NGINX is added in front of the
container.

The data for the service is stored inside the container's `/data` directory.
This should be bound as a persistent volume on the container host.

The service runs inside the container on port 8080.

Running the container by itself will use the following default configuration:

  * 1 worker process. Right now, the in-memory variant file cache means that
    using more than one worker can cause unexpected behaviour. This can be
    overridden by running the container with the option `--workers n`, where
    `n` is the number of workers.

  * `CHORD_URL=http://localhost/`. This will **NOT** work in production
    properly, as it is meant to represent the **public** URL of the node. This
    should be overridden.
