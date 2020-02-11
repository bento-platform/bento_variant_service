# CHORD Variant Service

![Build Status](https://api.travis-ci.org/c3g/chord_variant_service.svg?branch=master)
[![codecov](https://codecov.io/gh/c3g/chord_variant_service/branch/master/graph/badge.svg)](https://codecov.io/gh/c3g/chord_variant_service)

Proposed quality control pipeline:

* Standardize chromosome names (TODO: Only for humans? Maybe just remove `chr`)
* Verify positions are positive
* Investigate other error conditions for pytabix and check them in QC

The workflows exposed by this service currently depend on:

* HTSlib


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

All endpoints use **0-based** coordinates.


## Environment Variables

Default values for environment variables are listed on the right-hand side.

```bash
SERVICE_ID=ca.c3g.chord:variant:VERSION
DATA=/path/to/data/directory
CHORD_URL=http://localhost/  # URL for the CHORD node or standalone service
```

### Notes

  * If left unset, `SERVICE_ID` will default to `ca.c3g.chord:variant:VERSION`,
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
FLASK_APP=chord_variant_service.app FLASK_DEBUG=True flask run
```


## Running Tests

To run all tests and calculate coverage, including branch coverage, run the
following command:

```bash
python3 -m pytest --cov=chord_variant_service --cov-branch
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
