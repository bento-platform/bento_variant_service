# CHORD Variant Service

![Build Status](https://api.travis-ci.org/c3g/chord_variant_service.svg?branch=master)
[![codecov](https://codecov.io/gh/c3g/chord_variant_service/branch/master/graph/badge.svg)](https://codecov.io/gh/c3g/chord_variant_service)

Proposed quality control pipeline:

* Standardize chromosome names (TODO: Only for humans? Maybe just remove `chr`)
* Verify positions are positive
* Investigate other error conditions for pytabix and check them in QC

The workflows exposed by this service currently depend on:

* HTSlib


## Environment Variables

Default values for environment variables are listed on the right-hand side.

```bash
SERVICE_ID=ca.c3g.chord:variant:VERSION
DATA=/path/to/data/directory
CHORD_URL=http://localhost/  # URL of CHORD node or service URL
```

### Notes

  * If left unset, `SERVICE_ID` will default to `ca.c3g.chord:variant:VERSION`,
    where `VERSION` is the current version of the service package.

  * `CHORD_URL` is used to construct the reverse domain-name notation identifier
    for the GA4GH Beacon endpoints.


## Running in Development

```bash
FLASK_APP=chord_variant_service.app FLASK_DEBUG=True flask run
```


## Running Tests

```bash
python3 -m pytest --cov=chord_variant_service --cov-branch
```
