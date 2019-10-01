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

```bash
DATA=/path/to/data/directory
DEMO_DATA=  # Set this to something other than blank to download example data on boot
CHORD_URL=http://localhost/  # URL of CHORD node or service URL
```


## Running Tests

```bash
python3 -m pytest --cov=chord_variant_service --cov-branch
```
