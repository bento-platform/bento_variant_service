#!/usr/bin/env python

import setuptools

from chord_example_service import __version__

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_variant_service",
    version=__version__,

    python_requires=">=3.6",
    install_requires=["Flask", "pysam", "requests", "tqdm"],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="An implementation of a variant store for the CHORD project.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=["chord_variant_service"],

    url="TODO",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
