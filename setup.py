#!/usr/bin/env python

import configparser
import os
import setuptools

with open("README.md", "r") as rf:
    long_description = rf.read()

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "bento_variant_service", "package.cfg"))

setuptools.setup(
    name=config["package"]["name"],
    version=config["package"]["version"],

    python_requires=">=3.6",
    install_requires=[
        "bento_lib[flask]==0.11.0",
        "Flask>=1.1.2,<2.0",
        "jsonschema>=3.2.0,<4.0",
        "pysam>=0.16.0.1,<0.17",
        "requests>=2.25.1,<3.0",
        "requests_unixsocket>=0.2.0,<0.3.0",
    ],

    author=config["package"]["authors"],
    author_email=config["package"]["author_emails"],

    description="An implementation of a variant store for the Bento platform.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=setuptools.find_packages(),
    include_package_data=True,

    url="https://github.com/bento-platform/bento_variant_service",
    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent"
    ]
)
