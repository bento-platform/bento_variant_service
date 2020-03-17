import os
from chord_variant_service import __version__

__all__ = [
    "CHORD_URL",
    "DATA_PATH",
    "SERVICE_NAME",
    "SERVICE_TYPE",
    "SERVICE_ID",
]

# Get CHORD_URL, defaulting to the development server URL otherwise.
CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")

DATA_PATH = os.environ.get("DATA", "data/")

SERVICE_NAME = "CHORD Variant Service"
SERVICE_TYPE = "ca.c3g.chord:variant:{}".format(__version__)
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)
