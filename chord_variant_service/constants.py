import os
from chord_variant_service import __version__

__all__ = [
    "CHORD_URL",
    "DRS_URL_BASE_PATH",
    "SERVICE_NAME",
    "SERVICE_TYPE",
    "SERVICE_ID",
]

# Get CHORD_URL, defaulting to the development server URL otherwise.
CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")

# Base path for accessing internal DRS instance
DRS_URL_BASE_PATH = os.environ.get("DRS_URL_BASE_PATH", "/api/drs")

SERVICE_NAME = "Bento Variant Service"
SERVICE_TYPE = "ca.c3g.bento:variant:{}".format(__version__)
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)
