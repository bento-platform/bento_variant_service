import os
from bento_variant_service import __version__

__all__ = [
    "CHORD_URL",
    "SERVICE_NAME",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
]

# Get CHORD_URL, defaulting to the development server URL otherwise.
CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")

SERVICE_NAME = "Bento Variant Service"
SERVICE_ARTIFACT = "variant"
SERVICE_TYPE_NO_VER = f"ca.c3g.bento:{SERVICE_ARTIFACT}"
SERVICE_TYPE = f"{SERVICE_TYPE_NO_VER}:{__version__}"
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE_NO_VER)
