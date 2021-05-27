import re
import requests
import requests_unixsocket
import sys

from flask import current_app
from jsonschema import Draft7Validator
from requests.exceptions import ConnectionError as RequestsConnectionError
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from bento_variant_service.constants import SERVICE_NAME


__all__ = [
    "DRS_DATA_SCHEMA",
    "DRS_DATA_SCHEMA_VALIDATOR",
    "DRS_URI_SCHEME",
    "drs_vcf_to_internal_paths",
]


# Monkey-patch in socket request support to query DRS internally
# TODO: Replace by proper access headers and CHORD_URL?
requests_unixsocket.monkeypatch()


OptionalHeaders = Optional[Dict[str, str]]

HTTP_PATTERN = re.compile(r"^https?")
STARTING_SLASH_PATTERN = re.compile(r"^/")

DRS_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "data": {
            "type": "string"
        },
        "index": {
            "type": "string"
        }
    },
    "required": ["data", "index"]
}

DRS_DATA_SCHEMA_VALIDATOR = Draft7Validator(DRS_DATA_SCHEMA)

DRS_URI_SCHEME = "drs"


def _get_drs_decoded_url(parsed_url):
    # TODO: Make this not CHORD-specific in its URL format - switch to ga4gh namespace

    config_drs_url = current_app.config["DRS_URL"]
    base = config_drs_url if config_drs_url else f"https://{parsed_url.netloc}"
    return f"{base}/objects/{parsed_url.path.split('/')[-1]}"


def _get_file_access_method_if_any(drs_object_record: dict) -> Optional[dict]:
    # TODO: Handle malformed DRS responses
    return next((a for a in drs_object_record.get("access_methods", []) if a.get("type", None) == "file"), None)


def drs_vcf_to_internal_paths(
    vcf_url: str,
    index_url: str,
) -> Optional[Tuple[str, str, OptionalHeaders, OptionalHeaders]]:
    parsed_vcf_url = urlparse(vcf_url)
    parsed_index_url = urlparse(index_url)

    if parsed_vcf_url.scheme != "drs" or parsed_index_url.scheme != "drs":
        print(f"[{SERVICE_NAME}] Invalid scheme: '{parsed_vcf_url.scheme}' or '{parsed_index_url.scheme}'",
              file=sys.stderr, flush=True)
        return None

    vcf_decoded_url = _get_drs_decoded_url(parsed_vcf_url)
    idx_decoded_url = _get_drs_decoded_url(parsed_index_url)

    try:
        print(f"[{SERVICE_NAME}] Attempting to fetch {vcf_decoded_url}", flush=True)
        vcf_res = requests.get(vcf_decoded_url)
        print(f"[{SERVICE_NAME}] Attempting to fetch {idx_decoded_url}", flush=True)
        idx_res = requests.get(idx_decoded_url)

        if vcf_res.status_code != 200 or idx_res.status_code != 200:
            print(f"[{SERVICE_NAME}] Could not fetch: '{vcf_url}' or '{index_url}'", file=sys.stderr, flush=True)
            print(f"\tAttempted VCF URL: {vcf_decoded_url} (Status: {vcf_res.status_code})",
                  file=sys.stderr, flush=True)
            print(f"\tAttempted TBI URL: {idx_decoded_url} (Status: {idx_res.status_code})",
                  file=sys.stderr, flush=True)
            return None

        # TODO: Handle JSON parse errors
        vcf_access = _get_file_access_method_if_any(vcf_res.json())
        idx_access = _get_file_access_method_if_any(idx_res.json())

        if vcf_access is None or idx_access is None:
            print(f"[{SERVICE_NAME}] Could not find access data for: '{vcf_url}' or '{index_url}'",
                  file=sys.stderr, flush=True)
            print(f"\tVCF Response:   {vcf_res.json()}", file=sys.stderr, flush=True)
            print(f"\tIndex Response: {idx_res.json()}", file=sys.stderr, flush=True)
            return None

        vcf_path = vcf_access["access_url"]["url"]
        idx_path = idx_access["access_url"]["url"]

        return (
            str(vcf_path).replace("file://", ""),  # TODO: ??
            str(idx_path).replace("file://", ""),  # TODO: "
            vcf_access["access_url"].get("headers"),
            idx_access["access_url"].get("headers"),
        )

    except RequestsConnectionError as e:
        print(f"[{SERVICE_NAME}] Encountered connection error while fetching '{vcf_url}' or '{index_url}' ({str(e)})",
              file=sys.stderr, flush=True)
        return None
