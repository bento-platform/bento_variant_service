import os
import re
import requests
import requests_unixsocket
import sys

from ..constants import CHORD_URL, SERVICE_NAME
from typing import Dict, Optional, Tuple
from urllib.parse import quote, urlparse


# Monkey-patch in socket request support to query DRS internally
# TODO: Replace by proper access headers and CHORD_URL?
requests_unixsocket.monkeypatch()


OptionalHeaders = Optional[Dict[str, str]]


HTTP_PATTERN = re.compile(r"^https?")
NGINX_INTERNAL_SOCKET = quote(os.environ.get("NGINX_INTERNAL_SOCKET", "/chord/tmp/nginx_internal.sock"), safe="")
UNIX_DRS_REQUEST_TEMPLATE = f"http+unix://{NGINX_INTERNAL_SOCKET}/api/drs"


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

    # TODO: Support external DRS providers?
    chord_url_no_protocol = re.sub(HTTP_PATTERN, "", CHORD_URL)
    if chord_url_no_protocol not in vcf_url or chord_url_no_protocol not in index_url:
        print(f"[{SERVICE_NAME}] External DRS url supplied (not implemented): '{vcf_url}' or '{index_url}'",
              file=sys.stderr, flush=True)
        return None

    # TODO: Make this not CHORD-specific in its URL format
    vcf_res = requests.get(f"{UNIX_DRS_REQUEST_TEMPLATE}/objects/{parsed_vcf_url.path}")
    idx_res = requests.get(f"{UNIX_DRS_REQUEST_TEMPLATE}/objects/{parsed_index_url.path}")

    if vcf_res.status_code != 200 or idx_res.status_code != 200:
        print(f"[{SERVICE_NAME}] Could not fetch: '{vcf_url}' or '{index_url}'",
              file=sys.stderr, flush=True)
        return None

    # TODO: Handle JSON parse errors
    vcf_data = vcf_res.json()
    idx_data = idx_res.json()

    vcf_access = next((a for a in vcf_data.get("access_methods", []) if a.get("type", None) == "file"), None)
    idx_access = next((a for a in idx_data.get("access_methods", []) if a.get("type", None) == "file"), None)

    vcf_path = vcf_access.get("access_url", {}).get("url", None)
    idx_path = idx_access.get("access_url", {}).get("url", None)

    if vcf_path is None or idx_path is None:
        print(f"[{SERVICE_NAME}] Could not find access data for: '{vcf_url}' or '{index_url}'",
              file=sys.stderr, flush=True)

    return (
        str(vcf_path).replace("file://", ""),  # TODO: Leave this here?
        str(idx_path).replace("file://", ""),  # TODO: "
        vcf_access.get("access_url", {}).get("headers", None),
        idx_access.get("access_url", {}).get("headers", None),
    )
