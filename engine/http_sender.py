"""HTTP Sender — POSTs FHIR resources to a FHIR server with retry logic."""

import json
import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_TIMEOUT = 30  # seconds


def create_session(max_retries: int = DEFAULT_MAX_RETRIES,
                   backoff_factor: float = DEFAULT_BACKOFF_FACTOR) -> requests.Session:
    """Create a requests session with retry logic.

    Retries on 429, 500, 502, 503, 504 with exponential backoff.
    Does NOT retry on 4xx client errors (resource is wrong).
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "PUT", "GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def send_observation(observation: dict, config: dict, *,
                     dry_run: bool = False,
                     session: requests.Session | None = None) -> dict:
    """POST a FHIR Observation to the configured FHIR server.

    Args:
        observation: Validated FHIR Observation dict.
        config:      Config dict with fhir_server_url and fhir_auth_token.
        dry_run:     If True, log the resource but don't send.
        session:     Optional pre-built session (reuse for batch).

    Returns:
        dict with 'success', 'status_code', 'response', 'resource_id', 'error_type'.
    """
    resource_id = observation.get("id", "unknown")
    base_url = config["fhir_server_url"].rstrip("/")
    url = f"{base_url}/Observation"

    if dry_run:
        logger.info("[DRY RUN] Would POST Observation %s to %s", resource_id, url)
        logger.debug("[DRY RUN] Resource:\n%s", json.dumps(observation, indent=2))
        return {
            "success": True,
            "status_code": None,
            "response": "dry_run",
            "resource_id": resource_id,
            "error_type": None,
        }

    headers = {"Content-Type": "application/fhir+json"}
    token = config.get("fhir_auth_token", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    if session is None:
        session = create_session()

    try:
        resp = session.post(url, json=observation, headers=headers, timeout=DEFAULT_TIMEOUT)

        if resp.status_code in (200, 201):
            logger.info("Sent Observation %s — HTTP %d", resource_id, resp.status_code)
            return {
                "success": True,
                "status_code": resp.status_code,
                "response": resp.text[:500],
                "resource_id": resource_id,
                "error_type": None,
            }

        # Differentiate 4xx (client error) from 5xx (server error)
        if 400 <= resp.status_code < 500:
            logger.error(
                "Client error for Observation %s — HTTP %d (not retried): %s",
                resource_id, resp.status_code, resp.text[:300],
            )
            return {
                "success": False,
                "status_code": resp.status_code,
                "response": resp.text[:500],
                "resource_id": resource_id,
                "error_type": "client_error",
            }
        else:
            # 5xx are handled by urllib3 retry — this path means retries exhausted
            logger.error(
                "Server error for Observation %s — HTTP %d (retries exhausted): %s",
                resource_id, resp.status_code, resp.text[:300],
            )
            return {
                "success": False,
                "status_code": resp.status_code,
                "response": resp.text[:500],
                "resource_id": resource_id,
                "error_type": "server_error",
            }

    except requests.RequestException as exc:
        logger.error("HTTP error sending Observation %s: %s", resource_id, exc)
        return {
            "success": False,
            "status_code": None,
            "response": str(exc),
            "resource_id": resource_id,
            "error_type": "network_error",
        }
