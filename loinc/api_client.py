"""NLM LOINC API Client — queries the Clinical Tables API with retry and backoff."""

import logging
import time

import requests

logger = logging.getLogger(__name__)

API_URL = "https://clinicaltables.nlm.nih.gov/api/loinc_items/v3/search"
DEFAULT_TIMEOUT = 10  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0   # seconds — base for exponential backoff


def search_loinc(lab_name: str, max_results: int = 5) -> list[dict] | None:
    """Search the NLM Clinical Tables API for LOINC codes.

    Args:
        lab_name:    The lab test name to search for.
        max_results: Maximum number of results.

    Returns:
        List of dicts with 'loinc_code', 'display_name', 'confidence'.
        Empty list if API returns no matches.
        None if the API is unreachable after all retries.
    """
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                API_URL,
                params={"terms": lab_name, "maxList": max_results},
                timeout=DEFAULT_TIMEOUT,
            )

            # Rate limited — retry with backoff
            if resp.status_code == 429:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    "NLM API rate limited (429) for '%s', retry %d/%d in %.1fs",
                    lab_name, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            # Server error — retry with backoff
            if resp.status_code >= 500:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    "NLM API server error (%d) for '%s', retry %d/%d in %.1fs",
                    resp.status_code, lab_name, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return _parse_response(resp, lab_name)

        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    "NLM API unreachable for '%s' (attempt %d/%d): %s — retrying in %.1fs",
                    lab_name, attempt, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.warning(
                    "NLM API unreachable for '%s' after %d attempts: %s",
                    lab_name, MAX_RETRIES, exc,
                )

    # All retries exhausted — return None to signal unreachable
    return None


def _parse_response(resp: requests.Response, lab_name: str) -> list[dict]:
    """Parse a successful NLM API response."""
    try:
        data = resp.json()
        # API returns: [total_count, [codes], null, [[display_names]]]
        if not data or len(data) < 4:
            return []

        total = data[0]
        codes = data[1] if data[1] else []
        displays = data[3] if data[3] else []

        results = []
        for i, code in enumerate(codes):
            display = displays[i][0] if i < len(displays) and displays[i] else ""
            # Compute confidence based on position and total
            if total > 0:
                confidence = max(0.5, 1.0 - (i * 0.15))  # top result ~1.0
            else:
                confidence = 0.0
            results.append({
                "loinc_code": code,
                "display_name": display,
                "confidence": round(confidence, 2),
            })

        return results

    except (ValueError, KeyError, IndexError) as exc:
        logger.warning("Failed to parse NLM API response for '%s': %s", lab_name, exc)
        return []
