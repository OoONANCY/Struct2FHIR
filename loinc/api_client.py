"""NLM LOINC API Client — queries the Clinical Tables API as a fallback resolver."""

import logging

import requests

logger = logging.getLogger(__name__)

API_URL = "https://clinicaltables.nlm.nih.gov/api/loinc_items/v3/search"
DEFAULT_TIMEOUT = 10  # seconds


def search_loinc(lab_name: str, max_results: int = 5) -> list[dict]:
    """Search the NLM Clinical Tables API for LOINC codes.

    Args:
        lab_name:    The lab test name to search for.
        max_results: Maximum number of results.

    Returns:
        List of dicts with 'loinc_code', 'display_name', 'confidence'.
        Empty list if API is unreachable.
    """
    try:
        resp = requests.get(
            API_URL,
            params={"terms": lab_name, "maxList": max_results},
            timeout=DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("NLM LOINC API unreachable for '%s': %s", lab_name, exc)
        return []

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
            # Compute a simple confidence based on position and total
            if total > 0:
                confidence = max(0.5, 1.0 - (i * 0.15))  # top result ~1.0, drops by 15%
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
