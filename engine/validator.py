"""Validator — structural checks on FHIR Observation resources before sending."""

import logging
import re

logger = logging.getLogger(__name__)

VALID_STATUSES = {
    "registered", "preliminary", "final", "amended",
    "corrected", "cancelled", "entered-in-error", "unknown",
}

# Loose ISO-8601 pattern
_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?([\+\-]\d{2}:\d{2}|Z)?)?$"
)


def validate_observation(obs: dict) -> list[str]:
    """Validate a FHIR R4 Observation resource.

    Args:
        obs: FHIR Observation dict.

    Returns:
        List of error strings.  Empty list means valid.
    """
    errors = []

    # resourceType
    if obs.get("resourceType") != "Observation":
        errors.append("resourceType must be 'Observation'")

    # status
    status = obs.get("status")
    if not status:
        errors.append("Missing required field: status")
    elif status not in VALID_STATUSES:
        errors.append(f"Invalid status '{status}'; must be one of {sorted(VALID_STATUSES)}")

    # code
    code = obs.get("code")
    if not code:
        errors.append("Missing required field: code")
    else:
        codings = code.get("coding", [])
        if not codings:
            errors.append("code.coding must have at least one entry")
        else:
            for i, c in enumerate(codings):
                if not c.get("code"):
                    errors.append(f"code.coding[{i}].code is empty")

    # subject
    subject = obs.get("subject")
    if not subject:
        errors.append("Missing required field: subject")
    else:
        identifier = subject.get("identifier", {})
        if not identifier.get("value"):
            errors.append("subject.identifier.value is empty")

    # Must have either valueQuantity or valueString
    has_value = obs.get("valueQuantity") is not None or obs.get("valueString") is not None
    if not has_value:
        errors.append("Missing value: must have either valueQuantity or valueString")

    # effectiveDateTime format
    eff = obs.get("effectiveDateTime")
    if eff and not _DATETIME_RE.match(eff):
        errors.append(f"effectiveDateTime '{eff}' is not valid ISO-8601")

    # valueQuantity checks
    vq = obs.get("valueQuantity")
    if vq is not None:
        val = vq.get("value")
        if val is not None and not isinstance(val, (int, float)):
            errors.append(f"valueQuantity.value must be numeric, got {type(val).__name__}")
        if not vq.get("system"):
            errors.append("valueQuantity.system is missing (expected UCUM)")

    if errors:
        logger.warning("Validation failed with %d error(s): %s", len(errors), "; ".join(errors))

    return errors
